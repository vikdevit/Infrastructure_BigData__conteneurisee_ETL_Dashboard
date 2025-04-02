from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, hour, dayofweek, month, year, current_timestamp, unix_timestamp, count, lit, row_number, date_format, udf, concat_ws, from_unixtime, round
from pyspark.sql.window import Window
import boto3
import json
import uuid
from pyspark.sql.types import StringType

import datetime  

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Configurer l'accès à Localstack via boto3
s3_client = boto3.client('s3',
                         endpoint_url='http://172.17.0.1:4566',
                         aws_access_key_id='test',
                         aws_secret_access_key='test',
                         region_name='us-east-1')

# Téléchargement du fichier JSON depuis le bucket S3 simulé
bucket_name_1 = 'aviationstack'
json_file_key_1 = 'aviation-data.json'  # Nom du fichier dans le bucket S3 (pas de répertoire)
json_file_path_1 = '/app/aviation-data.json'  # Chemin local dans le conteneur Docker

bucket_name_2 = 'openskytrax'
json_file_key_2 = 'opensky_data.json'
json_file_path_2 = '/app/opensky_data.json'

bucket_name_2 = 'openskytrax'
json_file_key_3 = 'skytrax_data.json'
json_file_path_3 = '/app/skytrax_data.json'


# Télécharger le fichier S3 dans le conteneur local
s3_client.download_file(bucket_name_1, json_file_key_1, json_file_path_1)
s3_client.download_file(bucket_name_2, json_file_key_2, json_file_path_2)
s3_client.download_file(bucket_name_2, json_file_key_3, json_file_path_3)

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Flatten, Clean, Transform and Save NDJSON") \
    .getOrCreate()

# Charger le fichier JSON en DataFrame, en ignorant la clé "pagination"
df = spark.read.option("multiline","true").json(json_file_path_1)

# Explosion de 'data' pour créer une ligne par entrée
df_exploded = df.select(explode(col("data")).alias("flight_info"))

# Aplatir les champs imbriqués à l'intérieur de "flight_info"
flattened_df = df_exploded.select(
    # Champs simples de l'objet "flight_info"
    col("flight_info.flight_date").alias("flight_date"),

    # Aplatir "departure" (un champ imbriqué)
    col("flight_info.departure.scheduled").alias("departure_scheduled"),
    col("flight_info.departure.airport").alias("departure_airport"),
    col("flight_info.departure.timezone").alias("departure_timezone"),
    col("flight_info.departure.iata").alias("departure_iata"),
    
    # Aplatir "arrival" (un champ imbriqué)
    col("flight_info.arrival.scheduled").alias("arrival_scheduled"),
    col("flight_info.arrival.airport").alias("arrival_airport"),
    col("flight_info.arrival.timezone").alias("arrival_timezone"),
    col("flight_info.arrival.iata").alias("arrival_iata"),
    
    # Aplatir "airline" (un champ imbriqué)
    col("flight_info.airline.name").alias("airline_name"),
    
    # Aplatir "flight" (un champ imbriqué)
    col("flight_info.flight.number").alias("flight_number"),
    col("flight_info.flight.iata").alias("flight_iata")
)

# Convertir les colonnes de date/heure en formats appropriés (timestamp)
flattened_df = flattened_df.withColumn("flight_date", to_timestamp("flight_date", "yyyy-MM-dd"))
flattened_df = flattened_df.withColumn("departure_scheduled", to_timestamp("departure_scheduled"))
flattened_df = flattened_df.withColumn("arrival_scheduled", to_timestamp("arrival_scheduled"))

# Supprimer les lignes contenant des valeurs nulles
flattened_df = flattened_df.dropna()

# Afficher quelques résultats pour vérification
flattened_df.show(truncate=False)

airport_schema = "airport_id INT, name STRING, city STRING, country STRING, iata STRING, icao STRING, latitude DOUBLE, longitude DOUBLE, altitude INT, timezone_offset INT, dst STRING, tz_database STRING, type STRING, source STRING"
airport_df = spark.read.format("csv").schema(airport_schema).load("/app/airports.dat")
airport_coords = airport_df.select(col("iata"), col("latitude"), col("longitude"))

flights_with_coords = flattened_df \
    .join(airport_coords.withColumnRenamed("latitude", "departure_latitude")
                        .withColumnRenamed("longitude", "departure_longitude"),
          flattened_df.departure_iata == airport_coords.iata, "left") \
    .drop("iata") \
    .join(airport_coords.withColumnRenamed("latitude", "arrival_latitude")
                        .withColumnRenamed("longitude", "arrival_longitude"),
          flattened_df.arrival_iata == airport_coords.iata, "left") \
    .drop("iata")

# Ajouter des colonnes GeoPoint
flights_with_coords = flights_with_coords \
    .withColumn("departure_geopoint", concat_ws(",", col("departure_latitude"), col("departure_longitude"))) \
    .withColumn("arrival_geopoint", concat_ws(",", col("arrival_latitude"), col("arrival_longitude")))

# Afficher quelques colonnes geopoint
flights_with_coords.select("flight_number", "departure_geopoint", "arrival_geopoint").show(5)


# --- TRANSFORMATIONS ET ANALYSES SUPPLÉMENTAIRES ---

# Extraire des informations temporelles supplémentaires (jour de la semaine, mois, année, heure)
flights_with_coords = flights_with_coords.withColumn("departure_day", dayofweek("departure_scheduled")) \
       .withColumn("arrival_day", dayofweek("arrival_scheduled"))

# Extraire les heures de départ et d'arrivée
flights_with_coords = flights_with_coords.withColumn("departure_hour", hour("departure_scheduled")) \
       .withColumn("arrival_hour", hour("arrival_scheduled"))

# Extraire la durée du vol

# Convertir en secondes UNIX
flights_with_coords = flights_with_coords.withColumn("departure_unix", unix_timestamp("departure_scheduled"))
flights_with_coords = flights_with_coords.withColumn("arrival_unix", unix_timestamp("arrival_scheduled"))

# Calculer la durée du vol en heures et l'arrondir à 1 chiffre après la virgule
flights_with_coords = flights_with_coords.withColumn(
    "flight_duration_hours", 
    round((col("arrival_unix") - col("departure_unix")) / 3600, 1)
)

# Afficher le résultat
flights_with_coords.select("flight_duration_hours").show()

# Générer un UUID unique pour chaque ligne
def generate_uuid():
    return str(uuid.uuid4())
generate_uuid_udf = udf(generate_uuid, StringType())
flights_with_coords = flights_with_coords.withColumn("unique_id", generate_uuid_udf())

# Formater les dates après toutes les transformations
flights_with_coords = flights_with_coords.withColumn("departure_scheduled", date_format("departure_scheduled", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
flights_with_coords = flights_with_coords.withColumn("arrival_scheduled", date_format("arrival_scheduled", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
flights_with_coords = flights_with_coords.withColumn("flight_date", date_format("flight_date", "yyyy-MM-dd"))

# Filtrer les lignes où les géopoints ou d'autres colonnes critiques sont nulles
valid_flights = flights_with_coords.filter(
    (col("departure_latitude").isNotNull()) &   
    (col("departure_longitude").isNotNull()) & 
    (col("arrival_latitude").isNotNull()) &     
    (col("arrival_longitude").isNotNull()) &   
    (col("departure_geopoint").isNotNull()) &  
    (col("arrival_geopoint").isNotNull()) &     
    (col("flight_date").isNotNull()) &          
    (col("flight_number").isNotNull()) &        
    (col("airline_name").isNotNull()) &         
    (col("departure_iata").isNotNull()) &       
    (col("arrival_iata").isNotNull()) &           
    (col("departure_scheduled").isNotNull()) &
    (col("departure_unix").isNotNull()) &
    (col("departure_airport").isNotNull()) &
    (col("departure_timezone").isNotNull()) &
    (col("arrival_scheduled").isNotNull()) &
    (col("arrival_unix").isNotNull()) &
    (col("arrival_airport").isNotNull()) &
    (col("arrival_timezone").isNotNull()) &
    (col("flight_number").isNotNull()) &
    (col("flight_iata").isNotNull()) &
    (col("departure_day").isNotNull()) &
    (col("arrival_day").isNotNull()) &
    (col("departure_hour").isNotNull()) &
    (col("arrival_hour").isNotNull()) &
    (col("flight_duration_hours").isNotNull()) &
    (col("flight_duration_hours") > 0)
)

# Configurer Elasticsearch
es = Elasticsearch(["http://172.17.0.1:9200"])   
index_name = "viken_khatcherian_m2i_cdsd_bloc1_aviationstack"
mapping = {
    "mappings": {
        "properties": {
            "departure_geopoint": {"type": "geo_point"},
            "arrival_geopoint": {"type": "geo_point"}
        }
    }
}

if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name, body=mapping)
    print(f"Index {index_name} created with GeoPoint mapping.")
else:
    print(f"Index {index_name} already exists.")

# Préparer et envoyer les données à Elasticsearch
def generate_bulk_data(row):
    action = {
        "_op_type": "index",
        "_index": index_name,
        "_id": f"{row.flight_number}_{row.flight_date}_{row.departure_hour}",
        "_source": {
            "flight_date": row.flight_date,      
            "flight_number": row.flight_number,
            "flight_iata": row.flight_iata,
            "airline_name": row.airline_name,
            "departure_iata": row.departure_iata,
            "arrival_iata": row.arrival_iata,
            "departure_geopoint": row.departure_geopoint,
            "arrival_geopoint": row.arrival_geopoint,
            "departure_scheduled": row.departure_scheduled,
            "arrival_scheduled": row.arrival_scheduled,
            "departure_hour": row.departure_hour,
            "arrival_hour" : row.arrival_hour,
            "flight_duration_hours": row.flight_duration_hours,  
            "departure_timezone": row.departure_timezone, 
            "arrival_timezone": row.arrival_timezone   
        }
    }
    return action

# Convertir les données valides en format bulk pour Elasticsearch
es_bulk_data = valid_flights.rdd.map(generate_bulk_data).collect()

# Envoyer les données à Elasticsearch via bulk
success, failed = bulk(es, es_bulk_data)
print(f"Successfully indexed: {success}, Failed: {failed}")

# ---- Traitement du fichier opensky_data.json ----
df_2 = spark.read.option("multiline", "true").json(json_file_path_2)

states_cleaned = df_2.select(
    col("icao24").alias("icao24"),  
    col("origin_country").alias("origin_country"),  
    from_unixtime(col("time_position")).alias("time_position_date"),  # Convertir en date type text sinon faire to_timestamp(col("time_position")).alias("time_position_date") pour type date
    col("longitude").alias("longitude"),  # Longitude
    col("latitude").alias("latitude"),  # Latitude
    concat_ws(",", col("latitude"), col("longitude")).alias("geopoint"),  # Créer une colonne GeoPoint
    col("altitude").alias("altitude"),
    col("on_ground").alias("on_ground"),
    col("velocity").alias("velocity"),
    col("heading").alias("heading"),
    col("vertical_rate").alias("vertical_rate"),
    col("squawk").alias("squawk"),
    date_format(from_unixtime(col("time_position")), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp")  # Colonne compatible Elasticsearch
)

# Ajouter un UUID unique
generate_uuid_udf = udf(generate_uuid, StringType())
states_cleaned = states_cleaned.withColumn("unique_id", generate_uuid_udf())

# Filtrer les lignes où les géopoints ou d'autres colonnes critiques sont nulles
valid_states_cleaned = states_cleaned.filter(
    (col("icao24").isNotNull()) &   
    (col("origin_country").isNotNull()) &
    (col("time_position_date").isNotNull()) &     
    (col("longitude").isNotNull()) &  
    (col("latitude").isNotNull()) &  
    (col("geopoint").isNotNull()) &      
    (col("altitude").isNotNull()) &
    (col("on_ground") == "false") &
    (col("velocity").isNotNull()) &
    (col("heading").isNotNull()) &
    (col("vertical_rate").isNotNull()) &
    (col("squawk").isNotNull()) &
    (col("timestamp").isNotNull())
)

# Afficher les lignes validées avant l'indexation
valid_states_cleaned.show(5)

index_name_2 = "viken_khatcherian_m2i_cdsd_bloc1_opensky"
mapping_2 = {
    "mappings": {
        "properties": {
            "geopoint": {"type": "geo_point"}
        }
    }
}

if not es.indices.exists(index=index_name_2):
    es.indices.create(index=index_name_2, body=mapping_2)
    print(f"Index {index_name_2} created with GeoPoint mapping.")
else:
    print(f"Index {index_name_2} already exists.")

# Préparer et envoyer les données à Elasticsearch
def generate_bulk_data_2(row):
    action = {
        "_op_type": "index",
        "_index": index_name_2,
        "_id": f"{row.icao24}_{row.timestamp}",
        "_source": {
            "icao24": row.icao24,       
            "origin_country": row.origin_country,
            "time_position_date": row.time_position_date,
            "longitude": row.longitude,
            "latitude": row.latitude,
            "geopoint": row.geopoint,
            "altitude": row.altitude,
            "on_ground": row.on_ground,
            "velocity": row.velocity,
            "heading": row.heading,
            "vertical_rate": row.vertical_rate,
            "squawk": row.squawk,           
            "timestamp": row.timestamp
        }
    }
    return action
    
# Convertir les données valides en format bulk pour Elasticsearch
es_bulk_data_2 = valid_states_cleaned.rdd.map(generate_bulk_data_2).collect()

# Envoyer les données à Elasticsearch via bulk
success, failed = bulk(es, es_bulk_data_2)
print(f"Successfully indexed: {success}, Failed: {failed}")

# ---- Traitement du fichier skytrax_data.json ----
df_3 = spark.read.option("multiline", "true").json(json_file_path_3)

# Fonction pour obtenir la date actuelle au format ISO 8601
def get_current_datetime():
    return datetime.datetime.now().isoformat()

airlines_cleaned = df_3.select(
    col("Airline").alias("airline"), 
    col("Stars").alias("stars")
)

# Ajouter un UUID unique
generate_uuid_udf = udf(generate_uuid, StringType())
airlines_cleaned = airlines_cleaned.withColumn("unique_id", generate_uuid_udf())

# Ajouter la colonne "indexed_at" avec la date actuelle
indexed_at_udf = udf(get_current_datetime, StringType())
airlines_cleaned = airlines_cleaned.withColumn("indexed_at", indexed_at_udf())

# Filtrer les lignes où les géopoints ou d'autres colonnes critiques sont nulles
valid_airlines_cleaned = airlines_cleaned.filter(
    (col("airline").isNotNull()) &  
    (col("stars").isNotNull())
)

# Afficher les lignes validées avant l'indexation
valid_airlines_cleaned.show(5)

# Configurer Elasticsearch  
index_name_3 = "viken_khatcherian_m2i_cdsd_bloc1_skytrax_stars"

if not es.indices.exists(index=index_name_3):
    es.indices.create(index=index_name_3)
    print(f"Index {index_name_3} created.")
else:
    print(f"Index {index_name_3} already exists.")

# Préparer et envoyer les données à Elasticsearch
def generate_bulk_data_3(row):
    # Utiliser l'airline et stars comme ID
    _id = f"{row.airline}_{row.stars}"
    
    action = {
        "_op_type": "update",  # Utiliser "update" pour mettre à jour si le document existe
        "_index": index_name_3,
        "_id": _id,  # Utiliser l'ID unique basé sur la compagnie et les étoiles
        "doc": {  # Mettre à jour le document existant
            "airline": row.airline,
            "stars": row.stars,
            "indexed_at": row.indexed_at  # Ajouter la date d'indexation
        },
        "doc_as_upsert": True  # Si le document n'existe pas, l'insérer
    }
    
    return action
    
# Convertir les données valides en format bulk pour Elasticsearch
es_bulk_data_3 = valid_airlines_cleaned.rdd.map(generate_bulk_data_3).collect()

# Envoyer les données à Elasticsearch via bulk
success, failed = bulk(es, es_bulk_data_3)
print(f"Successfully indexed: {success}, Failed: {failed}")

# Fermer la session Spark
spark.stop()

