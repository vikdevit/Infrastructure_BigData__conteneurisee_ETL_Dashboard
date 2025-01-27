from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, hour, dayofweek, month, year, current_timestamp, unix_timestamp, count, lit, row_number, date_format, udf, concat_ws, from_unixtime
from pyspark.sql.window import Window
import boto3
import json
import uuid
from pyspark.sql.types import StringType

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# Configurer l'accès à LocalStack via boto3
s3_client = boto3.client('s3',
                         endpoint_url='http://172.17.0.1:4566',
                         aws_access_key_id='test',
                         aws_secret_access_key='test',
                         region_name='us-east-1')

# Téléchargement du fichier JSON depuis le bucket S3 simulé
bucket_name = 'aviationstack'
json_file_key = 'aviation-data.json'  # Nom du fichier dans le bucket S3 (pas de répertoire)
json_file_path = '/app/aviation-data.json'  # Chemin local dans le conteneur Docker

"""vk_bucket = 'bucketvk'
vk_json_file = 'opensky_data.json'
vk_local_path = '/opensky_data.json'"""

# Télécharger le fichier S3 dans le conteneur local
s3_client.download_file(bucket_name, json_file_key, json_file_path)
#s3_client.download_file(vk_bucket, vk_json_file, vk_local_path)

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Flatten, Clean, Transform and Save NDJSON") \
    .getOrCreate()

# Charger le fichier JSON en DataFrame, en ignorant la clé "pagination"
df = spark.read.option("multiline","true").json(json_file_path)

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

# Calculer la durée du vol en minutes
flights_with_coords = flights_with_coords.withColumn("flight_duration_minutes", 
                   (col("arrival_scheduled").cast("long") - col("departure_scheduled").cast("long")) / 60)

# Analyser les vols par compagnie aérienne
"""flights_with_coords = flights_with_coords.withColumn("airline_count", 
                   count("airline_name").over(Window.partitionBy("airline_name")))"""

# Analyser les vols par aéroport de départ
"""flights_with_coords = flights_with_coords.withColumn("departure_airport_count", 
                   count("departure_airport").over(Window.partitionBy("departure_airport")))"""

# Analyser les vols par aéroport d'arrivée
"""flights_with_coords = flights_with_coords.withColumn("arrival_airport_count", 
                   count("arrival_airport").over(Window.partitionBy("arrival_airport")))"""

# Extraire le fuseau horaire de départ et d'arrivée
"""flights_with_coords = flights_with_coords.withColumn("departure_timezone", col("departure_timezone")) \
       .withColumn("arrival_timezone", col("arrival_timezone"))"""

# Générer un UUID unique pour chaque ligne
def generate_uuid():
    return str(uuid.uuid4())
generate_uuid_udf = udf(generate_uuid, StringType())
flights_with_coords = flights_with_coords.withColumn("unique_id", generate_uuid_udf())

# **Maintenant, formater les dates après toutes les transformations**
flights_with_coords = flights_with_coords.withColumn("departure_scheduled", date_format("departure_scheduled", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
flights_with_coords = flights_with_coords.withColumn("arrival_scheduled", date_format("arrival_scheduled", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
flights_with_coords = flights_with_coords.withColumn("flight_date", date_format("flight_date", "yyyy-MM-dd"))

# Filtrer les lignes où les géopoints ou d'autres colonnes critiques sont NULL
valid_flights = flights_with_coords.filter(
    (col("departure_latitude").isNotNull()) &   # Vérifie la latitude de départ
    (col("departure_longitude").isNotNull()) &  # Vérifie la longitude de départ
    (col("arrival_latitude").isNotNull()) &     # Vérifie la latitude d'arrivée
    (col("arrival_longitude").isNotNull()) &    # Vérifie la longitude d'arrivée
    (col("departure_geopoint").isNotNull()) &   # Vérifie le géopoint de départ
    (col("arrival_geopoint").isNotNull()) &     # Vérifie le géopoint d'arrivée
    (col("flight_date").isNotNull()) &          # Vérifie la date de vol # Vérifie le statut du vol
    (col("flight_number").isNotNull()) &        # Vérifie le numéro du vol
    (col("airline_name").isNotNull()) &         # Vérifie le nom de la compagnie aérienne
    (col("departure_iata").isNotNull()) &       # Vérifie l'IATA du départ
    (col("arrival_iata").isNotNull()) &           # Vérifie l'IATA d'arrivée
    (col("departure_scheduled").isNotNull()) &
    (col("departure_airport").isNotNull()) &
    (col("departure_timezone").isNotNull()) &
    (col("arrival_scheduled").isNotNull()) &
    (col("arrival_airport").isNotNull()) &
    (col("arrival_timezone").isNotNull()) &
    (col("flight_number").isNotNull()) &
    (col("flight_iata").isNotNull()) &
    (col("departure_day").isNotNull()) &
    (col("arrival_day").isNotNull()) &
    (col("departure_hour").isNotNull()) &
    (col("arrival_hour").isNotNull()) &
    (col("flight_duration_minutes").isNotNull())
)

# Afficher les lignes validées avant l'indexation
#valid_flights.show(truncate=False)

# Configurer Elasticsearch
es = Elasticsearch(["http://172.17.0.1:9200"])   
index_name = "aviation_data_0_2501_index"
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
        "_id": row.unique_id,
        "_source": {
            "flight_date": row.flight_date,       #"flight_status": row.flight_status,
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
            "flight_duration_minutes": row.flight_duration_minutes,  #"departure_airport_count": row.departure_airport_count,
            "departure_timezone": row.departure_timezone,  #"arrival_airport_count": row.arrival_airport_count,
            "arrival_timezone": row.arrival_timezone   
        }
    }
    return action

# Convertir les données valides en format bulk pour Elasticsearch
es_bulk_data = valid_flights.rdd.map(generate_bulk_data).collect()

# Envoyer les données à Elasticsearch via bulk
success, failed = bulk(es, es_bulk_data)
print(f"Successfully indexed: {success}, Failed: {failed}")

"""# ---- 2. Traitement du fichier opensky_data.json ----
vk_df = spark.read.option("multiline", "true").json(vk_local_path)

states_cleaned = vk_df.select(
    col("icao24").alias("icao24"),  # ICAO24
    col("origin_country").alias("origin_country"),  # Origin country
    from_unixtime(col("time_position")).alias("time_position_date"),  # Convertir en date
    col("longitude").alias("longitude"),  # Longitude
    col("latitude").alias("latitude"),  # Latitude
    concat_ws(",", col("latitude"), col("longitude")).alias("geopoint")  # Créer une colonne GeoPoint
)

# Ajouter un UUID unique
generate_uuid_udf = udf(generate_uuid, StringType())
states_cleaned = states_cleaned.withColumn("unique_id", generate_uuid_udf())

# Filtrer les lignes où les géopoints ou d'autres colonnes critiques sont NULL
valid_states_cleaned = states_cleaned.filter(
    (col("icao24").isNotNull()) &   # Vérifie la latitude de départ
    (col("origin_country").isNotNull()) &  # Vérifie la longitude de départ
    (col("time_position_date").isNotNull()) &     # Vérifie la latitude d'arrivée
    (col("longitude").isNotNull()) &   # Vérifie la latitude de départ
    (col("latitude").isNotNull()) &  # Vérifie la longitude de départ
    (col("geopoint").isNotNull())      # Vérifie la latitude d'arrivée
)

index_name_2 = "opensky_data_22_index"
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
        "_id": row.unique_id,
        "_source": {
            "icao24": row.icao24,       #"flight_status": row.flight_status,
            "origin_country": row.origin_country,
            "time_position_date": row.time_position_date,
            "longitude": row.longitude,
            "latitude": row.latitude,
            "geopoint": row.geopoint
        }
    }
    return action

# Convertir les données valides en format bulk pour Elasticsearch
es_bulk_data_2 = valid_states_cleaned.rdd.map(generate_bulk_data_2).collect()

# Envoyer les données à Elasticsearch via bulk
success, failed = bulk(es, es_bulk_data_2)
print(f"Successfully indexed: {success}, Failed: {failed}")"""

# Stop Spark session
spark.stop()
