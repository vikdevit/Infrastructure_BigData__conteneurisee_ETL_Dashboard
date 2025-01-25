import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import boto3
import json
import time

# Configuration du Kafka
KAFKA_TOPIC = 'aviation_data'
KAFKA_BROKER = 'kafka:9092'

# Configuration de S3 (via LocalStack)
S3_BUCKET_NAME = 'openskytrax'
S3_ENDPOINT = 'http://172.17.0.1:4566'
s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1')

# Fonction pour scraper les données Skytrax
def scrape_skytrax():
    url = "https://skytraxratings.com/a-z-of-airline-ratings"
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'id': 'tablepress-1'})
        if table:
            tbody = table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                resultats = []
                for row in rows:
                    stars = None
                    airline_name = ""
                    airline_url = ""
                    column1 = row.find('td', class_='column-1')
                    if column1:
                        stars_text = column1.get_text(strip=True)
                        if stars_text.isdigit():
                            stars = int(stars_text)
                    column2 = row.find('td', class_='column-2')
                    if column2:
                        link = column2.find('a')
                        airline_name = link.text.strip() if link else ""
                    if airline_name:
                        resultats.append({"Airline": airline_name, "Stars": stars})
                return resultats
    return []

# Fonction pour récupérer les données depuis l'API OpenSky Network
def fetch_opensky_data():
    url = "https://opensky-network.org/api/states/all"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        states = data.get("states", [])
        resultats = []
        for state in states:
            resultats.append({
                "icao24": state[0] if len(state) > 0 else None,
                "callsign": state[1] if len(state) > 1 else None,
                "origin_country": state[2] if len(state) > 2 else None,
                "time_position": state[3] if len(state) > 3 else None,
                "last_contact": state[4] if len(state) > 4 else None,
                "longitude": state[5] if len(state) > 5 else None,
                "latitude": state[6] if len(state) > 6 else None,
                "altitude": state[7] if len(state) > 7 else None,
                "on_ground": state[8] if len(state) > 8 else None,
                "velocity": state[9] if len(state) > 9 else None,
                "heading": state[10] if len(state) > 10 else None,
                "vertical_rate": state[11] if len(state) > 11 else None,
                "geo_altitude": state[12] if len(state) > 12 else None,
                "squawk": state[13] if len(state) > 13 else None,
                "spi": state[14] if len(state) > 14 else None,
            })
        return resultats
    return []

# Fonction pour envoyer les données vers Kafka
def send_to_kafka(producer, data):
    if not data:
        print("Aucune donnée à envoyer à Kafka")
    else:
        try:
            producer.produce(KAFKA_TOPIC, key="aviation_data", value=json.dumps(data))
            producer.flush()
            print(f"Données envoyées avec succès au topic {KAFKA_TOPIC}")
        except KafkaException as e:
            print(f"Erreur Kafka lors de l'envoi des données : {e}")

# Fonction pour consommer les données depuis Kafka et les envoyer vers S3
def consume_from_kafka_and_send_to_s3(consumer):
    skytrax_data = []
    opensky_data = []

    try:
        consumer.subscribe([KAFKA_TOPIC])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Fin de partition atteinte pour le topic {msg.topic()}, offset {msg.offset()}")
                else:
                    print(f"Erreur consommateur Kafka : {msg.error()}")
            else:
                message_data = json.loads(msg.value())
                if "Airline" in message_data:
                    skytrax_data.append(message_data)
                elif "icao24" in message_data:
                    opensky_data.append(message_data)

            if skytrax_data:
                skytrax_json = json.dumps(skytrax_data, indent=4)
                s3.put_object(Bucket=S3_BUCKET_NAME, Key="skytrax_data.json", Body=skytrax_json)
                print("Données Skytrax sauvegardées avec succès dans S3.")
                skytrax_data = []

            if opensky_data:
                opensky_json = json.dumps(opensky_data, indent=4)
                s3.put_object(Bucket=S3_BUCKET_NAME, Key="opensky_data.json", Body=opensky_json)
                print("Données OpenSky sauvegardées avec succès dans S3.")
                opensky_data = []

    except KafkaException as e:
        print(f"Erreur Kafka lors de la consommation : {e}")

# Main du script
if __name__ == "__main__":
    try:
        print("Initialisation du producteur Kafka...")
        producer = Producer({'bootstrap.servers': KAFKA_BROKER})

        print("Initialisation du consommateur Kafka...")
        consumer = Consumer({'bootstrap.servers': KAFKA_BROKER, 'group.id': 'aviation_group', 'auto.offset.reset': 'earliest'})

        # Scraper les données Skytrax
        print("Démarrage du scraping des données Skytrax...")
        scraped_data = scrape_skytrax()
        print("Scraping terminé, données extraites.")

        # Envoyer les données Skytrax à Kafka
        print("Envoi des données Skytrax à Kafka...")
        send_to_kafka(producer, scraped_data)

        # Récupérer les données OpenSky
        print("Récupération des données OpenSky...")
        opensky_data = fetch_opensky_data()
        print("Données OpenSky récupérées.")

        # Envoyer les données OpenSky à Kafka
        print("Envoi des données OpenSky à Kafka...")
        send_to_kafka(producer, opensky_data)

        # Consommer les données depuis Kafka et les envoyer vers S3
        print("Consommation des données depuis Kafka et envoi vers S3...")
        consume_from_kafka_and_send_to_s3(consumer)

    except Exception as main_error:
        print(f"Erreur dans le script principal : {main_error}")

