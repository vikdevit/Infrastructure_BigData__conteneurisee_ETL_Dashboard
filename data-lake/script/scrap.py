import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import boto3
import json

# Configuration de Kafka
KAFKA_TOPIC = 'aviation_data'
KAFKA_BROKER = 'kafka:9092'

# Configuration de S3 (via Localstack)
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
                        airline_url = link['href'] if link and 'href' in link.attrs else ""
                    if airline_name:
                        resultats.append({"Airline": airline_name, "Stars": stars, "URL": airline_url})
                return resultats
    return []

# Fonction pour récupérer les données OpenSky
def fetch_opensky_data():
    url = "https://opensky-network.org/api/states/all"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Limiter aux 50 premiers états
        states = data.get("states", [])[:50]  # On prend seulement les 50 premiers états
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
    else:
        print(f"Erreur lors de la récupération des données OpenSky. Code HTTP: {response.status_code}")
        return []

# Fonction pour envoyer les données vers Kafka
def send_to_kafka(producer, data, source):
    if not data:
        print(f"Aucune donnée à envoyer à Kafka depuis {source}")
    else:
        try:
            print(f"Envoi des données à Kafka depuis {source}")
            producer.produce(KAFKA_TOPIC, key=source, value=json.dumps(data))
            producer.flush()
            print(f"Données envoyées avec succès au topic {KAFKA_TOPIC} depuis {source}")
        except KafkaException as e:
            print(f"Erreur Kafka lors de l'envoi des données : {e}")

# Fonction pour consommer les données depuis Kafka et les envoyer vers S3
def consume_from_kafka_and_send_to_s3(consumer):
    try:
        print(f"Abonnement au topic {KAFKA_TOPIC}")
        consumer.subscribe([KAFKA_TOPIC])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print("Aucun message reçu durant ce cycle.")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Fin de partition atteinte pour le topic {msg.topic()}, offset {msg.offset()}")
                else:
                    print(f"Erreur consommateur Kafka : {msg.error()}")
            else:
                source = msg.key().decode('utf-8')  # Récupérer la source (Skytrax ou OpenSky)
                data = json.loads(msg.value().decode('utf-8'))
                print(f"Message reçu de {source}: {json.dumps(data, indent=4)}")
                try:
                    if source == "Skytrax":
                        key = "skytrax_data.json"
                    elif source == "OpenSky":
                        key = "opensky_data.json"
                    else:
                        key = f"unknown_data.json"                    
                    # Sauvegarde dans S3
                    s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=json.dumps(data))
                    print(f"Données sauvegardées avec succès dans le bucket {S3_BUCKET_NAME} avec la clé {key}")
                except Exception as s3_error:
                    print(f"Erreur lors de la sauvegarde dans S3 : {s3_error}")
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

        # Récupérer les données OpenSky
        print("Récupération des données OpenSky...")
        opensky_data = fetch_opensky_data()

        # Envoyer les données à Kafka
        print("Envoi des données Skytrax à Kafka...")
        send_to_kafka(producer, scraped_data, source="Skytrax")

        print("Envoi des données OpenSky à Kafka...")
        send_to_kafka(producer, opensky_data, source="OpenSky")

        # Consommer les données depuis Kafka et les envoyer vers S3
        print("Consommation des données depuis Kafka et envoi vers S3...")
        consume_from_kafka_and_send_to_s3(consumer)
    except Exception as main_error:
        print(f"Erreur dans le script principal : {main_error}")

