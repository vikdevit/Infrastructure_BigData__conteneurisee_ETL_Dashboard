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
    # URL de la page à scraper
    url = "https://skytraxratings.com/a-z-of-airline-ratings"
    
    # Envoyer une requête HTTP pour récupérer le contenu HTML 
    response = requests.get(url)
    if response.status_code == 200:
        print("Page récupérée avec succès!")   
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Trouver la table avec l'ID 'tablepress-1'
        table = soup.find('table', {'id': 'tablepress-1'})
        if table:
            print("Table trouvée!")
            # Trouver la balise tbody
            tbody = table.find('tbody') if table else None
        
            if tbody:
                # Parcourir toutes les balises html <tr> dans <tbody>
                rows = tbody.find_all('tr')
                print(f"Nombre de lignes trouvées: {len(rows)}") # afficher le nombre de lignes trouvées
            
                # Création d'une liste pour stocker les résultats
                resultats = []

                for row in rows:
                    stars = None
                    airline_name = ""
                    airline_url = ""
                    
                    # Extraire la première colonne (nombre d'étoiles)
                    column1 = row.find('td', class_='column-1')
                    if column1:
                        # Afficher le contenu de la première colonne pour vérification
                        print(f"Column 1 : {column1.prettify()}")

                        # Extraire le texte de la colonne et convertir en entier
                        stars_text = column1.get_text(strip=True) # On prend le texte de la colonne et on supprime les espaces inutiles
                        if stars_text.isdigit(): # Si le texte est un nombre, on le convertit en entier
                            stars = int(stars_text)
              
                    # Extraire la deuxième colonne (nom et lien de la compagnie aérienne)
                    column2 = row.find('td', class_='column-2')
                    if column2:
                        link = column2.find('a')
                        airline_name = link.text.strip() if link else ""
                        airline_url = link['href'] if link and 'href' in link.attrs else ""

                    # Affichage des données extraites avant d'ajouter à la liste
                    print(f"Extrait - Airline: {airline_name}, Stars: {stars}, URL: {airline_url}")

                    # Si les données sont valides, les ajouter à la liste
                    if airline_name: # Même si les étoiles sont manquantes, on garde l'airline_name
                        resultats.append({"Airline": airline_name, "Stars": stars, "URL": airline_url})
                        print(f"Ajouté à la liste : {airline_name}, {stars}, {airline_url}")
                    else:
                        print("Données invalides ou manquantes.")
                    
                # Afficher tous les résultats extraits
                print(f"Résultats extraits: {json.dumps(resultats, indent=4)}")

            else:
                print("Aucune balise <tbody> trouvée dans la table.")

        else:
            print("Table avec l'ID 'tablepress-1' non trouvée.")

    else:
        print(f"Erreur: Impossible d'accéder à la page. Statut HTTP: {response.status_code}")
        
    return resultats

# Fonction pour envoyer les données vers Kafka
def send_to_kafka(producer, data):
    if not data:
        print("Aucune donnée à envoyer à Kafka") #Vérifie si les données sont vides
    else:
        try:
            print(f"Envoi des données à Kafka: {json.dumps(data, indent=4)}")  # Log des données avant envoi
            producer.produce(KAFKA_TOPIC, key="aviation_data", value=json.dumps(data))
            producer.flush()
            print(f"Données envoyées avec succès au topic {KAFKA_TOPIC}")
        except KafkaException as e:
            print(f"Erreur Kafka lors de l'envoi des données : {e}")
            print(f"Données ayant causé l'erreur : {json.dumps(data, indent=4)}")

# Fonction pour consommer les données depuis Kafka et les envoyer vers S3
def consume_from_kafka_and_send_to_s3(consumer):
    try:
        print(f"Abonnement au topic {KAFKA_TOPIC}")
        consumer.subscribe([KAFKA_TOPIC])
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print("Aucun message reçu durant ce cycle.")  # Log en cas d'attente
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Fin de partition atteinte pour le topic {msg.topic()}, offset {msg.offset()}")
                else:
                    print(f"Erreur consommateur Kafka : {msg.error()}")
            else:
                print(f"Message reçu: {msg.value().decode('utf-8')}")
                try:
                    # Sauvegarder dans S3
                    key = f"aviation_data_{int(time.time())}.json"
                    print(f"Tentative de sauvegarde dans S3 avec la clé : {key}")
                    s3.put_object(Bucket=S3_BUCKET_NAME, Key=key, Body=msg.value())
                    print(f"Données sauvegardées avec succès dans le bucket {S3_BUCKET_NAME} avec la clé {key}")
                except Exception as s3_error:
                    print(f"Erreur lors de la sauvegarde dans S3 : {s3_error}")
                    print(f"Données ayant causé l'erreur : {msg.value().decode('utf-8')}")
   
    except KafkaException as e:
        print(f"Erreur Kafka lors de la consommation : {e}")

# Main du scrap
if __name__ == "__main__":
    try:
        print("Initialisation du producteur Kafka...")
        producer = Producer({'bootstrap.servers': KAFKA_BROKER})
        print("Producteur Kafka initialisé avec succès.")

        print("Initialisation du consommateur Kafka...")
        consumer = Consumer({'bootstrap.servers': KAFKA_BROKER, 'group.id': 'aviation_group', 'auto.offset.reset': 'earliest'})
        print("Consommateur Kafka initialisé avec succès.")

        # Scraper les données
        print("Démarrage du scraping des données Skytrax...")
        scraped_data = scrape_skytrax()
        print("Scraping terminé, données extraites :")
        print(json.dumps(scraped_data, indent=4))

        # Envoyer les données à Kafka
        print("Envoi des données à Kafka...")
        send_to_kafka(producer, scraped_data)

        # Consommer les données depuis Kafka et les envoyer vers S3
        print("Consommation des données depuis Kafka et envoi vers S3...")
        consume_from_kafka_and_send_to_s3(consumer)
    except Exception as main_error:
        print(f"Erreur dans le script principal : {main_error}")    

