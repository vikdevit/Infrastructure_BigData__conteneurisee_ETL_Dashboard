# Utilisez l'image pré-configurée de Spark avec PySpark
FROM bitnami/spark:3.3.1

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Mettre à jour pip et installer les dépendances Python nécessaires
RUN pip install --upgrade pip
RUN pip install pyspark boto3 elasticsearch

# Copier le fichier des coordonnées des aéroports
COPY airports.dat /app/airports.dat

# Copier votre script PySpark dans le conteneur
COPY script.py /app/script.py

# Exécuter le script Python (PySpark) lors du démarrage du conteneur
CMD ["python", "/app/script.py"]



