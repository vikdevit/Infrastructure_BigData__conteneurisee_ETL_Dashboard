# Utiliser l'image officielle Python 3.12
FROM python:3.12-slim

# Créer un environnement virtuel
RUN python -m venv /opt/venv

# Activer l'environnement virtuel en définissant la variable d'environnement
ENV PATH="/opt/venv/bin:$PATH"

# Définir le répertoire de travail
WORKDIR /app

# Copier le fichier des dépendances
COPY requirements.txt .

# Installer les dépendances dans l'environnement virtuel
RUN pip install --no-cache-dir -r requirements.txt

# Copier les scripts Python dans le conteneur
COPY script/ /app/script/

# Commande par défaut pour démarrer le scraper
CMD ["python", "/app/script/scrap.py"]
