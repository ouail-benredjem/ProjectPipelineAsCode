import requests
import json
import os
import random
import time
from datetime import datetime, timedelta
import boto3
import argparse

# Utiliser les variables d'environnement si disponibles
default_minio_url = os.environ.get('MINIO_URL', 'http://172.17.16.1:9002')
default_minio_access_key = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
default_minio_secret_key = os.environ.get('MINIO_SECRET_KEY', 'minioadmin')

OPENWEATHER_KEY="1b930df782fb8928244dec558ecb466c"

def fetch_current_weather(api_key, city="New York"):
    """
    Récupère les données météo actuelles pour une ville donnée
    """
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Erreur lors de la récupération des données météo : {response.status_code}")
    
    return response.json()

def modify_timestamp(data, target_date):
    """
    Modifie le timestamp des données météo pour simuler des données historiques
    """
    # Copie des données pour ne pas modifier l'original
    modified_data = data.copy()
    
    # Remplace le timestamp par celui de la date cible
    modified_data['dt'] = int(target_date.timestamp())
    
    # Ajout de variation aléatoire pour simuler différentes conditions météo
    temp_variation = random.uniform(-5, 5)  # Variation de température en °C
    modified_data['main']['temp'] += temp_variation
    modified_data['main']['feels_like'] += temp_variation
    modified_data['main']['humidity'] = min(100, max(30, modified_data['main']['humidity'] + random.randint(-15, 15)))
    modified_data['wind']['speed'] += random.uniform(-2, 2)
    
    # Ajouter différentes catégories météo en fonction de la température et d'autres facteurs
    temp = modified_data['main']['temp']
    humidity = modified_data['main']['humidity']
    wind_speed = modified_data['wind']['speed']
    
    # Définir les catégories météo possibles
    weather_categories = [
        "Clear", "Partly Cloudy", "Cloudy", "Rainy", "Stormy", "Snowy", "Foggy", "Windy"
    ]
    
    # Pondérer les probabilités en fonction des conditions
    weights = [1, 1, 1, 1, 1, 1, 1, 1]  # Probabilités égales par défaut
    
    # Ajuster les poids en fonction de la température
    if temp < 0:
        weights[5] = 5  # Snowy
        weights[0] = 0.5  # Clear
    elif temp < 10:
        weights[2] = 3  # Cloudy
        weights[6] = 2  # Foggy
    elif temp > 25:
        weights[0] = 3  # Clear
        weights[1] = 2  # Partly Cloudy
    
    # Ajuster les poids en fonction de l'humidité
    if humidity > 80:
        weights[3] = 3  # Rainy
        weights[4] = 2  # Stormy
        weights[6] = 2  # Foggy
    
    # Ajuster les poids en fonction de la vitesse du vent
    if wind_speed > 5:
        weights[7] = 3  # Windy
        weights[4] = 2  # Stormy
    
    # Choisir une catégorie météo au hasard en tenant compte des poids
    chosen_category = random.choices(weather_categories, weights=weights, k=1)[0]
    
    # Mettre à jour les données météo
    if 'weather' not in modified_data or not modified_data['weather']:
        modified_data['weather'] = [{}]
    
    modified_data['weather'][0]['main'] = chosen_category
    modified_data['weather'][0]['description'] = f"{chosen_category.lower()} conditions"
    modified_data['weather_category'] = chosen_category
    
    # Ajouter une indication que ce sont des données simulées
    if 'note' not in modified_data:
        modified_data['note'] = 'Données historiques simulées pour correspondre aux données taxi de janvier 2023'
    
    return modified_data

def save_to_minio(data, date, bucket_name, endpoint_url, access_key, secret_key):
    """
    Sauvegarde les données météo dans MinIO
    """
    # Format du timestamp pour le nom de fichier et le chemin
    timestamp_str = date.strftime("%Y-%m-%dT%H-%M-%S")
    file_name = f"weather_{timestamp_str}.json"
    file_path = file_name
    
    # Sauvegarde temporaire locale
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
    
    # Configuration du client MinIO
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='us-east-1'
    )
    
    # Vérifie si le bucket existe, sinon le crée
    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception as e:
        print(f"Le bucket '{bucket_name}' n'existe pas encore. Création en cours...")
        s3.create_bucket(Bucket=bucket_name)
    
    # Chemin dans MinIO : année/mois/jour/heure/fichier.json
    minio_path = f"{date.strftime('%Y/%m/%d/%H')}/{file_name}"
    
    # Upload du fichier
    s3.upload_file(file_path, bucket_name, minio_path)
    print(f"[OK] Données météo pour {date.strftime('%Y-%m-%d %H:%M:%S')} uploadées dans MinIO: {bucket_name}/{minio_path}")
    
    # Suppression du fichier local temporaire
    os.remove(file_path)
    
    return minio_path

def generate_historical_weather_data(start_date, end_date, interval_hours, api_key, 
                                 bucket_name, endpoint_url, access_key, secret_key):
    """
    Génère des données météo historiques simulées pour une période donnée
    """
    # Récupère les données météo actuelles comme base
    current_weather = fetch_current_weather(api_key)
    
    # Génère des données pour chaque intervalle dans la période
    current_date = start_date
    paths = []
    
    print(f"Génération de données météo historiques du {start_date} au {end_date} toutes les {interval_hours} heures")
    
    count = 0
    while current_date <= end_date:
        # Modifie les données avec le timestamp historique
        modified_data = modify_timestamp(current_weather, current_date)
        
        # Sauvegarde dans MinIO
        path = save_to_minio(modified_data, current_date, bucket_name, endpoint_url, access_key, secret_key)
        paths.append(path)
        
        # Avance à la prochaine date
        current_date += timedelta(hours=interval_hours)
        count += 1
        
        # Petite pause pour éviter de surcharger l'API ou MinIO
        if count % 10 == 0:
            print(f"Généré {count} enregistrements météo...")
            time.sleep(0.5)
    
    print(f"Génération terminée ! {count} enregistrements météo historiques créés.")
    return paths

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Générateur de données météo historiques")
    
    # Paramètres pour la période de simulation
    parser.add_argument("--start-date", type=str, default="2023-01-01", 
                       help="Date de début (format: YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2023-01-31", 
                       help="Date de fin (format: YYYY-MM-DD)")
    parser.add_argument("--interval", type=int, default=1, 
                       help="Intervalle en heures entre chaque enregistrement")
    
    # Paramètres OpenWeather
    parser.add_argument("--api-key", type=str, default=OPENWEATHER_KEY, 
                       help="Clé API OpenWeatherMap")
    
    # Paramètres MinIO
    parser.add_argument("--bucket", type=str, default="weather-data", 
                       help="Nom du bucket MinIO")
    parser.add_argument("--endpoint", type=str, default=default_minio_url, 
                       help="URL de l'endpoint MinIO")
    parser.add_argument("--access-key", type=str, default=default_minio_access_key, 
                       help="Clé d'accès MinIO")
    parser.add_argument("--secret-key", type=str, default=default_minio_secret_key, 
                       help="Clé secrète MinIO")
    
    args = parser.parse_args()
    
    # Convertit les dates en objets datetime
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
    
    # Génère les données météo historiques
    generate_historical_weather_data(
        start_date, end_date, args.interval, args.api_key,
        args.bucket, args.endpoint, args.access_key, args.secret_key
    )
