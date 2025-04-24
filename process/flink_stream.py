# flink_stream.py
# Script for streaming processing of Weather data using PyFlink
# Version simplifiée compatible avec l'installation limitée de PyFlink

import os
import json
import argparse
import wget
import boto3
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
from urllib.parse import quote_plus
import tempfile
import shutil

# Simule certaines fonctionnalités de Flink avec des outils standards
# puisque l'installation complète de PyFlink présente des problèmes

def categorize_weather(weather_condition):
    """
    Categorize weather conditions into broader categories
    """
    if not weather_condition:
        return "Unknown"
    
    # Lowercase for easier matching
    condition = weather_condition.lower()
    
    # Clear conditions
    if any(clear_term in condition for clear_term in ["clear", "sunny", "fair"]):
        return "Clear"
    
    # Rainy conditions
    if any(rain_term in condition for rain_term in ["rain", "drizzle", "shower"]):
        return "Rainy"
    
    # Stormy conditions
    if any(storm_term in condition for storm_term in ["storm", "thunder", "lightning", "thunderstorm"]):
        return "Stormy"
    
    # Cloudy conditions
    if any(cloudy_term in condition for cloudy_term in ["cloud", "overcast", "fog", "mist", "haze"]):
        return "Cloudy"
    
    # Snowy conditions
    if any(snow_term in condition for snow_term in ["snow", "sleet", "hail", "ice", "blizzard", "frost"]):
        return "Snowy"
    
    # Default fallback
    return "Other"

def download_jdbc_driver():
    """
    Télécharge le pilote JDBC PostgreSQL si nécessaire
    """
    jdbc_jar = "postgresql-42.3.1.jar"
    jdbc_path = os.path.join(os.getcwd(), jdbc_jar)
    
    if not os.path.exists(jdbc_path):
        print(f"Téléchargement du pilote JDBC PostgreSQL...")
        wget.download(
            "https://jdbc.postgresql.org/download/postgresql-42.3.1.jar",
            out=jdbc_path
        )
        print(f"\nPilote téléchargé dans {jdbc_path}")
    
    return jdbc_path

def download_from_minio(bucket_name, directory_path, minio_endpoint, minio_access_key, minio_secret_key):
    """
    Télécharge les fichiers JSON depuis MinIO
    """
    print(f"Téléchargement des données météo depuis {bucket_name}/{directory_path}...")
    
    # Créer un client MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )
    
    # Créer un répertoire temporaire pour stocker les fichiers téléchargés
    temp_dir = tempfile.mkdtemp()
    print(f"Répertoire temporaire créé: {temp_dir}")
    
    # Liste des fichiers dans le bucket/directory
    try:
        # Paginate through the results to handle large directories
        paginator = s3_client.get_paginator('list_objects_v2')
        all_files = []
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix=directory_path):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.json'):
                        all_files.append(obj['Key'])
        
        if not all_files:
            print(f"Aucun fichier JSON trouvé dans {bucket_name}/{directory_path}")
            return None
        
        print(f"{len(all_files)} fichiers JSON trouvés dans {bucket_name}/{directory_path}")
        
        # Télécharger tous les fichiers pour couvrir tout le mois
        download_count = len(all_files)
        files_to_download = all_files
        
        for i, file_key in enumerate(files_to_download):
            local_file_path = os.path.join(temp_dir, os.path.basename(file_key))
            s3_client.download_file(bucket_name, file_key, local_file_path)
            if (i + 1) % 10 == 0 or i == download_count - 1:
                print(f"Téléchargement en cours: {i + 1}/{download_count} fichiers")
        
        print(f"Téléchargement terminé. {download_count} fichiers sauvegardés dans {temp_dir}")
        return temp_dir
    
    except Exception as e:
        print(f"Erreur lors du téléchargement depuis MinIO: {str(e)}")
        shutil.rmtree(temp_dir)
        return None

def process_weather_data(temp_dir):
    """
    Traite les fichiers JSON météo téléchargés et les transforme
    """
    print("Traitement des données météo...")
    
    all_data = []
    json_files = [f for f in os.listdir(temp_dir) if f.endswith('.json')]
    
    for json_file in json_files:
        try:
            with open(os.path.join(temp_dir, json_file), 'r') as f:
                data = json.load(f)
                all_data.append(data)
        except Exception as e:
            print(f"Erreur lors de la lecture du fichier {json_file}: {str(e)}")
    
    print(f"{len(all_data)} fichiers JSON lus avec succès")
    
    if not all_data:
        return None
    
    # Transformer les données
    transformed_data = []
    
    for item in all_data:
        # Traiter les données météo pour chaque fichier
        try:
            # OpenWeatherMap timestamp est en secondes, pas en millisecondes
            timestamp = datetime.fromtimestamp(item['dt'])
            
            # Accéder aux données imbriquées
            if 'weather' in item and len(item['weather']) > 0:
                weather_main = item['weather'][0]['main']
                weather_description = item['weather'][0]['description']
            else:
                weather_main = "Unknown"
                weather_description = "Unknown"
            
            # Créer un enregistrement transformé
            transformed_item = {
                'timestamp': timestamp,
                'city': item.get('name', 'Unknown'),
                'temperature': item.get('main', {}).get('temp'),
                'feels_like': item.get('main', {}).get('feels_like'),
                'humidity': item.get('main', {}).get('humidity'),
                'wind_speed': item.get('wind', {}).get('speed'),
                'wind_direction': item.get('wind', {}).get('deg'),
                'weather_main': weather_main,
                'weather_description': weather_description,
                'weather_category': categorize_weather(weather_main),
                'pressure': item.get('main', {}).get('pressure'),
                'hour_of_day': timestamp.hour,
                'day_of_week': timestamp.weekday() + 1,  # 1 = lundi, 7 = dimanche
                'processing_time': datetime.now()
            }
            
            transformed_data.append(transformed_item)
        except Exception as e:
            print(f"Erreur lors de la transformation d'un élément: {str(e)}")
    
    print(f"{len(transformed_data)} enregistrements transformés")
    
    # Convertir en DataFrame pandas
    df = pd.DataFrame(transformed_data)
    
    return df

def save_to_postgres(df, pg_host, pg_port, pg_db, pg_user, pg_password, table_name="dim_weather"):
    """
    Sauvegarde les données transformées dans PostgreSQL
    """
    if df is None or df.empty:
        print("Aucune donnée à sauvegarder dans PostgreSQL")
        return False
    
    try:
        # Encoder le mot de passe pour gérer les caractères spéciaux
        encoded_password = quote_plus(pg_password)
        
        # Construire l'URL de connexion
        db_url = f"postgresql://{pg_user}:{encoded_password}@{pg_host}:{pg_port}/{pg_db}"
        
        # Créer un moteur SQLAlchemy
        engine = create_engine(db_url)
        
        print(f"Sauvegarde de {len(df)} enregistrements dans PostgreSQL, table {table_name}...")
        
        # Insérer les données dans PostgreSQL
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',
            index=False,
            chunksize=10000
        )
        
        print(f"Données sauvegardées avec succès dans la table {table_name}")
        return True
    
    except Exception as e:
        print(f"Erreur lors de la sauvegarde dans PostgreSQL: {str(e)}")
        return False

def main(bucket_name, directory_path, postgres_host, postgres_port, postgres_db, postgres_user, postgres_password, 
         minio_endpoint, minio_access_key, minio_secret_key):
    """
    Fonction principale pour orchestrer le processus de streaming simulé
    Utilise des bibliothèques standards pour contourner les limitations de PyFlink
    """
    try:
        # Télécharger le driver JDBC si nécessaire
        download_jdbc_driver()
        
        # Télécharger les données depuis MinIO
        temp_dir = download_from_minio(
            bucket_name, 
            directory_path, 
            minio_endpoint, 
            minio_access_key, 
            minio_secret_key
        )
        
        if not temp_dir:
            print("Impossible de télécharger les données depuis MinIO. Arrêt du traitement.")
            return
        
        try:
            # Traiter les données météo
            weather_df = process_weather_data(temp_dir)
            
            if weather_df is not None and not weather_df.empty:
                # Sauvegarder dans PostgreSQL
                save_to_postgres(
                    weather_df,
                    postgres_host,
                    postgres_port,
                    postgres_db,
                    postgres_user,
                    postgres_password
                )
                
                print("Traitement des données météo terminé avec succès")
            else:
                print("Aucune donnée météo à traiter")
        
        finally:
            # Nettoyer le répertoire temporaire
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                print(f"Répertoire temporaire {temp_dir} supprimé")
    
    except Exception as e:
        print(f"Erreur dans le traitement: {str(e)}")
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Traitement des données météo avec Flink (simulation)")
    
    # MinIO connection parameters
    parser.add_argument("--bucket", type=str, default="weather-data", help="MinIO bucket name")
    parser.add_argument("--directory-path", type=str, default="2023/01/", 
                        help="Directory path in MinIO où les fichiers météo JSON de janvier 2023 sont stockés")
    
    # PostgreSQL connection parameters
    parser.add_argument("--pg-host", type=str, default="localhost", help="PostgreSQL host")
    parser.add_argument("--pg-port", type=str, default="5432", help="PostgreSQL port")
    parser.add_argument("--pg-db", type=str, default="Minio", help="PostgreSQL database name")
    parser.add_argument("--pg-user", type=str, default="postgres", help="PostgreSQL username")
    parser.add_argument("--pg-password", type=str, default="Affili@12707", help="PostgreSQL password avec caractères spéciaux comme @ autorisés")
    parser.add_argument("--minio-endpoint", type=str, default="http://172.17.16.1:9002", help="MinIO endpoint URL")
    parser.add_argument("--minio-access-key", type=str, default="minioadmin", help="MinIO access key")
    parser.add_argument("--minio-secret-key", type=str, default="minioadmin", help="MinIO secret key")
    
    args = parser.parse_args()
    
    main(
        args.bucket,
        args.directory_path,
        args.pg_host,
        args.pg_port,
        args.pg_db,
        args.pg_user,
        args.pg_password,
        args.minio_endpoint,
        args.minio_access_key,
        args.minio_secret_key
    )
