#!/usr/bin/env python
# load_to_postgres.py
# Script pour charger les données de MinIO vers PostgreSQL

import os
import argparse
import boto3
import pandas as pd
import pyarrow.parquet as pq
import tempfile
import io
from sqlalchemy import create_engine

def get_minio_client(endpoint, access_key, secret_key):
    """
    Crée et renvoie un client MinIO
    """
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='us-east-1'  # Peut être n'importe quelle valeur pour MinIO
    )

def download_parquet_from_minio(minio_client, bucket, path, local_path):
    """
    Télécharge les fichiers parquet depuis MinIO et les sauvegarde localement
    """
    print(f"Téléchargement des données depuis {bucket}/{path}...")
    
    # Lister tous les objets dans le chemin donné
    objects = minio_client.list_objects_v2(Bucket=bucket, Prefix=path)
    
    if 'Contents' not in objects:
        raise ValueError(f"Aucun fichier trouvé dans {bucket}/{path}")
    
    # Télécharger chaque fichier parquet dans un dossier temporaire
    os.makedirs(local_path, exist_ok=True)
    
    for obj in objects['Contents']:
        if obj['Key'].endswith('.parquet'):
            file_name = os.path.basename(obj['Key'])
            local_file = os.path.join(local_path, file_name)
            
            print(f"Téléchargement de {obj['Key']} vers {local_file}")
            minio_client.download_file(bucket, obj['Key'], local_file)
    
    print(f"Téléchargement terminé. Fichiers sauvegardés dans {local_path}")

def load_parquet_to_dataframe(local_path):
    """
    Charge les fichiers parquet locaux dans un DataFrame pandas
    """
    print(f"Chargement des fichiers parquet depuis {local_path}...")
    
    # Trouver tous les fichiers parquet dans le dossier
    parquet_files = [os.path.join(local_path, f) for f in os.listdir(local_path) 
                    if f.endswith('.parquet')]
    
    if not parquet_files:
        raise ValueError(f"Aucun fichier parquet trouvé dans {local_path}")
    
    # Charger et concatener tous les fichiers parquet
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        dfs.append(df)
    
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"DataFrame chargé avec {len(combined_df)} lignes")
    
    return combined_df

def save_to_postgres(df, db_connection_string, table_name):
    """
    Sauvegarde le DataFrame dans PostgreSQL
    """
    print(f"Sauvegarde des données dans PostgreSQL, table {table_name}...")
    
    engine = create_engine(db_connection_string)
    
    # Créer la table et insérer les données
    df.to_sql(
        table_name,
        engine,
        if_exists='replace',
        index=False,
        chunksize=10000  # Insérer par lots pour éviter les problèmes de mémoire
    )
    
    print(f"Données sauvegardées avec succès dans la table {table_name}")

def main():
    parser = argparse.ArgumentParser(description="Chargement des données de MinIO vers PostgreSQL")
    
    # MinIO parameters
    parser.add_argument("--minio-endpoint", type=str, default="http://172.17.16.1:9002",
                       help="URL du endpoint MinIO")
    parser.add_argument("--minio-access-key", type=str, default="minioadmin",
                       help="Clé d'accès MinIO")
    parser.add_argument("--minio-secret-key", type=str, default="minioadmin",
                       help="Clé secrète MinIO")
    parser.add_argument("--minio-bucket", type=str, default="processed-data",
                       help="Nom du bucket MinIO contenant les données")
    parser.add_argument("--minio-path", type=str, default="processed/yellow_taxi/fact_taxi_trips",
                       help="Chemin des données dans le bucket")
    
    # PostgreSQL parameters
    parser.add_argument("--pg-host", type=str, default="localhost",
                       help="Hôte PostgreSQL")
    parser.add_argument("--pg-port", type=str, default="5432",
                       help="Port PostgreSQL")
    parser.add_argument("--pg-db", type=str, default="Minio",
                       help="Nom de la base de données PostgreSQL")
    parser.add_argument("--pg-user", type=str, default="postgres",
                       help="Utilisateur PostgreSQL")
    parser.add_argument("--pg-password", type=str, default="Affili@12707",
                       help="Mot de passe PostgreSQL")
    parser.add_argument("--pg-table", type=str, default="fact_taxi_trips",
                       help="Nom de la table PostgreSQL")
    
    args = parser.parse_args()
    
    # Créer un répertoire temporaire pour stocker les fichiers parquet
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Initialiser le client MinIO
            minio_client = get_minio_client(
                args.minio_endpoint,
                args.minio_access_key,
                args.minio_secret_key
            )
            
            # Télécharger les fichiers parquet depuis MinIO
            download_parquet_from_minio(
                minio_client,
                args.minio_bucket,
                args.minio_path,
                temp_dir
            )
            
            # Charger les données dans un DataFrame
            df = load_parquet_to_dataframe(temp_dir)
            
            # Limiter à 100000 lignes pour éviter les problèmes de transaction
            print(f"Limitation à 100000 lignes sur {len(df)} lignes au total")
            df = df.head(100000)
            
            # Construire la chaîne de connexion PostgreSQL en utilisant SQLAlchemy de manière sécurisée
            from urllib.parse import quote_plus
            # Encoder le mot de passe pour gérer les caractères spéciaux comme @
            encoded_password = quote_plus(args.pg_password)
            db_connection_string = f"postgresql://{args.pg_user}:{encoded_password}@{args.pg_host}:{args.pg_port}/{args.pg_db}"
            
            # Sauvegarder les données dans PostgreSQL
            save_to_postgres(df, db_connection_string, args.pg_table)
            
            print("Processus de chargement terminé avec succès")
            
        except Exception as e:
            print(f"Erreur lors du chargement des données: {str(e)}")
            raise e

if __name__ == "__main__":
    main()
