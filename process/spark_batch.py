# spark_batch.py
# Script for batch processing of Yellow Taxi NYC data using PySpark

import os
import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, when, lit, round, datediff, 
    unix_timestamp, from_unixtime, expr, concat, to_timestamp
)
from pyspark.sql.types import DoubleType, IntegerType, StringType

# Define payment type mapping
PAYMENT_TYPE_MAPPING = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}

def create_spark_session(app_name="YellowTaxiProcessor"):
    """
    Create and configure a Spark session
    """
    # Configurer le driver JDBC PostgreSQL
    # Télécharger le pilote PostgreSQL si nécessaire
    import os
    from urllib import request
    
    # Chemin où nous allons télécharger le JAR du pilote PostgreSQL
    jar_dir = os.path.join(os.path.expanduser("~"), ".spark-jars")
    postgresql_jar_path = os.path.join(jar_dir, "postgresql-42.3.1.jar")
    
    # Créer le répertoire s'il n'existe pas
    if not os.path.exists(jar_dir):
        os.makedirs(jar_dir)
    
    # Télécharger le JAR PostgreSQL s'il n'existe pas déjà
    if not os.path.exists(postgresql_jar_path):
        print(f"Téléchargement du driver PostgreSQL JDBC vers {postgresql_jar_path}")
        url = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.1/postgresql-42.3.1.jar"
        request.urlretrieve(url, postgresql_jar_path)
    
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", postgresql_jar_path)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .config("spark.hadoop.fs.s3a.endpoint", "http://172.17.16.1:9002")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        .getOrCreate()
    )

def load_data_from_minio(spark, bucket_name, file_path):
    """
    Load parquet data from MinIO
    """
    full_path = f"s3a://{bucket_name}/{file_path}"
    print(f"Loading data from {full_path}")
    return spark.read.parquet(full_path)

def transform_taxi_data(df):
    """
    Transform Yellow Taxi data according to the requirements
    """
    print("Starting data transformation...")
    
    # Afficher un échantillon des timestamps source avant conversion
    print("\n\u00c9chantillon de timestamps source (avant conversion) :")
    df.select("tpep_pickup_datetime").show(10, truncate=False)
    
    # Ensure timestamp fields are properly formatted
    df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    df = df.withColumn("tpep_dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    
    # Afficher un échantillon des timestamps après conversion
    print("\n\u00c9chantillon de timestamps après conversion :")
    df.select("tpep_pickup_datetime").show(10, truncate=False)
    
    # Calculate trip duration in minutes
    df = df.withColumn(
        "trip_duration_minutes",
        round(unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
    )
    
    # Extract time features
    df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    df = df.withColumn("pickup_dow", dayofweek(col("tpep_pickup_datetime")))
    
    # Afficher les timestamps avec les heures extraites
    print("\n\u00c9chantillon de timestamps avec heures extraites :")
    df.select("tpep_pickup_datetime", "pickup_hour").show(10, truncate=False)
    
    # Afficher la distribution des heures
    print("\nDistribution des heures de prise en charge :")
    df.groupBy("pickup_hour").count().orderBy("pickup_hour").show(24)
    
    # Calculate distance category
    df = df.withColumn(
        "distance_category",
        when(col("trip_distance") < 2, "0-2 km")
        .when((col("trip_distance") >= 2) & (col("trip_distance") < 5), "2-5 km")
        .otherwise(">5 km")
    )
    
    # Calculate tip percentage
    df = df.withColumn(
        "tip_percentage",
        when(col("fare_amount") > 0, round((col("tip_amount") / col("fare_amount")) * 100, 2))
        .otherwise(0)
    )
    
    # Convert payment type to descriptive value
    payment_mapping_expr = ""
    for key, value in PAYMENT_TYPE_MAPPING.items():
        mapping_expr = f"WHEN payment_type = {key} THEN '{value}'"
        payment_mapping_expr += mapping_expr + " "
    
    df = df.withColumn(
        "payment_type_desc",
        expr(f"CASE {payment_mapping_expr} ELSE 'Unknown' END")
    )
    
    # Select and rename columns for the final dataset
    transformed_df = df.select(
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("trip_duration_minutes"),
        col("pickup_hour"),
        col("pickup_dow").alias("pickup_day_of_week"),
        col("passenger_count"),
        col("trip_distance"),
        col("distance_category"),
        col("PULocationID").alias("pickup_location_id"),
        col("DOLocationID").alias("dropoff_location_id"),
        col("fare_amount"),
        col("tip_amount"),
        col("tip_percentage"),
        col("total_amount"),
        col("payment_type"),
        col("payment_type_desc")
    )
    
    print("Data transformation completed")
    return transformed_df

def save_to_minio(df, bucket_name, file_path):
    """
    Save DataFrame to MinIO in parquet format
    """
    output_path = f"s3a://{bucket_name}/{file_path}"
    print(f"Saving data to MinIO: {output_path}")
    
    # Write to MinIO in parquet format
    df.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"Data successfully saved to {output_path}")

def main(input_bucket, input_path, output_bucket):
    """
    Main function to orchestrate the ETL process
    """
    # Initialize Spark session
    spark = create_spark_session()
    
    try:
        # Load data from MinIO
        raw_df = load_data_from_minio(spark, input_bucket, input_path)
        print(f"Loaded {raw_df.count()} records from MinIO")
        
        # Transform data
        transformed_df = transform_taxi_data(raw_df)
        
        # Save transformed data to MinIO
        output_path = "processed/yellow_taxi/fact_taxi_trips"
        save_to_minio(transformed_df, output_bucket, output_path)
    
    except Exception as e:
        print(f"Error in batch processing: {str(e)}")
        raise e
    
    finally:
        # Stop Spark session
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Yellow Taxi data with PySpark")
    
    # MinIO connection parameters
    parser.add_argument("--input-bucket", type=str, default="yellow-taxi", 
                        help="Input MinIO bucket name")
    parser.add_argument("--input-path", type=str, default="2023/yellow_tripdata_2023-01.parquet", 
                        help="Path to the input parquet file in MinIO")
    parser.add_argument("--output-bucket", type=str, default="processed-data", 
                        help="Output MinIO bucket name for processed data")
    
    args = parser.parse_args()
    
    main(
        args.input_bucket,
        args.input_path,
        args.output_bucket
    )
