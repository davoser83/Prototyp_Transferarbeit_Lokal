import requests
from google.cloud import storage
import json

def download_data_from_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to download data: {response.status_code}")

def upload_to_gcs(bucket_name, data, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    blob.upload_from_string(
        data=json.dumps(data),
        content_type='application/json'
    )
    print(f"File {destination_blob_name} uploaded to {bucket_name}.")

# Beispiel-URL der JSONPlaceholder API
api_url = "https://jsonplaceholder.typicode.com/posts"
data = download_data_from_api(api_url)

# Beispiel-Bucket-Name und Zielpfad
bucket_name = "your-gcs-bucket"
destination_blob_name = "dummy_data/posts.json"

# Daten auf GCS hochladen
upload_to_gcs(bucket_name, data, destination_blob_name)



from pyspark.sql import SparkSession
import requests
from google.cloud import storage
import json

# Spark-Session erstellen
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()

def download_data_from_api(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to download data: {response.status_code}")

def upload_to_gcs(bucket_name, data, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    blob.upload_from_string(
        data=json.dumps(data),
        content_type='application/json'
    )
    print(f"File {destination_blob_name} uploaded to {bucket_name}.")

# Beispiel-URL der JSONPlaceholder API
api_url = "https://jsonplaceholder.typicode.com/posts"
data = download_data_from_api(api_url)

# Konvertieren der Daten in ein DataFrame
rdd = spark.sparkContext.parallelize(data)
df = spark.read.json(rdd)

# Optional: Daten transformieren
df_transformed = df.select("userId", "id", "title", "body")

# Speichern des DataFrames in eine temporäre JSON-Datei
temp_file = "/tmp/posts.json"
df_transformed.coalesce(1).write.mode('overwrite').json(temp_file)

# Daten von der temporären Datei auf GCS hochladen
bucket_name = "your-gcs-bucket"
destination_blob_name = "dummy_data/posts.json"

with open(f"{temp_file}/part-00000", "r") as file:
    data = file.read()
    upload_to_gcs(bucket_name, data, destination_blob_name)

# Spark-Session beenden
spark.stop()
