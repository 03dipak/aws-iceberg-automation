import sys
import yaml
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
import textwrap
import os
import requests

# 1. Define required JARs and download them
JAR_DIR = "/opt/aws-glue-libs/jarsv1"
os.makedirs(JAR_DIR, exist_ok=True)

JARS = [
    # Iceberg JARs
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.0/iceberg-spark-runtime-3.4_2.12-1.4.0.jar",
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.0/iceberg-aws-1.4.0.jar",
    # AWS SDK JARs
    "https://repo1.maven.org/maven2/software/amazon/awssdk/glue/2.20.143/glue-2.20.143.jar",
    "https://repo1.maven.org/maven2/software/amazon/awssdk/sts/2.20.143/sts-2.20.143.jar",
    "https://repo1.maven.org/maven2/software/amazon/awssdk/auth/2.20.143/auth-2.20.143.jar",
    "https://repo1.maven.org/maven2/software/amazon/awssdk/aws-core/2.20.143/aws-core-2.20.143.jar",
    "https://repo1.maven.org/maven2/software/amazon/awssdk/sdk-core/2.20.143/sdk-core-2.20.143.jar",
    "https://repo1.maven.org/maven2/software/amazon/awssdk/regions/2.20.143/regions-2.20.143.jar",
]

for url in JARS:
    filename = os.path.join(JAR_DIR, url.split("/")[-1])
    if not os.path.exists(filename):
        print(f"📥 Downloading: {url}")
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()  # Raise error for bad HTTP responses

            with open(filename, "wb") as f:
                f.write(r.content)

            print(f"✅ File downloaded and saved as '{filename}'")
        except requests.exceptions.HTTPError as http_err:
            print(f"❌ HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            print(f"❌ Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            print(f"❌ Timeout error occurred: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            print(f"❌ Request failed: {req_err}")
        except IOError as io_err:
            print(f"❌ File write error: {io_err}")

full_har_dir =  ",".join([os.path.join(JAR_DIR, jar.split("/")[-1]) for jar in JARS])
warehouse_path = "s3://glue-bucket-dev-prod-bucket-march2025/warehouse/"
spark = SparkSession.builder.appName("IcebergTableCreator") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.warehouse", warehouse_path) \
        .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
        .config("spark.sql.catalog.glue_catalog.lock.table", "iceberg_lock_table") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", "glue_catalog") \
        .config("spark.jars", full_har_dir) \
        .enableHiveSupport() \
        .config("spark.driver.extraClassPath", full_har_dir) \
        .config("spark.executor.extraClassPath", full_har_dir) \
        .getOrCreate()

pathcheck = os.environ.get("CLASSPATH")
print(f"❌ get class file path: {pathcheck}")
spark.sql("SHOW DATABASES").show()

