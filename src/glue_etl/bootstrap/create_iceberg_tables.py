import sys
import yaml
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
import textwrap
import os
import requests

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def ensure_database_exists(database_name):
    client = boto3.client('glue')
    try:
        client.get_database(Name=database_name)
        print(f"✅ Database '{database_name}' already exists.")
    except client.exceptions.EntityNotFoundException:
        print(f"📁 Database '{database_name}' not found. Creating...")
        client.create_database(DatabaseInput={"Name": database_name})
        print(f"✅ Database '{database_name}' created successfully.")

def generate_sql(conf: dict) -> str:
    columns = ",\n  ".join([f"{col['name']} {col['type']}" for col in conf["columns"]])
    partition_clause = f"PARTITIONED BY ({', '.join(conf['partitioned_by'])})" if conf.get("partitioned_by") else ""
    location_clause = f"LOCATION '{conf['location']}'" if conf.get("location") else ""

    sql_query = f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{conf['database']}.{conf['table']} (
      {columns}
    )
    USING iceberg
    {location_clause}
    {partition_clause}
    TBLPROPERTIES ('format-version'='{conf.get("format_version", "2")}')
    """
    sql_query = f"""
        CREATE TABLE IF NOT EXISTS glue_catalog.bronze.customers (
            FirstName STRING,
            LastName STRING,
            CompanyName STRING,
            EmailAddress STRING,
            Phone STRING,
            CustomerID STRING,
            AddressLine1 STRING,
            City STRING,
            CountryRegion STRING,
            PostalCode STRING
        )
        USING iceberg
        LOCATION 's3://glue-bucket-dev-prod-bucket-march2025/warehouse/bronze/customers'
        PARTITIONED BY (CountryRegion, PostalCode)
        TBLPROPERTIES ('format-version'='2')
    """

    return textwrap.dedent(sql_query).strip()

def main():
    if len(sys.argv) < 3:
        print("Usage: python create_iceberg_table.py <config_yaml> <warehouse_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    warehouse_path = sys.argv[2]

    conf = load_config(config_path)
    # 1. Define required JARs and download them
    JAR_DIR = "/opt/aws-glue-libs/jarsv1"
    os.makedirs(JAR_DIR, exist_ok=True)

    JARS = [
        # Iceberg JARs
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.3.0/iceberg-spark-runtime-3.3_2.12-1.3.0.jar",
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.3.0/iceberg-aws-1.3.0.jar",
        # AWS SDK JARs
        "https://repo1.maven.org/maven2/software/amazon/awssdk/glue/2.20.143/glue-2.20.143.jar",
        "https://repo1.maven.org/maven2/software/amazon/awssdk/sts/2.20.143/sts-2.20.143.jar",
        "https://repo1.maven.org/maven2/software/amazon/awssdk/auth/2.20.143/auth-2.20.143.jar",
        "https://repo1.maven.org/maven2/software/amazon/awssdk/core/2.20.143/core-2.20.143.jar",
        "https://repo1.maven.org/maven2/software/amazon/awssdk/regions/2.20.143/regions-2.20.143.jar",
    ]

    for url in JARS:
        filename = os.path.join(JAR_DIR, url.split("/")[-1])
        if not os.path.exists(filename):
            print(f"📥 Downloading: {url}")
            r = requests.get(url)
            with open(filename, "wb") as f:
                f.write(r.content)  

    spark = SparkSession.builder.appName("IcebergTableCreator") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.warehouse", warehouse_path) \
        .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
        .config("spark.sql.catalog.glue_catalog.lock.table", "iceberg_lock_table") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", "glue_catalog") \
        .config("spark.jars", ",".join([os.path.join(JAR_DIR, jar.split("/")[-1]) for jar in JARS])) \
        .enableHiveSupport() \
        .getOrCreate()
        

    create_sql = generate_sql(conf)
    ensure_database_exists(conf['database'])
    try:
        print("🔧 Running SQL:\n", create_sql)
        spark.sql(create_sql)
        print(f"✅ Iceberg table glue_catalog.{conf['database']}.{conf['table']} created.")
    except Exception as e:
        print("❌ Error executing SQL:")
        import traceback
        traceback.print_exc()    
    
if __name__ == "__main__":
    main()
