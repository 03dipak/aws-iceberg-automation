import sys
import yaml
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError
import textwrap

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
    spark = SparkSession.builder.appName("IcebergTableCreator") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.warehouse", warehouse_path) \
        .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
        .config("spark.sql.catalog.glue_catalog.lock.table", "iceberg_lock_table") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", "glue_catalog") \
        .config("spark.jars", "/opt/aws-glue-libs/jarsv1/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12-1.3.0.jar") \
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
