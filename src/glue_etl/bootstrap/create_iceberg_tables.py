import sys
import yaml
from pyspark.sql import SparkSession

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def create_spark_session(warehouse_path: str, app_name="IcebergTableCreator") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name) \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.warehouse", warehouse_path) \
        .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
        .config("spark.sql.catalog.glue_catalog.lock.table", "iceberg_lock_table") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", "glue_catalog") \
        .config("spark.jars", "/opt/aws-glue-libs/jarsv1/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12-1.3.0.jar") \
        .enableHiveSupport() \
        .getOrCreate()
    )

def generate_sql(conf: dict) -> str:
    columns = ",\n  ".join([f"{col['name']} {col['type']}" for col in conf["columns"]])
    partition_clause = f"PARTITIONED BY ({', '.join(conf['partitioned_by'])})" if conf.get("partitioned_by") else ""
    location_clause = f"LOCATION '{conf['location']}'" if conf.get("location") else ""

    return f"""
    CREATE TABLE IF NOT EXISTS glue_catalog.{conf['database']}.{conf['table']} (
      {columns}
    )
    USING iceberg
    {location_clause}
    {partition_clause}
    TBLPROPERTIES ('format-version'='{conf.get("format_version", "2")}')
    """

def main():
    if len(sys.argv) < 3:
        print("Usage: python create_iceberg_table.py <config_yaml> <warehouse_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    warehouse_path = sys.argv[2]

    conf = load_config(config_path)
    spark = create_spark_session(warehouse_path)

    create_sql = generate_sql(conf)
    print("🔧 Running SQL:\n", create_sql)

    spark.sql(create_sql)
    print(f"✅ Iceberg table glue_catalog.{conf['database']}.{conf['table']} created.")
if __name__ == "__main__":
    main()
