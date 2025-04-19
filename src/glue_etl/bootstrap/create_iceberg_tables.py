import os
import yaml
from pyspark.sql import SparkSession

def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)

def generate_sql(conf):
    cols = ",\n".join([f"{c['name']} {c['type']}" for c in conf['columns']])
    parts = ", ".join(conf.get("partitioned_by", []))
    return f"""
    CREATE TABLE IF NOT EXISTS {conf['database']}.{conf['table']} (
        {cols}
    )
    USING iceberg
    LOCATION '{conf['location']}'
    PARTITIONED BY ({parts})
    TBLPROPERTIES ('format-version'='{conf['format_version']}')
    """

def main():
    spark = SparkSession.builder.appName("IcebergTableCreator").getOrCreate()
    conf = load_config("iceberg_tables/customers.yml")
    sql = generate_sql(conf)
    spark.sql(sql)
    print(f"Table {conf['table']} created.")

if __name__ == "__main__":
    main()
