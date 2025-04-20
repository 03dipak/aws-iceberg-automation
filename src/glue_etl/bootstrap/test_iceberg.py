import sys
import yaml

import boto3
from botocore.exceptions import ClientError
import textwrap
import os
import requests
import zipfile
from py4j.protocol import Py4JJavaError


jar_dir = "/opt/aws-glue-libs/jarsv1"
jars = ",".join([os.path.join(jar_dir, f) for f in os.listdir(jar_dir) if f.endswith(".jar")])

print(f"🔍 Using JARs:\n{jars}\n")

# Check if JARs are included in the classpath
print("ClassPath:", os.environ.get("CLASSPATH"))
print("PYSPARK_SUBMIT_ARGS:", os.environ.get("PYSPARK_SUBMIT_ARGS"))

# JAR file path
jar_path = "/opt/aws-glue-libs/jarsv1/iceberg-spark-runtime-3.4_2.12-1.4.0.jar"

# Step 1: Validate the class exists in the JAR
print("🔍 Checking if SparkCatalog class exists in the JAR...")
with zipfile.ZipFile(jar_path, 'r') as jar:
    found = any("org/apache/iceberg/spark/SparkCatalog.class" in name for name in jar.namelist())
    print("✅ Class found in JAR!" if found else "❌ Class NOT found in JAR!")

from pyspark.sql import SparkSession
warehouse_path = "s3://glue-bucket-dev-prod-bucket-march2025/warehouse/"
spark = SparkSession.builder.appName("IcebergTableCreator") \
        .config("spark.jars", jars) \
        .config("spark.driver.extraClassPath", jars) \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.warehouse", warehouse_path) \
        .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
        .config("spark.sql.catalog.glue_catalog.lock.table", "iceberg_lock_table") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.defaultCatalog", "glue_catalog") \
        .enableHiveSupport() \
        .getOrCreate()

# Check if JARs are included in the classpath
print("ClassPath:", os.environ.get("CLASSPATH"))
print("PYSPARK_SUBMIT_ARGS:", os.environ.get("PYSPARK_SUBMIT_ARGS"))
print("hello")
print("--------------------------")
print(spark.sparkContext._conf.get("spark.jars"))
print("--------------------------")
print(spark.sparkContext._conf.get("spark.driver.extraClassPath"))
print("--------------------------") 
print("hello")

# Step 3: Java class resolution check
try:
    print("🔍 Trying to load Java class: org.apache.iceberg.spark.SparkCatalog")
    catalog_class = spark._jvm.java.lang.Class.forName("org.apache.iceberg.spark.SparkCatalog")
    print("✅ SparkCatalog class found!")
except Py4JJavaError as e:
    print("❌ SparkCatalog class NOT found.")
    print(e.java_exception.getMessage())

try:
    catalog_class = spark._jvm.java.lang.Class.forName("org.apache.iceberg.spark.SparkCatalog")
    print("✅ SparkCatalog class found!")
except Py4JJavaError as e:
    print("❌ SparkCatalog class NOT found.")
    print(e.java_exception.getMessage())

pathcheck = os.environ.get("CLASSPATH")
print(f"❌ get class file path: {pathcheck}")

print("🔥 Spark Version:", spark.version)


jar_path = "/opt/aws-glue-libs/jarsv1/iceberg-spark-runtime-3.4_2.12-1.4.0.jar"
with zipfile.ZipFile(jar_path, 'r') as jar:
    with jar.open('META-INF/MANIFEST.MF') as manifest:
        for line in manifest:
            print(line.decode().strip())

spark.sql("SHOW DATABASES").show()

