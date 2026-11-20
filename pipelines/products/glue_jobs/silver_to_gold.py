import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

SILVER_TABLE = "lakehouse.silver.products"
GOLD_TABLE = "lakehouse.gold.product_category_summary"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
sc.setLogLevel("WARN")

glue_context = GlueContext(sc)
spark = glue_context.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Catalogo Iceberg: misma configuracion utilizada en Silver.
spark.conf.set(
    "spark.sql.catalog.lakehouse",
    "org.apache.iceberg.spark.SparkCatalog",
)
spark.conf.set("spark.sql.catalog.lakehouse.type", "hadoop")
spark.conf.set(
    "spark.sql.catalog.lakehouse.warehouse",
    "s3a://ucv-data-platform/products",
)

# Conexion desde Glue hacia Floci.
hconf = sc._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", "http://floci:4566")
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.connection.ssl.enabled", "false")
hconf.set("fs.s3a.access.key", "test")
hconf.set("fs.s3a.secret.key", "test")
hconf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
)

silver = spark.table(SILVER_TABLE)
silver_count = silver.count()

if silver_count == 0:
    raise ValueError("Silver esta vacia.")

# Una fila por categoria.
gold = (
    silver
    .groupBy("category")
    .agg(
        F.count("*").alias("product_count"),
        F.round(F.avg("price"), 2).alias("average_price"),
        F.round(F.avg("rating"), 2).alias("average_rating"),
        F.sum("stock").alias("total_stock"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
    )
    .withColumn("generated_at", F.current_timestamp())
)

# Validar el contrato antes de escribir.
invalid = gold.filter(
    F.col("category").isNull()
    | (F.trim(F.col("category")) == "")
    | (F.col("product_count") <= 0)
    | F.col("average_price").isNull()
    | (F.col("average_price") <= 0)
).count()

if invalid > 0:
    raise ValueError(f"Gold contiene {invalid} categorias invalidas.")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

gold.writeTo(GOLD_TABLE).using("iceberg").createOrReplace()

# Releer la tabla guardada.
result = spark.table(GOLD_TABLE)

print("\n=== ESQUEMA GOLD ===")
result.printSchema()

print("\n=== INDICADORES POR CATEGORIA ===")
result.orderBy(
    F.desc("product_count"), F.asc("category")
).show(100, truncate=False)

gold_count = result.count()

duplicates = (
    result.groupBy("category")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

represented_products = result.agg(
    F.sum("product_count").alias("total")
).first()["total"]

print(f"Gold rows: {gold_count}")
print(f"Duplicate categories: {duplicates}")
print(f"Products represented in Gold: {represented_products}")
print(f"Silver rows: {silver_count}")

if gold_count == 0 or duplicates > 0:
    raise ValueError("Gold esta vacia o contiene categorias duplicadas.")

if represented_products != silver_count:
    raise ValueError("El total de productos de Gold no coincide con Silver.")

job.commit()
print("GOLD VALIDATION PASSED")