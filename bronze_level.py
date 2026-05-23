
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType
)

# Definir esquema
bronze_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("ship_date", DateType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("ingestion_date", DateType(), True)
])

# Leer CSV raw
bronze_df = (
    spark.read
    .option("header", "true")
    .schema(bronze_schema)
    .csv("/opt/spark-data/raw/retail_sales_raw.csv")
)

# Validaciones opcionales
bronze_df.count()
bronze_df.printSchema()

# Guardar capa bronze
bronze_df.write.mode("overwrite").parquet(
    "/opt/spark-data/bronze/retail_sales_bronze"
)