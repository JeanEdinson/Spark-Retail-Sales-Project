from pyspark.sql.functions import (
    col,
    when,
    upper,
    trim
)

# Leer datos bronze
bronze_df = spark.read.parquet(
    "/opt/spark-data/bronze/retail_sales_bronze"
)

# Eliminar duplicados
silver_df = bronze_df.dropDuplicates(["transaction_id"])

# Corregir ship_date inválidas
silver_df = silver_df.withColumn(
    "ship_date",
    when(
        col("ship_date") < col("order_date"),
        None
    ).otherwise(col("ship_date"))
)

# Filtrar cantidades inválidas
silver_df = silver_df.filter(
    col("quantity") > 0
)

# Eliminar precios iguales a cero
silver_df = silver_df.filter(
    col("unit_price") != 0
)

# Convertir precios negativos a positivos
silver_df = silver_df.withColumn(
    "unit_price",
    when(
        col("unit_price") <= 0,
        col("unit_price") * -1
    ).otherwise(col("unit_price"))
)

# Corregir descuentos inválidos
silver_df = silver_df.withColumn(
    "discount_pct",
    when(
        (col("discount_pct") < 0) |
        (col("discount_pct") > 100),
        None
    ).otherwise(col("discount_pct"))
)

# Filtrar edades inválidas
silver_df = silver_df.filter(
    (col("customer_age") > 10) &
    (col("customer_age") < 115)
)

# Normalizar género
silver_df = silver_df.withColumn(
    "gender",
    when(upper(col("gender")) == "MALE", "M")
    .when(upper(col("gender")) == "FEMALE", "F")
    .when(col("gender").isin("M", "F"), col("gender"))
    .otherwise(None)
)

# Guardar capa silver
silver_df.write.mode("overwrite").parquet(
    "/opt/spark-data/silver/retail_silver_clean"
)