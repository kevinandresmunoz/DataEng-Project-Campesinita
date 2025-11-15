# Databricks notebook source
"""
Bronze to Silver: Clientes Segmentados
Segmenta clientes con logica RFM
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

print("INICIO: Segmentacion Clientes (Bronze → Silver)")

# COMMAND ----------

df_clientes = read_bronze_table("clientes")
print(f"Clientes Bronze: {df_clientes.count():,}")

# COMMAND ----------

df_clientes_transformed = df_clientes \
    .filter(col("nombre").isNotNull()) \
    .filter(col("email").isNotNull()) \
    .dropDuplicates(["id"]) \
    .withColumn("cliente_id", col("id")) \
    .withColumn("nivel_credito", col("tipo_cliente")) \
    .withColumn("antiguedad_dias", datediff(current_date(), col("fecha_registro"))) \
    .withColumn("segmento_antiguedad",
        when(datediff(current_date(), col("fecha_registro")) < 90, "Nuevo")
        .when(datediff(current_date(), col("fecha_registro")) < 365, "Regular")
        .otherwise("Antiguo")) \
    .withColumn("nivel_credito_num",
        when(col("tipo_cliente") == "VIP", 3)
        .when(col("tipo_cliente") == "Regular", 2)
        .otherwise(1))

# Seleccionar solo las columnas necesarias para evitar duplicados
df_clientes_clean = df_clientes_transformed.select(
    col("cliente_id"),
    col("nombre"),
    col("email"),
    col("telefono"),
    col("ciudad"),
    col("fecha_registro"),
    col("nivel_credito"),
    col("antiguedad_dias"),
    col("segmento_antiguedad"),
    col("nivel_credito_num")
)

print(f"Clientes limpios: {df_clientes_clean.count():,}")

# COMMAND ----------

write_silver(df_clientes_clean, "clientes_clean")

# COMMAND ----------

optimize_table("silver.clientes_clean", zorder_cols=["ciudad", "nivel_credito"])

# COMMAND ----------

print("COMPLETADO: Clientes (Bronze → Silver)")
dbutils.notebook.exit("SUCCESS")
