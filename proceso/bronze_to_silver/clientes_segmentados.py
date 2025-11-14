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

log("=== INICIO: Segmentacion Clientes (Bronze → Silver) ===")

# COMMAND ----------

df_clientes = read_bronze_table("clientes")
log(f"Clientes Bronze: {df_clientes.count():,}")

# COMMAND ----------

df_clientes_clean = df_clientes \
    .filter(col("nombre").isNotNull()) \
    .filter(col("email").isNotNull()) \
    .dropDuplicates(["cliente_id"]) \
    .withColumn("antiguedad_dias", datediff(current_date(), col("fecha_registro"))) \
    .withColumn("segmento_antiguedad",
        when(col("antiguedad_dias") < 90, "Nuevo")
        .when(col("antiguedad_dias") < 365, "Regular")
        .otherwise("Antiguo")) \
    .withColumn("nivel_credito_num",
        when(col("nivel_credito") == "Alto", 3)
        .when(col("nivel_credito") == "Medio", 2)
        .otherwise(1))

log(f"Clientes limpios: {df_clientes_clean.count():,}")

# COMMAND ----------

write_silver(df_clientes_clean, "clientes_clean")

# COMMAND ----------

optimize_table("silver.clientes_clean", zorder_cols=["ciudad", "nivel_credito"])

# COMMAND ----------

log("=== COMPLETADO: Clientes (Bronze → Silver) ===")
dbutils.notebook.exit("SUCCESS")
