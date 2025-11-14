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

df_clientes_clean = df_clientes \
    .filter(col("nombre").isNotNull()) \
    .filter(col("email").isNotNull()) \
    .withColumn("cliente_id", col("id")) \
    .withColumn("nivel_credito", col("tipo_cliente")) \
    .dropDuplicates(["id"]) \
    .withColumn("antiguedad_dias", datediff(current_date(), col("fecha_registro"))) \
    .withColumn("segmento_antiguedad",
        when(col("antiguedad_dias") < 90, "Nuevo")
        .when(col("antiguedad_dias") < 365, "Regular")
        .otherwise("Antiguo")) \
    .withColumn("nivel_credito_num",
        when(col("tipo_cliente") == "VIP", 3)
        .when(col("tipo_cliente") == "Regular", 2)
        .otherwise(1))

print(f"Clientes limpios: {df_clientes_clean.count():,}")

# COMMAND ----------

write_silver(df_clientes_clean, "clientes_clean")

# COMMAND ----------

optimize_table("silver.clientes_clean", zorder_cols=["ciudad", "nivel_credito"])

# COMMAND ----------

print("COMPLETADO: Clientes (Bronze → Silver)")
dbutils.notebook.exit("SUCCESS")
