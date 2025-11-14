# Databricks notebook source
"""
Bronze to Silver: Clientes Segmentados
Lee desde Bronze (PostgreSQL) y aplica segmentación RFM
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

log("=== INICIO: Segmentación de Clientes ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer Clientes desde Bronze

# COMMAND ----------

df_clientes = read_bronze("postgres", "clientes")
log(f"Clientes Bronze: {df_clientes.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpieza y Normalización

# COMMAND ----------

df_clientes_clean = df_clientes \
    .filter(col("activo") == True) \
    .filter(col("email").isNotNull()) \
    .dropDuplicates(["codigo_cliente"]) \
    .withColumn("nombre_completo", concat_ws(" ", col("nombre"), col("apellidos"))) \
    .withColumn("email_lower", lower(trim(col("email")))) \
    .withColumn("telefono_clean", regexp_replace(col("telefono"), "[^0-9]", "")) \
    .withColumn("antiguedad_dias", datediff(current_date(), col("fecha_registro")))

log(f"Clientes después de limpieza: {df_clientes_clean.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Segmentación de Clientes

# COMMAND ----------

df_clientes_segmented = df_clientes_clean \
    .withColumn("categoria_antiguedad",
        when(col("antiguedad_dias") < 90, "Nuevo")
        .when(col("antiguedad_dias") < 365, "Reciente")
        .when(col("antiguedad_dias") < 730, "Establecido")
        .otherwise("Veterano")
    ) \
    .withColumn("nivel_credito",
        when(col("credito_limite") < 200000, "Bajo")
        .when(col("credito_limite") < 1000000, "Medio")
        .otherwise("Alto")
    ) \
    .withColumn("segmento_negocio",
        when(col("tipo_cliente") == "VIP", "Premium")
        .when(col("tipo_cliente") == "Mayorista", "Corporativo")
        .when(col("antiguedad_dias") > 365, "Fiel")
        .otherwise("Regular")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Estadísticas de Segmentación

# COMMAND ----------

log("Distribución por segmento:")
df_clientes_segmented.groupBy("segmento_negocio").count().orderBy(desc("count")).show()

log("Distribución por antigüedad:")
df_clientes_segmented.groupBy("categoria_antiguedad").count().orderBy(desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Escribir a Silver

# COMMAND ----------

write_silver(df_clientes_segmented, "clientes_clean", partition_by=["ciudad"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimizar Tabla

# COMMAND ----------

optimize_table("silver.clientes_clean", zorder_cols=["codigo_cliente", "tipo_cliente"])

# COMMAND ----------

log("=== FIN: Segmentación de Clientes Completada ===")
