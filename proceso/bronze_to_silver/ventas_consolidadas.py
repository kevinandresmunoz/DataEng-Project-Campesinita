# Databricks notebook source
"""
Bronze to Silver: Ventas Consolidadas
Limpia y valida ventas + detalle
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

log("=== INICIO: Limpieza Ventas (Bronze → Silver) ===")

# COMMAND ----------

df_ventas = read_bronze_table("ventas")
df_detalle = read_bronze_table("detalle_ventas")

log(f"Ventas Bronze: {df_ventas.count():,}")
log(f"Detalle Bronze: {df_detalle.count():,}")

# COMMAND ----------

df_ventas_clean = df_ventas \
    .filter(col("total") > 0) \
    .filter(col("estado") == "completada") \
    .filter(col("fecha").isNotNull()) \
    .dropDuplicates(["venta_id"]) \
    .withColumn("fecha_date", to_date(col("fecha"))) \
    .withColumn("hora", hour(col("fecha"))) \
    .withColumn("dia_semana", dayofweek(col("fecha"))) \
    .withColumn("es_fin_semana", when(col("dia_semana").isin([1, 7]), True).otherwise(False)) \
    .withColumn("periodo_dia",
        when(col("hora").between(6, 11), "Mañana")
        .when(col("hora").between(12, 17), "Tarde")
        .otherwise("Noche"))

log(f"Ventas limpias: {df_ventas_clean.count():,}")

# COMMAND ----------

df_detalle_clean = df_detalle \
    .filter(col("cantidad") > 0) \
    .filter(col("precio_unitario") > 0) \
    .filter(col("subtotal") > 0) \
    .dropDuplicates(["detalle_id"]) \
    .withColumn("subtotal_calculado", col("cantidad") * col("precio_unitario")) \
    .withColumn("diferencia_subtotal", abs(col("subtotal") - col("subtotal_calculado"))) \
    .filter(col("diferencia_subtotal") < 0.01)

log(f"Detalle limpio: {df_detalle_clean.count():,}")

# COMMAND ----------

write_silver(df_ventas_clean, "ventas_clean", partition_by=["fecha_date"])
write_silver(df_detalle_clean, "detalle_ventas_clean")

# COMMAND ----------

optimize_table("silver.ventas_clean", zorder_cols=["cliente_id", "sucursal_id"])
optimize_table("silver.detalle_ventas_clean", zorder_cols=["venta_id", "producto_id"])

# COMMAND ----------

log("=== COMPLETADO: Ventas (Bronze → Silver) ===")
dbutils.notebook.exit("SUCCESS")
