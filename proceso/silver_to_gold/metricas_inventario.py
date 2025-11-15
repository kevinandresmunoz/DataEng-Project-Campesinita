# Databricks notebook source
"""
Silver to Gold: Metricas de Inventario
KPIs de stock, alertas y rotacion
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

print("INICIO: Metricas de Inventario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Inventario Actual

# COMMAND ----------

catalog = get_catalog()
df_inventario = spark.table(f"{catalog}.silver.inventario_clean")
df_productos = spark.table(f"{catalog}.silver.productos_clean")

# Join con productos para enriquecer
df_inv_enriched = df_inventario.join(
    df_productos.select("producto_id", "precio", "costo"),
    "producto_id",
    "left"
)

df_fact_inventario = df_inv_enriched.select(
    col("inventario_id"),
    col("producto_id").alias("producto_key"),
    col("sucursal_id").alias("sucursal_key"),
    current_date().alias("fecha_key"),
    col("cantidad"),
    (col("cantidad") * col("costo")).alias("valor_inventario"),
    (col("cantidad") * col("precio")).alias("valor_venta_potencial"),
    col("fecha_actualizacion"),
    col("fecha_caducidad"),
    col("dias_hasta_caducidad"),
    col("estado_caducidad"),
    col("alerta_stock"),
    when(col("estado_caducidad").isin(["Critico", "Alerta"]), True)
        .when(col("alerta_stock") == "Sin Stock", True)
        .otherwise(False).alias("requiere_accion"),
    current_timestamp().alias("fecha_carga")
)

print(f"Fact Inventario: {df_fact_inventario.count():,}")
write_gold(df_fact_inventario, "fact_inventario", partition_by=["sucursal_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPIs de Inventario por Sucursal

# COMMAND ----------

df_kpis_inventario = df_fact_inventario.groupBy("sucursal_key").agg(
    sum("cantidad").alias("stock_total"),
    sum("valor_inventario").alias("valor_total_inventario"),
    sum("valor_venta_potencial").alias("valor_venta_potencial_total"),
    count("inventario_id").alias("items_unicos"),
    sum(when(col("estado_caducidad") == "Critico", 1).otherwise(0)).alias("items_criticos"),
    sum(when(col("estado_caducidad") == "Alerta", 1).otherwise(0)).alias("items_alerta"),
    sum(when(col("alerta_stock") == "Sin Stock", 1).otherwise(0)).alias("items_sin_stock"),
    sum(when(col("alerta_stock") == "Stock Bajo", 1).otherwise(0)).alias("items_stock_bajo"),
    avg("dias_hasta_caducidad").alias("dias_promedio_caducidad")
)

print(f"KPIs Inventario: {df_kpis_inventario.count():,}")
write_gold(df_kpis_inventario, "fact_kpis_inventario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alertas de Inventario

# COMMAND ----------

df_alertas = df_fact_inventario.filter(col("requiere_accion") == True).select(
    col("inventario_id"),
    col("producto_key"),
    col("sucursal_key"),
    col("cantidad"),
    col("estado_caducidad"),
    col("dias_hasta_caducidad"),
    col("alerta_stock"),
    col("valor_inventario"),
    when(col("estado_caducidad") == "Critico", "Alta")
        .when(col("alerta_stock") == "Sin Stock", "Alta")
        .when(col("estado_caducidad") == "Alerta", "Media")
        .otherwise("Baja").alias("prioridad"),
    when(col("dias_hasta_caducidad") < 7, "Liquidar urgente")
        .when(col("dias_hasta_caducidad") < 15, "Promocionar")
        .when(col("alerta_stock") == "Sin Stock", "Reabastecer urgente")
        .when(col("alerta_stock") == "Stock Bajo", "Reabastecer")
        .otherwise("Monitorear").alias("accion_recomendada"),
    current_timestamp().alias("fecha_alerta")
)

print(f"Alertas Inventario: {df_alertas.count():,}")
write_gold(df_alertas, "fact_alertas_inventario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimizar Tablas

# COMMAND ----------

optimize_table("gold.fact_inventario", zorder_cols=["producto_key", "estado_caducidad"])
optimize_table("gold.fact_kpis_inventario", zorder_cols=["stock_total"])
optimize_table("gold.fact_alertas_inventario", zorder_cols=["prioridad", "producto_key"])

# COMMAND ----------

print("COMPLETADO: Metricas de Inventario")
dbutils.notebook.exit("SUCCESS")
