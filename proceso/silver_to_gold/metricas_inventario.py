# Databricks notebook source
"""
Silver to Gold: Métricas de Inventario
KPIs de stock, alertas y rotación
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

log("=== INICIO: Métricas de Inventario ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fact Inventario Actual

# COMMAND ----------

df_inventario = spark.table("silver.inventario_clean")
df_productos = spark.table("silver.productos_clean")

# Join con productos
df_inv_enriched = df_inventario.join(
    df_productos.select(
        col("producto_id"),
        col("precio_venta"),
        col("margen_porcentaje")
    ),
    "producto_id",
    "left"
)

df_fact_inventario = df_inv_enriched.select(
    col("id").alias("inventario_id"),
    col("producto_id").alias("producto_key"),
    col("sucursal_id").alias("sucursal_key"),
    to_date(col("fecha_ultimo_conteo")).alias("fecha_key"),
    col("cantidad_actual"),
    col("costo_promedio"),
    col("valor_inventario"),
    (col("cantidad_actual") * col("precio_venta")).alias("valor_venta_potencial"),
    col("ubicacion"),
    col("lote"),
    col("fecha_caducidad"),
    col("dias_hasta_caducidad"),
    col("estado_caducidad"),
    col("requiere_reabastecimiento"),
    when(col("estado_caducidad") == "Crítico", True).otherwise(False).alias("requiere_accion_inmediata"),
    current_timestamp().alias("fecha_carga")
)

log(f"Fact Inventario: {df_fact_inventario.count():,}")
write_gold(df_fact_inventario, "fact_inventario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. KPIs de Inventario por Sucursal

# COMMAND ----------

df_kpis_inventario = df_fact_inventario.groupBy("sucursal_key").agg(
    sum("cantidad_actual").alias("stock_total"),
    sum("valor_inventario").alias("valor_total_inventario"),
    sum("valor_venta_potencial").alias("valor_venta_potencial_total"),
    count("inventario_id").alias("items_unicos"),
    sum(when(col("estado_caducidad") == "Crítico", 1).otherwise(0)).alias("items_criticos"),
    sum(when(col("estado_caducidad") == "Alerta", 1).otherwise(0)).alias("items_alerta"),
    sum(when(col("requiere_reabastecimiento") == True, 1).otherwise(0)).alias("items_reabastecer"),
    avg("dias_hasta_caducidad").alias("dias_promedio_caducidad")
)

log(f"KPIs Inventario: {df_kpis_inventario.count():,}")
write_gold(df_kpis_inventario, "fact_kpis_inventario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Alertas de Inventario

# COMMAND ----------

df_alertas = df_fact_inventario.filter(
    (col("estado_caducidad").isin(["Crítico", "Alerta"])) |
    (col("requiere_reabastecimiento") == True)
).select(
    col("inventario_id"),
    col("producto_key"),
    col("sucursal_key"),
    col("cantidad_actual"),
    col("estado_caducidad"),
    col("dias_hasta_caducidad"),
    col("valor_inventario"),
    col("requiere_reabastecimiento"),
    when(col("estado_caducidad") == "Crítico", "Alta")
        .when(col("estado_caducidad") == "Alerta", "Media")
        .when(col("requiere_reabastecimiento") == True, "Media")
        .otherwise("Baja").alias("prioridad"),
    when(col("dias_hasta_caducidad") < 7, "Liquidar urgente")
        .when(col("dias_hasta_caducidad") < 15, "Promocionar")
        .when(col("requiere_reabastecimiento") == True, "Reabastecer")
        .otherwise("Monitorear").alias("accion_recomendada"),
    current_timestamp().alias("fecha_alerta")
)

log(f"Alertas Inventario: {df_alertas.count():,}")
write_gold(df_alertas, "fact_alertas_inventario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Optimizar Tablas

# COMMAND ----------

optimize_table("gold.fact_inventario", zorder_cols=["sucursal_key", "producto_key"])
optimize_table("gold.fact_kpis_inventario", zorder_cols=["sucursal_key"])
optimize_table("gold.fact_alertas_inventario", zorder_cols=["prioridad", "sucursal_key"])

# COMMAND ----------

log("=== FIN: Métricas de Inventario Creadas ===")
