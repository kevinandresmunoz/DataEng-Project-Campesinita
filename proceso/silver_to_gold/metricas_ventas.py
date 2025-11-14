# Databricks notebook source
"""
Silver to Gold: Metricas de Ventas
KPIs de ventas, rentabilidad y analisis RFM
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

log("=== INICIO: Metricas de Ventas ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Ventas (Transaccional)

# COMMAND ----------

df_ventas = spark.table("silver.ventas_clean")
df_detalle = spark.table("silver.detalle_ventas_clean")
df_productos = spark.table("silver.productos_clean")

# Enriquecer detalle con margenes
df_detalle_enriched = df_detalle.join(
    df_productos.select("producto_id", "margen"),
    "producto_id",
    "left"
).withColumn("margen_item", col("subtotal") * col("margen") / 100)

# Agregar metricas por venta
df_detalle_agg = df_detalle_enriched.groupBy("venta_id").agg(
    sum("cantidad").alias("cantidad_total_items"),
    count("detalle_id").alias("num_items_distintos"),
    sum("margen_item").alias("margen_total")
)

# Join ventas con detalle agregado
df_fact_ventas = df_ventas.join(df_detalle_agg, "venta_id", "left")

df_fact_ventas_final = df_fact_ventas.select(
    col("venta_id"),
    col("fecha_date").alias("fecha_key"),
    col("cliente_id").alias("cliente_key"),
    col("sucursal_id").alias("sucursal_key"),
    col("total"),
    col("metodo_pago"),
    col("hora"),
    col("dia_semana"),
    col("es_fin_semana"),
    col("periodo_dia"),
    col("cantidad_total_items"),
    col("num_items_distintos"),
    col("margen_total"),
    (col("total") / col("num_items_distintos")).alias("ticket_promedio_item"),
    current_timestamp().alias("fecha_carga")
)

log(f"Fact Ventas: {df_fact_ventas_final.count():,}")
write_gold(df_fact_ventas_final, "fact_ventas", partition_by=["fecha_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPIs Diarios por Sucursal

# COMMAND ----------

df_kpis_diarios = df_fact_ventas_final.groupBy("fecha_key", "sucursal_key").agg(
    sum("total").alias("ventas_totales"),
    count("venta_id").alias("numero_transacciones"),
    avg("total").alias("ticket_promedio"),
    sum("margen_total").alias("margen_bruto_total"),
    countDistinct("cliente_key").alias("clientes_unicos"),
    sum("cantidad_total_items").alias("items_vendidos"),
    sum(when(col("es_fin_semana") == True, 1).otherwise(0)).alias("ventas_fin_semana"),
    sum(when(col("periodo_dia") == "Mañana", col("total")).otherwise(0)).alias("ventas_manana"),
    sum(when(col("periodo_dia") == "Tarde", col("total")).otherwise(0)).alias("ventas_tarde"),
    sum(when(col("periodo_dia") == "Noche", col("total")).otherwise(0)).alias("ventas_noche")
)

log(f"KPIs Diarios: {df_kpis_diarios.count():,}")
write_gold(df_kpis_diarios, "fact_kpis_diarios", partition_by=["fecha_key"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analisis RFM de Clientes

# COMMAND ----------

df_rfm = df_fact_ventas_final.groupBy("cliente_key").agg(
    max("fecha_key").alias("ultima_compra"),
    count("venta_id").alias("frecuencia_compras"),
    sum("total").alias("valor_total_compras"),
    avg("total").alias("ticket_promedio_cliente"),
    sum("cantidad_total_items").alias("items_totales"),
    countDistinct("sucursal_key").alias("sucursales_visitadas")
)

df_rfm_enriched = df_rfm \
    .withColumn("recency_dias", datediff(current_date(), col("ultima_compra"))) \
    .withColumn("segmento_rfm",
        when((col("recency_dias") < 30) & (col("frecuencia_compras") > 10) & (col("valor_total_compras") > 1000000), "Champions")
        .when((col("recency_dias") < 60) & (col("frecuencia_compras") > 5), "Leales")
        .when((col("recency_dias") < 90) & (col("frecuencia_compras") > 2), "Potenciales")
        .when(col("recency_dias") > 180, "En Riesgo")
        .otherwise("Regulares"))

log(f"Analisis RFM: {df_rfm_enriched.count():,}")
write_gold(df_rfm_enriched, "fact_analisis_clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rendimiento de Productos

# COMMAND ----------

df_productos_perf = df_detalle_enriched.groupBy("producto_id").agg(
    sum("cantidad").alias("unidades_vendidas"),
    sum("subtotal").alias("ingresos_totales"),
    sum("margen_item").alias("margen_total"),
    count("venta_id").alias("numero_ventas"),
    countDistinct("venta_id").alias("transacciones_unicas")
)

log(f"Rendimiento Productos: {df_productos_perf.count():,}")
write_gold(df_productos_perf, "fact_rendimiento_productos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimizar Tablas

# COMMAND ----------

optimize_table("gold.fact_ventas", zorder_cols=["sucursal_key", "cliente_key"])
optimize_table("gold.fact_kpis_diarios", zorder_cols=["sucursal_key"])
optimize_table("gold.fact_analisis_clientes", zorder_cols=["cliente_key"])
optimize_table("gold.fact_rendimiento_productos", zorder_cols=["producto_id"])

# COMMAND ----------

log("=== COMPLETADO: Metricas de Ventas ===")
dbutils.notebook.exit("SUCCESS")
