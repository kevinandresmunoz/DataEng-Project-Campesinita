# Databricks notebook source
"""
Silver to Gold: Métricas de Ventas
KPIs de ventas, rentabilidad y análisis RFM
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

log("=== INICIO: Métricas de Ventas ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fact Ventas (Transaccional)

# COMMAND ----------

df_ventas = spark.table("silver.ventas_clean")
df_detalle = spark.table("silver.detalle_ventas_clean")

# Agregar métricas de detalle
df_detalle_agg = df_detalle.groupBy("venta_id").agg(
    sum("cantidad").alias("cantidad_total_items"),
    sum("subtotal").alias("subtotal_items"),
    sum("margen_item").alias("margen_total_items"),
    avg("margen_porcentaje").alias("margen_promedio_items"),
)

df_fact_ventas = df_ventas.join(
    df_detalle_agg, df_ventas.id == df_detalle_agg.venta_id, "left"
)

df_fact_ventas_final = df_fact_ventas.select(
    col("id").alias("venta_id"),
    col("folio"),
    col("fecha").alias("fecha_key"),
    col("cliente_id").alias("cliente_key"),
    col("empleado_id").alias("empleado_key"),
    col("sucursal_id").alias("sucursal_key"),
    col("anio"),
    col("mes"),
    col("hora"),
    col("periodo_dia"),
    col("dia_semana"),
    col("es_fin_semana"),
    col("subtotal"),
    col("descuento"),
    col("descuento_porcentaje"),
    col("impuestos"),
    col("total"),
    col("num_productos"),
    col("cantidad_total_items"),
    col("margen_total_items"),
    col("margen_promedio_items"),
    col("metodo_pago"),
    col("ticket_promedio_item"),
    current_timestamp().alias("fecha_carga"),
)

log(f"Fact Ventas: {df_fact_ventas_final.count():,}")
write_gold(df_fact_ventas_final, "fact_ventas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. KPIs Diarios por Sucursal

# COMMAND ----------

df_kpis_diarios = df_fact_ventas_final.groupBy("fecha_key", "sucursal_key").agg(
    sum("total").alias("ventas_totales"),
    count("venta_id").alias("numero_transacciones"),
    avg("total").alias("ticket_promedio"),
    sum("margen_total_items").alias("margen_bruto_total"),
    countDistinct("cliente_key").alias("clientes_unicos"),
    sum("cantidad_total_items").alias("items_vendidos"),
    avg("margen_promedio_items").alias("margen_promedio"),
    sum(when(col("es_fin_semana") == True, 1).otherwise(0)).alias("ventas_fin_semana"),
    sum(when(col("periodo_dia") == "Mañana", col("total")).otherwise(0)).alias(
        "ventas_manana"
    ),
    sum(when(col("periodo_dia") == "Tarde", col("total")).otherwise(0)).alias(
        "ventas_tarde"
    ),
    sum(when(col("periodo_dia") == "Noche", col("total")).otherwise(0)).alias(
        "ventas_noche"
    ),
)

log(f"KPIs Diarios: {df_kpis_diarios.count():,}")
write_gold(df_kpis_diarios, "fact_kpis_diarios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Análisis RFM de Clientes

# COMMAND ----------

df_rfm = df_fact_ventas_final.groupBy("cliente_key").agg(
    max("fecha_key").alias("ultima_compra"),
    count("venta_id").alias("frecuencia_compras"),
    sum("total").alias("valor_total_compras"),
    avg("total").alias("ticket_promedio_cliente"),
    sum("cantidad_total_items").alias("items_totales"),
    countDistinct("sucursal_key").alias("sucursales_visitadas"),
)

df_rfm_enriched = df_rfm.withColumn(
    "recency_dias", datediff(current_date(), col("ultima_compra"))
).withColumn(
    "segmento_rfm",
    when(
        (col("recency_dias") < 30)
        & (col("frecuencia_compras") > 10)
        & (col("valor_total_compras") > 1000000),
        "Champions",
    )
    .when((col("recency_dias") < 60) & (col("frecuencia_compras") > 5), "Leales")
    .when((col("recency_dias") < 90) & (col("frecuencia_compras") > 2), "Potenciales")
    .when(col("recency_dias") > 180, "En Riesgo")
    .otherwise("Regulares"),
)

log(f"Análisis RFM: {df_rfm_enriched.count():,}")
write_gold(df_rfm_enriched, "fact_analisis_clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Rendimiento de Productos

# COMMAND ----------

df_productos_perf = df_detalle.groupBy("producto_id").agg(
    sum("cantidad").alias("unidades_vendidas"),
    sum("subtotal").alias("ingresos_totales"),
    sum("margen_item").alias("margen_total"),
    avg("margen_porcentaje").alias("margen_promedio"),
    count("venta_id").alias("numero_ventas"),
    countDistinct("venta_id").alias("transacciones_unicas"),
)

log(f"Rendimiento Productos: {df_productos_perf.count():,}")
write_gold(df_productos_perf, "fact_rendimiento_productos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Optimizar Tablas

# COMMAND ----------

optimize_table(
    "gold.fact_ventas", zorder_cols=["fecha_key", "sucursal_key", "cliente_key"]
)
optimize_table("gold.fact_kpis_diarios", zorder_cols=["fecha_key", "sucursal_key"])
optimize_table("gold.fact_analisis_clientes", zorder_cols=["cliente_key"])
optimize_table("gold.fact_rendimiento_productos", zorder_cols=["producto_id"])

# COMMAND ----------

log("=== FIN: Métricas de Ventas Creadas ===")
