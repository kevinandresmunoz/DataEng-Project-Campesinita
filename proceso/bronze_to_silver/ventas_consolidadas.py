# Databricks notebook source
"""
Bronze to Silver: Ventas Consolidadas
Lee desde Bronze (sincronizado cada 6h desde PostgreSQL)
Limpia y valida ventas + detalle
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

log("=== INICIO: Limpieza Ventas (Bronze → Silver) ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer Ventas desde Bronze (PostgreSQL)

# COMMAND ----------

df_ventas = read_bronze("postgres", "ventas_recientes")
df_detalle = read_bronze("postgres", "detalle_ventas_recientes")

log(f"Ventas Bronze: {df_ventas.count():,}")
log(f"Detalle Bronze: {df_detalle.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validar y Limpiar Ventas

# COMMAND ----------

df_ventas_clean = df_ventas \
    .filter(col("total") > 0) \
    .filter(col("estatus") == "completada") \
    .filter(col("fecha_hora").isNotNull()) \
    .dropDuplicates(["id"]) \
    .withColumn("fecha", to_date(col("fecha_hora"))) \
    .withColumn("hora", hour(col("fecha_hora"))) \
    .withColumn("dia_semana", dayofweek(col("fecha_hora"))) \
    .withColumn("es_fin_semana", when(col("dia_semana").isin([1, 7]), True).otherwise(False)) \
    .withColumn("periodo_dia",
        when(col("hora").between(6, 11), "Mañana")
        .when(col("hora").between(12, 17), "Tarde")
        .when(col("hora").between(18, 21), "Noche")
        .otherwise("Madrugada")
    ) \
    .withColumn("anio", year(col("fecha_hora"))) \
    .withColumn("mes", month(col("fecha_hora")))

# Validar consistencia matemática
df_ventas_validated = df_ventas_clean.filter(
    (abs(col("total") - (col("subtotal") + col("impuestos") - col("descuento"))) < 0.01)
)

log(f"Ventas después de limpieza: {df_ventas_validated.count():,}")
log(f"Registros eliminados: {df_ventas.count() - df_ventas_validated.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enriquecer con Métricas

# COMMAND ----------

df_ventas_enriched = df_ventas_validated \
    .withColumn("ticket_promedio_item", col("total") / col("num_productos")) \
    .withColumn("descuento_porcentaje", 
        when(col("subtotal") > 0, (col("descuento") / col("subtotal") * 100)).otherwise(0)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Limpiar Detalle de Ventas

# COMMAND ----------

df_detalle_clean = df_detalle \
    .filter(col("cantidad") > 0) \
    .filter(col("precio_unitario") > 0) \
    .dropDuplicates(["id"]) \
    .withColumn("margen_porcentaje", 
        when(col("costo_unitario") > 0,
            ((col("precio_unitario") - col("costo_unitario")) / col("costo_unitario") * 100)
        ).otherwise(0)
    )

log(f"Detalle después de limpieza: {df_detalle_clean.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Escribir a Silver

# COMMAND ----------

write_silver(df_ventas_enriched, "ventas_clean", partition_by=["anio", "mes"])
write_silver(df_detalle_clean, "detalle_ventas_clean", partition_by=["venta_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimizar Tablas

# COMMAND ----------

optimize_table("silver.ventas_clean", zorder_cols=["fecha", "sucursal_id"])
optimize_table("silver.detalle_ventas_clean", zorder_cols=["venta_id", "producto_id"])

# COMMAND ----------

log("=== FIN: Limpieza Ventas Completada ===")
