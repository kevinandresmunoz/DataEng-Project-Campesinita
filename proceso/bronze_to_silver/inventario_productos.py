# Databricks notebook source
"""
Bronze to Silver: Inventario y Productos
Consolida productos de PostgreSQL + MySQL
Procesa inventario actual
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

log("=== INICIO: Consolidación Inventario y Productos ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Consolidar Productos (PostgreSQL + MySQL)

# COMMAND ----------

# Productos operativos (PostgreSQL)
df_prod_postgres = read_bronze("postgres", "productos_activos").select(
    col("id").alias("producto_id"),
    col("codigo_producto"),
    col("nombre"),
    col("categoria_id"),
    col("precio_compra"),
    col("precio_venta"),
    col("perecedero"),
    lit("postgres").alias("source")
)

# Productos completos (MySQL ERP)
df_prod_mysql = read_bronze("mysql_erp", "productos_completo").select(
    col("id").alias("producto_id"),
    col("codigo_producto"),
    col("nombre"),
    col("categoria_id"),
    col("precio_compra"),
    col("precio_venta"),
    col("perecedero"),
    lit("mysql").alias("source")
)

# Unir y deduplicar (preferir MySQL por ser más completo)
df_productos_union = df_prod_postgres.union(df_prod_mysql)

window = Window.partitionBy("codigo_producto").orderBy(
    when(col("source") == "mysql", 1).otherwise(2)
)

df_productos_dedup = df_productos_union \
    .withColumn("row_num", row_number().over(window)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

log(f"Productos después de deduplicación: {df_productos_dedup.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Calcular Métricas de Productos

# COMMAND ----------

df_productos_enriched = df_productos_dedup \
    .filter(col("precio_venta") > 0) \
    .filter(col("precio_compra") > 0) \
    .withColumn("margen_absoluto", col("precio_venta") - col("precio_compra")) \
    .withColumn("margen_porcentaje", 
        ((col("precio_venta") - col("precio_compra")) / col("precio_compra") * 100)
    ) \
    .withColumn("categoria_margen",
        when(col("margen_porcentaje") < 15, "Bajo")
        .when(col("margen_porcentaje") < 30, "Medio")
        .otherwise("Alto")
    ) \
    .withColumn("rango_precio",
        when(col("precio_venta") < 5000, "Económico")
        .when(col("precio_venta") < 20000, "Medio")
        .otherwise("Premium")
    )

log(f"Productos enriquecidos: {df_productos_enriched.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Procesar Inventario Actual

# COMMAND ----------

df_inventario = read_bronze("postgres", "inventario_actual")

df_inventario_enriched = df_inventario \
    .filter(col("cantidad_actual") >= 0) \
    .withColumn("valor_inventario", col("cantidad_actual") * col("costo_promedio")) \
    .withColumn("dias_hasta_caducidad", 
        when(col("fecha_caducidad").isNotNull(),
            datediff(col("fecha_caducidad"), current_date())
        ).otherwise(999)
    ) \
    .withColumn("estado_caducidad",
        when(col("dias_hasta_caducidad") < 7, "Crítico")
        .when(col("dias_hasta_caducidad") < 15, "Alerta")
        .when(col("dias_hasta_caducidad") < 30, "Normal")
        .otherwise("Óptimo")
    ) \
    .withColumn("requiere_reabastecimiento",
        when(col("cantidad_actual") < 20, True).otherwise(False)
    )

log(f"Inventario procesado: {df_inventario_enriched.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consolidar Proveedores

# COMMAND ----------

df_proveedores = read_bronze("mysql_erp", "proveedores_completo") \
    .select(
        col("id").alias("proveedor_id"),
        col("codigo_proveedor"),
        col("nombre_empresa"),
        col("tipo_productos"),
        col("calificacion"),
        col("plazo_pago_dias"),
        col("descuento_volumen"),
        col("ciudad"),
        col("estado")
    )

log(f"Proveedores: {df_proveedores.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Escribir a Silver

# COMMAND ----------

write_silver(df_productos_enriched, "productos_clean", partition_by=["categoria_id"])
write_silver(df_inventario_enriched, "inventario_clean", partition_by=["sucursal_id"])
write_silver(df_proveedores, "proveedores_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimizar Tablas

# COMMAND ----------

optimize_table("silver.productos_clean", zorder_cols=["codigo_producto", "categoria_id"])
optimize_table("silver.inventario_clean", zorder_cols=["producto_id", "sucursal_id"])
optimize_table("silver.proveedores_clean", zorder_cols=["proveedor_id"])

# COMMAND ----------

log("=== FIN: Consolidación Inventario y Productos Completada ===")
