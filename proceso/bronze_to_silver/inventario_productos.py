# Databricks notebook source
"""
Bronze to Silver: Inventario y Productos
Consolida productos de PostgreSQL y MySQL
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

log("=== INICIO: Consolidacion Productos e Inventario (Bronze → Silver) ===")

# COMMAND ----------

df_productos_pg = read_bronze_table("productos")
df_productos_erp = read_bronze_table("productos_erp")
df_proveedores = read_bronze_table("proveedores")
df_inventario = read_bronze_table("inventario")

log(f"Productos PostgreSQL: {df_productos_pg.count():,}")
log(f"Productos ERP: {df_productos_erp.count():,}")
log(f"Proveedores: {df_proveedores.count():,}")
log(f"Inventario: {df_inventario.count():,}")

# COMMAND ----------

df_productos_clean = df_productos_pg \
    .filter(col("precio") > 0) \
    .filter(col("costo") > 0) \
    .dropDuplicates(["producto_id"]) \
    .withColumn("margen", (col("precio") - col("costo")) / col("precio") * 100) \
    .withColumn("categoria_grupo",
        when(col("categoria").isin(["Lacteos", "Carnes", "Frutas"]), "Perecederos")
        .when(col("categoria").isin(["Aseo", "Limpieza"]), "Hogar")
        .otherwise("Abarrotes"))

log(f"Productos limpios: {df_productos_clean.count():,}")

# COMMAND ----------

df_proveedores_clean = df_proveedores \
    .filter(col("nombre").isNotNull()) \
    .dropDuplicates(["proveedor_id"])

log(f"Proveedores limpios: {df_proveedores_clean.count():,}")

# COMMAND ----------

df_inventario_clean = df_inventario \
    .filter(col("cantidad") >= 0) \
    .dropDuplicates(["inventario_id"]) \
    .withColumn("dias_hasta_caducidad", datediff(col("fecha_caducidad"), current_date())) \
    .withColumn("estado_caducidad",
        when(col("dias_hasta_caducidad") < 7, "Critico")
        .when(col("dias_hasta_caducidad") < 30, "Alerta")
        .when(col("dias_hasta_caducidad") < 90, "Normal")
        .otherwise("Optimo")) \
    .withColumn("alerta_stock",
        when(col("cantidad") == 0, "Sin Stock")
        .when(col("cantidad") < 10, "Stock Bajo")
        .otherwise("Stock OK"))

log(f"Inventario limpio: {df_inventario_clean.count():,}")

# COMMAND ----------

write_silver(df_productos_clean, "productos_clean")
write_silver(df_proveedores_clean, "proveedores_clean")
write_silver(df_inventario_clean, "inventario_clean", partition_by=["sucursal_id"])

# COMMAND ----------

optimize_table("silver.productos_clean", zorder_cols=["categoria", "proveedor_id"])
optimize_table("silver.proveedores_clean", zorder_cols=["ciudad"])
optimize_table("silver.inventario_clean", zorder_cols=["producto_id", "estado_caducidad"])

# COMMAND ----------

log("=== COMPLETADO: Productos e Inventario (Bronze → Silver) ===")
dbutils.notebook.exit("SUCCESS")
