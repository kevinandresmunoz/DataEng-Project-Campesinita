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

print("INICIO: Consolidacion Productos e Inventario (Bronze → Silver)")

# COMMAND ----------

df_productos_pg = read_bronze_table("productos")
df_productos_erp = read_bronze_table("productos_erp")
df_proveedores = read_bronze_table("proveedores")
df_inventario = read_bronze_table("inventario")

print(f"Productos PostgreSQL: {df_productos_pg.count():,}")
print(f"Productos ERP: {df_productos_erp.count():,}")
print(f"Proveedores: {df_proveedores.count():,}")
print(f"Inventario: {df_inventario.count():,}")

# COMMAND ----------

df_productos_clean = df_productos_pg \
    .filter(col("precio_venta") > 0) \
    .filter(col("precio_compra") > 0) \
    .withColumn("producto_id", col("id")) \
    .withColumn("precio", col("precio_venta")) \
    .withColumn("costo", col("precio_compra")) \
    .withColumn("categoria", lit("General")) \
    .dropDuplicates(["id"]) \
    .withColumn("margen", (col("precio_venta") - col("precio_compra")) / col("precio_venta") * 100) \
    .withColumn("categoria_grupo", lit("Abarrotes"))

print(f"Productos limpios: {df_productos_clean.count():,}")

# COMMAND ----------

df_proveedores_clean = df_proveedores \
    .filter(col("nombre_empresa").isNotNull()) \
    .withColumn("proveedor_id", col("id")) \
    .withColumn("nombre", col("nombre_empresa")) \
    .withColumn("contacto", col("contacto_principal")) \
    .withColumn("telefono", col("telefono_principal")) \
    .withColumn("email", col("email_principal")) \
    .dropDuplicates(["id"])

print(f"Proveedores limpios: {df_proveedores_clean.count():,}")

# COMMAND ----------

df_inventario_clean = df_inventario \
    .filter(col("cantidad_actual") >= 0) \
    .withColumn("inventario_id", col("id")) \
    .withColumn("cantidad", col("cantidad_actual")) \
    .withColumn("fecha_actualizacion", col("fecha_ultimo_conteo")) \
    .dropDuplicates(["id"]) \
    .withColumn("dias_hasta_caducidad", 
        when(col("fecha_caducidad").isNotNull(), 
            datediff(col("fecha_caducidad"), current_date())
        ).otherwise(999)) \
    .withColumn("estado_caducidad",
        when(col("dias_hasta_caducidad") < 7, "Critico")
        .when(col("dias_hasta_caducidad") < 30, "Alerta")
        .when(col("dias_hasta_caducidad") < 90, "Normal")
        .otherwise("Optimo")) \
    .withColumn("alerta_stock",
        when(col("cantidad_actual") == 0, "Sin Stock")
        .when(col("cantidad_actual") < 10, "Stock Bajo")
        .otherwise("Stock OK"))

print(f"Inventario limpio: {df_inventario_clean.count():,}")

# COMMAND ----------

write_silver(df_productos_clean, "productos_clean")
write_silver(df_proveedores_clean, "proveedores_clean")
write_silver(df_inventario_clean, "inventario_clean", partition_by=["sucursal_id"])

# COMMAND ----------

optimize_table("silver.productos_clean", zorder_cols=["categoria", "proveedor_id"])
optimize_table("silver.proveedores_clean", zorder_cols=["ciudad"])
optimize_table("silver.inventario_clean", zorder_cols=["producto_id", "estado_caducidad"])

# COMMAND ----------

print("COMPLETADO: Productos e Inventario (Bronze → Silver)")
dbutils.notebook.exit("SUCCESS")
