# Databricks notebook source
"""
Silver to Gold: Modelo Dimensional
Construye las 4 dimensiones del modelo
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta

# COMMAND ----------

log("=== INICIO: Construccion Modelo Dimensional ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tiempo (Generada)

# COMMAND ----------

start_date = datetime(2024, 1, 1)
end_date = datetime(2025, 12, 31)

date_list = []
current = start_date
while current <= end_date:
    date_list.append((current,))
    current += timedelta(days=1)

df_dates = spark.createDataFrame(date_list, ["fecha"])

df_dim_tiempo = df_dates.select(
    col("fecha").alias("fecha_key"),
    year(col("fecha")).alias("anio"),
    month(col("fecha")).alias("mes"),
    dayofmonth(col("fecha")).alias("dia"),
    dayofweek(col("fecha")).alias("dia_semana"),
    weekofyear(col("fecha")).alias("semana_anio"),
    quarter(col("fecha")).alias("trimestre"),
    date_format(col("fecha"), "MMMM").alias("mes_nombre"),
    date_format(col("fecha"), "EEEE").alias("dia_semana_nombre"),
    when(dayofweek(col("fecha")).isin([1, 7]), "Fin de Semana").otherwise("Entre Semana").alias("tipo_dia"),
    when(month(col("fecha")).isin([12, 1, 6, 7]), "Alta")
        .when(month(col("fecha")).isin([2, 3, 8, 9]), "Media")
        .otherwise("Baja").alias("temporada")
)

log(f"Dimension Tiempo: {df_dim_tiempo.count():,} dias")
write_gold(df_dim_tiempo, "dim_tiempo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Clientes (desde Silver)

# COMMAND ----------

df_clientes = spark.table("silver.clientes_clean")

df_dim_clientes = df_clientes.select(
    col("cliente_id").alias("cliente_key"),
    col("nombre"),
    col("email"),
    col("telefono"),
    col("ciudad"),
    col("fecha_registro"),
    col("nivel_credito"),
    col("antiguedad_dias"),
    col("segmento_antiguedad"),
    col("nivel_credito_num"),
    current_timestamp().alias("fecha_actualizacion")
)

log(f"Dimension Clientes: {df_dim_clientes.count():,}")
write_gold(df_dim_clientes, "dim_clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Productos (desde Silver)

# COMMAND ----------

df_productos = spark.table("silver.productos_clean")

df_dim_productos = df_productos.select(
    col("producto_id").alias("producto_key"),
    col("nombre").alias("producto_nombre"),
    col("categoria"),
    col("precio"),
    col("costo"),
    col("proveedor_id"),
    col("margen"),
    col("categoria_grupo"),
    current_timestamp().alias("fecha_actualizacion")
)

log(f"Dimension Productos: {df_dim_productos.count():,}")
write_gold(df_dim_productos, "dim_productos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Sucursales (desde Bronze)

# COMMAND ----------

df_sucursales = read_bronze_table("sucursales")

df_dim_sucursales = df_sucursales.select(
    col("sucursal_id").alias("sucursal_key"),
    col("nombre").alias("sucursal_nombre"),
    col("ciudad"),
    col("region"),
    col("gerente"),
    current_timestamp().alias("fecha_actualizacion")
)

log(f"Dimension Sucursales: {df_dim_sucursales.count():,}")
write_gold(df_dim_sucursales, "dim_sucursales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimizar Dimensiones

# COMMAND ----------

optimize_table("gold.dim_tiempo", zorder_cols=["fecha_key", "anio", "mes"])
optimize_table("gold.dim_clientes", zorder_cols=["cliente_key", "ciudad"])
optimize_table("gold.dim_productos", zorder_cols=["producto_key", "categoria"])
optimize_table("gold.dim_sucursales", zorder_cols=["sucursal_key", "ciudad"])

# COMMAND ----------

log("=== COMPLETADO: Modelo Dimensional ===")
dbutils.notebook.exit("SUCCESS")
