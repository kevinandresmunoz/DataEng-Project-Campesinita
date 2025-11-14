# Databricks notebook source
"""
Construcción del modelo dimensional para análisis de negocio.
Genera las dimensiones necesarias para consultas analíticas optimizadas.
"""

# COMMAND ----------
# MAGIC %run ../Include/utils

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta

# COMMAND ----------

log("Iniciando construcción del modelo dimensional")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensión Tiempo
# MAGIC 
# MAGIC **Origen:** Generada desde código en Databricks (no existe en fuentes)
# MAGIC 
# MAGIC Genera un calendario completo con atributos temporales para análisis.
# MAGIC Cubre el período 2024-2025 para alinear con los datos históricos disponibles.
# MAGIC 
# MAGIC Esta dimensión se crea completamente en Databricks porque:
# MAGIC - No existe en las bases de datos operacionales
# MAGIC - Necesita atributos calculados (trimestre, temporada, tipo_dia)
# MAGIC - Es estática y no cambia con las sincronizaciones

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

log(f"Dimensión Tiempo: {df_dim_tiempo.count():,} días (2024-2025)")
write_gold(df_dim_tiempo, "dim_tiempo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensión Clientes
# MAGIC 
# MAGIC **Origen:** Transformada desde Silver (silver.clientes_clean)
# MAGIC 
# MAGIC Consolida información de clientes con segmentación de negocio.
# MAGIC Incluye clasificaciones calculadas en la capa Silver:
# MAGIC - Segmentación RFM (Recency, Frequency, Monetary)
# MAGIC - Categoría por antigüedad (Nuevo, Reciente, Establecido, Veterano)
# MAGIC - Nivel de crédito (Bajo, Medio, Alto)
# MAGIC 
# MAGIC La segmentación se calcula en Silver para mantener Gold optimizado.

# COMMAND ----------

df_clientes = spark.table("silver.clientes_clean")

df_dim_clientes = df_clientes.select(
    col("id").alias("cliente_key"),
    col("codigo_cliente"),
    col("nombre_completo"),
    col("email_lower").alias("email"),
    col("telefono_clean").alias("telefono"),
    col("ciudad"),
    col("tipo_cliente"),
    col("segmento_negocio"),
    col("categoria_antiguedad"),
    col("nivel_credito"),
    col("credito_limite"),
    col("puntos_acumulados"),
    col("fecha_registro"),
    col("antiguedad_dias"),
    current_timestamp().alias("fecha_actualizacion")
)

log(f"Dimensión Clientes: {df_dim_clientes.count():,}")
write_gold(df_dim_clientes, "dim_clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensión Productos
# MAGIC 
# MAGIC **Origen:** Transformada desde Silver (silver.productos_clean)
# MAGIC 
# MAGIC Catálogo de productos con métricas de rentabilidad.
# MAGIC Incorpora cálculos realizados en Silver:
# MAGIC - Consolidación de PostgreSQL + MySQL (deduplicación)
# MAGIC - Márgenes calculados (absoluto y porcentaje)
# MAGIC - Clasificación por categoría de margen (Bajo, Medio, Alto)
# MAGIC - Clasificación por rango de precio (Económico, Medio, Premium)
# MAGIC 
# MAGIC Silver consolida productos de ambas fuentes y calcula métricas.

# COMMAND ----------

df_productos = spark.table("silver.productos_clean")

df_dim_productos = df_productos.select(
    col("producto_id").alias("producto_key"),
    col("codigo_producto"),
    col("nombre").alias("producto_nombre"),
    col("categoria_id"),
    col("precio_compra"),
    col("precio_venta"),
    col("margen_absoluto"),
    col("margen_porcentaje"),
    col("categoria_margen"),
    col("rango_precio"),
    col("perecedero"),
    current_timestamp().alias("fecha_actualizacion")
)

log(f"Dimensión Productos: {df_dim_productos.count():,}")
write_gold(df_dim_productos, "dim_productos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensión Sucursales
# MAGIC 
# MAGIC **Origen:** Leída directamente desde Bronze (bronze/postgres/sucursales)
# MAGIC 
# MAGIC Catálogo de sucursales sincronizado desde PostgreSQL.
# MAGIC 
# MAGIC Esta dimensión se lee directamente de Bronze porque:
# MAGIC - Son datos maestros que no requieren transformación
# MAGIC - Se sincronizan desde PostgreSQL (tabla copiada desde maestros)
# MAGIC - Contienen información geográfica y operativa completa
# MAGIC - No necesitan limpieza ni enriquecimiento
# MAGIC 
# MAGIC Sincronización: Automática via Azure Data Factory 2 veces al dia (1 PM y 9 PM hora Colombia)
# MAGIC O manual ejecutando notebooks de ingestion durante setup inicial
# MAGIC Frecuencia recomendada: 2 veces al dia o cuando cambien datos maestros

# COMMAND ----------

df_sucursales_bronze = read_bronze("postgres", "sucursales")

df_dim_sucursales = df_sucursales_bronze.select(
    col("id").alias("sucursal_key"),
    col("nombre").alias("sucursal_nombre"),
    col("ciudad"),
    col("region"),
    col("area_metros"),
    col("gerente"),
    col("email"),
    col("telefono"),
    col("direccion"),
    col("fecha_apertura"),
    col("activa"),
    current_timestamp().alias("fecha_actualizacion")
)

log(f"Dimensión Sucursales: {df_dim_sucursales.count():,}")
write_gold(df_dim_sucursales, "dim_sucursales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimización de Tablas
# MAGIC 
# MAGIC Aplica técnicas de optimización para mejorar el rendimiento de consultas.

# COMMAND ----------

optimize_table("gold.dim_tiempo", zorder_cols=["fecha_key", "anio", "mes"])
optimize_table("gold.dim_clientes", zorder_cols=["cliente_key", "ciudad"])
optimize_table("gold.dim_productos", zorder_cols=["producto_key", "categoria_id"])
optimize_table("gold.dim_sucursales", zorder_cols=["sucursal_key", "ciudad"])

# COMMAND ----------

log("Modelo dimensional completado")
