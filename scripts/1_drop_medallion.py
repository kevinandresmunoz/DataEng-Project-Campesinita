# Databricks notebook source
# MAGIC %md
# MAGIC ##ADVERTENCIA: Este script eliminara TODAS las tablas y datos fisicos
# MAGIC # Drop Medallion - La Campesinita
# MAGIC Elimina tablas de Bronze, Silver y Gold

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametros

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("storage", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables y Validacion de Seguridad

# COMMAND ----------

# Obtener parametros o usar valores por defecto
catalog_input = dbutils.widgets.get("catalog")
storage_input = dbutils.widgets.get("storage")

# Si no se proporciona catalogo, usar el actual
if not catalog_input:
    catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    print(f"Catalogo detectado automaticamente: {catalog}")
    print(f"Verifica que {catalog} sea el catalogo correcto a eliminar datos, de lo contratrio proporciona el nombre del catalogo")
else:
    catalog = catalog_input
    print(f"Catalogo proporcionado: {catalog}")

# VALIDACION DE SEGURIDAD: Solo permitir en DEV
if "dev" not in catalog.lower():
    error_msg = f"ERROR: Este notebook solo puede ejecutarse en ambiente DEV. Catalogo actual: {catalog}"
    print(error_msg)
    dbutils.notebook.exit(error_msg)

# Si no se proporciona storage, inferir del catalogo
if not storage_input:
    if "dev" in catalog.lower():
        storage = "adlcampesinitadev"
    elif "prod" in catalog.lower():
        storage = "adlcampesinitaprod"
    else:
        storage = "adlcampesinitadev"
    print(f"Storage inferido: {storage}")
    print(f"Verifica que {storage} sea el storage correcto a eliminar datos, de lo contrario proporciona el nombre del storage")
else:
    storage = storage_input
    print(f"Storage proporcionado: {storage}")

print(f"\nCatalogo: {catalog}")
print(f"Storage: {storage}")
print("\n" + "="*80)
print("ADVERTENCIA: Este script eliminara TODAS las tablas y datos fisicos")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Eliminacion tablas Bronze

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.ventas")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.detalle_ventas")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.clientes")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.productos")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.sucursales")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.inventario")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.empleados")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.proveedores")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.ordenes_compra")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.movimientos_inventario")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.recepciones")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.bronze.productos_erp")

# COMMAND ----------

# REMOVE DATA (Bronze)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/postgres/ventas", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/postgres/detalle_ventas", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/postgres/clientes", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/postgres/productos", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/postgres/sucursales", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/postgres/inventario", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/postgres/empleados", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/mysql_erp/proveedores", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/mysql_erp/ordenes_compra", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/mysql_erp/movimientos_inventario", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/mysql_erp/recepciones", True)
dbutils.fs.rm(f"abfss://bronze@{storage}.dfs.core.windows.net/mysql_erp/productos_erp", True)

print("Tablas Bronze eliminadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Eliminacion tablas Silver

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.silver.clientes_clean")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.silver.productos_clean")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.silver.proveedores_clean")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.silver.inventario_clean")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.silver.ventas_clean")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.silver.detalle_ventas_clean")

# COMMAND ----------

# REMOVE DATA (Silver)
dbutils.fs.rm(f"abfss://silver@{storage}.dfs.core.windows.net/clientes_clean", True)
dbutils.fs.rm(f"abfss://silver@{storage}.dfs.core.windows.net/productos_clean", True)
dbutils.fs.rm(f"abfss://silver@{storage}.dfs.core.windows.net/proveedores_clean", True)
dbutils.fs.rm(f"abfss://silver@{storage}.dfs.core.windows.net/inventario_clean", True)
dbutils.fs.rm(f"abfss://silver@{storage}.dfs.core.windows.net/ventas_clean", True)
dbutils.fs.rm(f"abfss://silver@{storage}.dfs.core.windows.net/detalle_ventas_clean", True)

print("Tablas Silver eliminadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Eliminacion tablas Gold - Dimensiones

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.dim_tiempo")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.dim_clientes")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.dim_productos")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.dim_sucursales")

# COMMAND ----------

# REMOVE DATA (Gold - Dimensiones)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/dim_tiempo", True)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/dim_clientes", True)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/dim_productos", True)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/dim_sucursales", True)

print("Dimensiones Gold eliminadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Eliminacion tablas Gold - Hechos

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.fact_ventas")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.fact_kpis_diarios")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.fact_analisis_clientes")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.fact_rendimiento_productos")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.fact_inventario")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.fact_kpis_inventario")
spark.sql(f"DROP TABLE IF EXISTS {catalog}.gold.fact_alertas_inventario")

# COMMAND ----------

# REMOVE DATA (Gold - Hechos)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/fact_ventas", True)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/fact_kpis_diarios", True)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/fact_analisis_clientes", True)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/fact_rendimiento_productos", True)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/fact_inventario", True)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/fact_kpis_inventario", True)
dbutils.fs.rm(f"abfss://gold@{storage}.dfs.core.windows.net/fact_alertas_inventario", True)

print("Hechos Gold eliminados")

# COMMAND ----------

print(f"Todas las tablas del catalogo {catalog} han sido eliminadas exitosamente")
