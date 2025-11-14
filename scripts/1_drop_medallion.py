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

# Obtener parametros
catalog_input = dbutils.widgets.get("catalog")
storage_input = dbutils.widgets.get("storage")

# Detectar catalogo correcto
if catalog_input:
    # Si se proporciona catalogo, usarlo
    catalog = catalog_input
    print(f"Catalogo proporcionado: {catalog}")
else:
    # Buscar catalogo que contenga 'campesinita' y tenga tablas
    print("Buscando catalogo correcto...")
    catalogs = spark.sql("SHOW CATALOGS").collect()
    catalog = None
    
    for row in catalogs:
        cat_name = row[0]
        if "campesinita" in cat_name.lower():
            try:
                # Verificar si tiene schema bronze
                spark.sql(f"USE CATALOG {cat_name}")
                schemas = spark.sql(f"SHOW SCHEMAS IN {cat_name}").collect()
                has_bronze = any("bronze" in s[0].lower() for s in schemas)
                
                if has_bronze:
                    catalog = cat_name
                    print(f"Catalogo encontrado: {catalog}")
                    break
            except:
                continue
    
    if not catalog:
        print("No se encontro catalogo con 'campesinita', usando catalogo actual")
        catalog = spark.sql("SELECT current_catalog()").collect()[0][0]

# VALIDACION DE SEGURIDAD: Solo permitir en DEV
if "dev" not in catalog.lower():
    error_msg = f"ERROR: Este notebook solo puede ejecutarse en ambiente DEV. Catalogo actual: {catalog}"
    print(error_msg)
    dbutils.notebook.exit(error_msg)

# Inferir storage del catalogo
if storage_input:
    storage = storage_input
    print(f"Storage proporcionado: {storage}")
else:
    if "dev" in catalog.lower():
        storage = "adlcampesinitadev"
    elif "prod" in catalog.lower():
        storage = "adlcampesinitaprod"
    else:
        storage = "adlcampesinitadev"
    print(f"Storage inferido: {storage}")

print(f"\nCatalogo: {catalog}")
print(f"Storage: {storage}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PELIGRO: Eliminar Catalogo Completo (Opcional)
# MAGIC 
# MAGIC Esta opcion elimina el catalogo completo incluyendo toda la metadata.
# MAGIC 
# MAGIC Solo usar en fase de desarrollo para limpiar completamente.
# MAGIC 
# MAGIC Por defecto esta DESHABILITADO.

# COMMAND ----------

# Widget para habilitar eliminacion de catalogo
dbutils.widgets.dropdown("eliminar_catalogo", "NO", ["NO", "SI"])
dbutils.widgets.dropdown("confirmar_eliminacion", "NO", ["NO", "SI_ESTOY_SEGURO"])

# COMMAND ----------

eliminar_catalogo = dbutils.widgets.get("eliminar_catalogo")
confirmar_eliminacion = dbutils.widgets.get("confirmar_eliminacion")

if eliminar_catalogo == "SI" and confirmar_eliminacion == "SI_ESTOY_SEGURO":
    print("\n" + "!"*80)
    print("PELIGRO: ELIMINANDO CATALOGO COMPLETO")
    print("!"*80)
    print(f"Catalogo a eliminar: {catalog}")
    print("Esto eliminara TODA la metadata del catalogo")
    print("!"*80 + "\n")
    
    spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")
    
    print(f"Catalogo {catalog} eliminado completamente")
    print("Debes ejecutar el DDL nuevamente para recrear la estructura")
    dbutils.notebook.exit("CATALOG_DROPPED")
elif eliminar_catalogo == "SI" and confirmar_eliminacion == "NO":
    print("\n" + "="*80)
    print("ADVERTENCIA: Seleccionaste eliminar catalogo pero NO confirmaste")
    print("Cambia 'confirmar_eliminacion' a 'SI_ESTOY_SEGURO' para proceder")
    print("="*80 + "\n")
else:
    print("\n" + "="*80)
    print("Eliminacion de catalogo DESHABILITADA (por defecto)")
    print("Solo se eliminaran tablas individuales, el catalogo permanecera")
    print("="*80 + "\n")

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
