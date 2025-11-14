# Databricks notebook source
"""
Sincronizacion PostgreSQL a Bronze Layer
Estrategia: Escribir a tabla temporal y swap atomico
"""

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from datetime import datetime

# COMMAND ----------

# Buscar catalogo que contenga "campesinita" y "dev"
catalogs = spark.sql("SHOW CATALOGS").collect()
catalog = None

for row in catalogs:
    cat_name = row[0]
    # Buscar cualquier catalogo con "campesinita" y "dev"
    if "campesinita" in cat_name.lower() and "dev" in cat_name.lower():
        catalog = cat_name
        break

if not catalog:
    raise Exception("No se encontro un catalogo con 'campesinita' y 'dev'. Ejecuta el DDL primero.")

print(f"Catalogo encontrado: {catalog}")

# Usar el catalogo y schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA bronze")
print(f"Usando: {catalog}.bronze")

# Obtener secrets de conexion
jdbc_hostname = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgreshost")
jdbc_port = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresport")
jdbc_database = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresdatabase")
jdbc_username = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresuser")
jdbc_password = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgrespassword")

jdbc_url = f"jdbc:postgresql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

# COMMAND ----------

def sync_table(table_name, bronze_table):
    """
    Sincroniza tabla completa sin filtros (Bronze = espejo de la fuente)
    1. Lee TODA la tabla desde PostgreSQL
    2. Agrega timestamp de sincronizacion
    3. INSERT OVERWRITE atomico
    """
    print(f"Sincronizando {table_name}")
    
    try:
        # Leer tabla completa sin filtros
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", jdbc_username) \
            .option("password", jdbc_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        # Eliminar _sync_timestamp si ya existe (para evitar duplicados)
        if "_sync_timestamp" in df.columns:
            df = df.drop("_sync_timestamp")
        
        # Agregar timestamp de auditoria
        df_audit = df.withColumn("_sync_timestamp", current_timestamp())
        
        # INSERT OVERWRITE atomico
        df_audit.createOrReplaceTempView("temp_sync")
        spark.sql(f"INSERT OVERWRITE TABLE {bronze_table} SELECT * FROM temp_sync")
        
        count = df_audit.count()
        print(f"{table_name}: {count:,} registros sincronizados")
        return True
        
    except Exception as e:
        print(f"Error en {table_name}: {e}")
        return False

# COMMAND ----------

print("Iniciando sincronizacion PostgreSQL")
inicio = datetime.now()
exitosas = 0
totales = 7

# COMMAND ----------

if sync_table("sucursales", f"{catalog}.bronze.sucursales"):
    exitosas += 1

# COMMAND ----------

if sync_table("clientes", f"{catalog}.bronze.clientes"):
    exitosas += 1

# COMMAND ----------

if sync_table("productos_activos", f"{catalog}.bronze.productos"):
    exitosas += 1

# COMMAND ----------

if sync_table("empleados_operativos", f"{catalog}.bronze.empleados"):
    exitosas += 1

# COMMAND ----------

if sync_table("inventario_actual", f"{catalog}.bronze.inventario"):
    exitosas += 1

# COMMAND ----------

if sync_table("ventas_recientes", f"{catalog}.bronze.ventas"):
    exitosas += 1

# COMMAND ----------

if sync_table("detalle_ventas_recientes", f"{catalog}.bronze.detalle_ventas"):
    exitosas += 1

# COMMAND ----------

duracion = (datetime.now() - inicio).total_seconds()
print(f"Completado: {exitosas}/{totales} tablas en {duracion:.1f}s")

if exitosas == totales:
    dbutils.notebook.exit("SUCCESS")
else:
    dbutils.notebook.exit(f"PARTIAL: {exitosas}/{totales}")
