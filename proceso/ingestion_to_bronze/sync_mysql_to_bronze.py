# Databricks notebook source
"""
Sincronizacion MySQL a Bronze Layer
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
jdbc_hostname = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlhost")
jdbc_port = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlport")
jdbc_database = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqldatabase")
jdbc_username = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqluser")
jdbc_password = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlpassword")

jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

# COMMAND ----------

def sync_table(table_name, bronze_table):
    """
    Sincroniza tabla completa sin filtros (Bronze = espejo de la fuente)
    1. Lee TODA la tabla desde MySQL
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
            .option("driver", "com.mysql.cj.jdbc.Driver") \
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

print("Iniciando sincronizacion MySQL")
inicio = datetime.now()
exitosas = 0
totales = 5

# COMMAND ----------

if sync_table("proveedores_completo", f"{catalog}.bronze.proveedores"):
    exitosas += 1

# COMMAND ----------

if sync_table("ordenes_compra", f"{catalog}.bronze.ordenes_compra"):
    exitosas += 1

# COMMAND ----------

if sync_table("movimientos_inventario", f"{catalog}.bronze.movimientos_inventario"):
    exitosas += 1

# COMMAND ----------

if sync_table("recepciones_mercancia", f"{catalog}.bronze.recepciones"):
    exitosas += 1

# COMMAND ----------

if sync_table("productos_completo", f"{catalog}.bronze.productos_erp"):
    exitosas += 1

# COMMAND ----------

duracion = (datetime.now() - inicio).total_seconds()
print(f"Completado: {exitosas}/{totales} tablas en {duracion:.1f}s")

if exitosas == totales:
    dbutils.notebook.exit("SUCCESS")
else:
    dbutils.notebook.exit(f"PARTIAL: {exitosas}/{totales}")
