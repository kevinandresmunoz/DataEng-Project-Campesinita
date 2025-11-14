# Databricks notebook source
"""
Sincronizacion MySQL a Bronze Layer
Estrategia: Escribir a tabla temporal y swap atomico
"""

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from datetime import datetime

# COMMAND ----------

jdbc_hostname = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlhost")
jdbc_port = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlport")
jdbc_database = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqldatabase")
jdbc_username = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqluser")
jdbc_password = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlpassword")

try:
    catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
except:
    catalog = "adbslacampesinitadev"

print(f"Catalogo: {catalog}")

jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

# COMMAND ----------

def sync_table(table_name, query, bronze_table):
    """
    Sincroniza tabla con estrategia segura:
    1. Lee desde MySQL
    2. Escribe a tabla temporal
    3. Swap atomico (TRUNCATE + INSERT)
    """
    print(f"Sincronizando {table_name}")
    
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("user", jdbc_username) \
            .option("password", jdbc_password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        
        df_audit = df.withColumn("_sync_timestamp", current_timestamp())
        
        df_audit.createOrReplaceTempView("temp_sync")
        
        spark.sql(f"TRUNCATE TABLE {bronze_table}")
        spark.sql(f"INSERT INTO {bronze_table} SELECT * FROM temp_sync")
        
        count = df_audit.count()
        print(f"{table_name}: {count:,} registros")
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

if sync_table("proveedores",
              "SELECT proveedor_id, nombre, contacto, telefono, email, ciudad FROM proveedores WHERE activo = true",
              f"{catalog}.bronze.proveedores"):
    exitosas += 1

# COMMAND ----------

if sync_table("ordenes_compra",
              "SELECT orden_id, proveedor_id, fecha_orden, estado, total FROM ordenes_compra",
              f"{catalog}.bronze.ordenes_compra"):
    exitosas += 1

# COMMAND ----------

if sync_table("detalle_ordenes",
              "SELECT detalle_orden_id, orden_id, producto_id, cantidad, precio_unitario FROM detalle_ordenes",
              f"{catalog}.bronze.detalle_ordenes"):
    exitosas += 1

# COMMAND ----------

if sync_table("recepciones",
              "SELECT recepcion_id, orden_id, fecha_recepcion, sucursal_id, estado FROM recepciones",
              f"{catalog}.bronze.recepciones"):
    exitosas += 1

# COMMAND ----------

if sync_table("productos_erp",
              "SELECT producto_id, codigo_barras, nombre, categoria, unidad_medida FROM productos",
              f"{catalog}.bronze.productos_erp"):
    exitosas += 1

# COMMAND ----------

duracion = (datetime.now() - inicio).total_seconds()
print(f"Completado: {exitosas}/{totales} tablas en {duracion:.1f}s")

if exitosas == totales:
    dbutils.notebook.exit("SUCCESS")
else:
    dbutils.notebook.exit(f"PARTIAL: {exitosas}/{totales}")
