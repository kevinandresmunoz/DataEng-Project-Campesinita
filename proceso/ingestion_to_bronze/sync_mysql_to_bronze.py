# Databricks notebook source
"""
Sincronización MySQL ERP → Bronze Layer
Ejecutado por Azure Data Factory 2 veces al dia (1 PM y 9 PM hora Colombia)
"""

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

# Obtener credenciales desde Key Vault
jdbc_hostname = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlhost")
jdbc_port = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlport")
jdbc_database = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqldatabase")
jdbc_username = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqluser")
jdbc_password = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlpassword")

# Detectar catalogo (dev o prod)
try:
    current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    catalog = current_catalog
except:
    catalog = "adbslacampesinitadev"  # Fallback a dev

print(f"Catalogo: {catalog}")

# JDBC URL
jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones de Sincronización

# COMMAND ----------

def sync_table_to_bronze(table_name, query, catalog_table):
    """Sincroniza tabla de MySQL a Bronze Layer usando INSERT INTO"""
    print(f"Sincronizando {table_name}")
    
    try:
        # Leer desde MySQL
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("user", jdbc_username) \
            .option("password", jdbc_password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        
        # Agregar metadatos de auditoria
        df_audit = df \
            .withColumn("_sync_timestamp", current_timestamp())
        
        # Crear vista temporal
        df_audit.createOrReplaceTempView("temp_sync")
        
        # Truncar tabla Bronze (eliminar datos anteriores)
        spark.sql(f"TRUNCATE TABLE {catalog_table}")
        
        # Insertar datos nuevos
        spark.sql(f"INSERT INTO {catalog_table} SELECT * FROM temp_sync")
        
        count = df_audit.count()
        print(f"{table_name}: {count:,} registros insertados")
        return True
        
    except Exception as e:
        print(f"Error en {table_name}: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sincronización de Tablas

# COMMAND ----------
print("Sincronizacion MySQL ERP a Bronze Layer")

inicio = datetime.now()
tablas_exitosas = 0
tablas_totales = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Proveedores

# COMMAND ----------

tablas_totales += 1
query = "SELECT proveedor_id, nombre, contacto, telefono, email, ciudad FROM proveedores WHERE activo = true"
if sync_table_to_bronze("proveedores", query, f"{catalog}.bronze.proveedores"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Productos ERP

# COMMAND ----------

tablas_totales += 1
query = "SELECT producto_id, codigo_barras, nombre, categoria, unidad_medida FROM productos WHERE activo = true"
if sync_table_to_bronze("productos_erp", query, f"{catalog}.bronze.productos_erp"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Ordenes de Compra

# COMMAND ----------

tablas_totales += 1
query = "SELECT orden_id, proveedor_id, fecha_orden, estado, total FROM ordenes_compra"
if sync_table_to_bronze("ordenes_compra", query, f"{catalog}.bronze.ordenes_compra"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Detalle Ordenes

# COMMAND ----------

tablas_totales += 1
query = "SELECT detalle_orden_id, orden_id, producto_id, cantidad, precio_unitario FROM detalle_ordenes"
if sync_table_to_bronze("detalle_ordenes", query, f"{catalog}.bronze.detalle_ordenes"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Recepciones

# COMMAND ----------

tablas_totales += 1
query = "SELECT recepcion_id, orden_id, fecha_recepcion, sucursal_id, estado FROM recepciones"
if sync_table_to_bronze("recepciones", query, f"{catalog}.bronze.recepciones"):
    tablas_exitosas += 1
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen

# COMMAND ----------

fin = datetime.now()
duracion = (fin - inicio).total_seconds()

print(f"Completado: {tablas_exitosas}/{tablas_totales} tablas en {duracion:.1f}s")

if tablas_exitosas == tablas_totales:
    print("Todas las tablas sincronizadas correctamente")
    dbutils.notebook.exit("SUCCESS")
else:
    print(f"{tablas_totales - tablas_exitosas} tablas fallaron")
    dbutils.notebook.exit("PARTIAL_SUCCESS")
