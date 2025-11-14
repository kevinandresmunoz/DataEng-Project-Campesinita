# Databricks notebook source
"""
Sincronización PostgreSQL → Bronze Layer
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
jdbc_hostname = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgreshost")
jdbc_port = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresport")
jdbc_database = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresdatabase")
jdbc_username = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresuser")
jdbc_password = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgrespassword")

# Detectar catalogo (dev o prod)
try:
    current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    catalog = current_catalog
except:
    catalog = "adbslacampesinitadev"  # Fallback a dev

print(f"Catalogo: {catalog}")

# JDBC URL
jdbc_url = f"jdbc:postgresql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones de Sincronización

# COMMAND ----------

def sync_table_to_bronze(table_name, query, catalog_table):
    """Sincroniza tabla de PostgreSQL a Bronze Layer usando INSERT INTO"""
    print(f"Sincronizando {table_name}")
    
    try:
        # Leer desde PostgreSQL
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("user", jdbc_username) \
            .option("password", jdbc_password) \
            .option("driver", "org.postgresql.Driver") \
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

print("Sincronizacion PostgreSQL a Bronze Layer")

inicio = datetime.now()
tablas_exitosas = 0
tablas_totales = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Sucursales

# COMMAND ----------

tablas_totales += 1
query = "SELECT sucursal_id, nombre, ciudad, region, gerente FROM sucursales WHERE activa = true"
if sync_table_to_bronze("sucursales", query, f"{catalog}.bronze.sucursales"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Clientes

# COMMAND ----------

tablas_totales += 1
query = "SELECT cliente_id, nombre, email, telefono, ciudad, fecha_registro, nivel_credito FROM clientes WHERE activo = true"
if sync_table_to_bronze("clientes", query, f"{catalog}.bronze.clientes"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Productos

# COMMAND ----------

tablas_totales += 1
query = "SELECT producto_id, nombre, categoria, precio, costo, proveedor_id FROM productos WHERE activo = true"
if sync_table_to_bronze("productos", query, f"{catalog}.bronze.productos"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Empleados

# COMMAND ----------

tablas_totales += 1
query = "SELECT empleado_id, nombre, sucursal_id, cargo, salario, fecha_contratacion FROM empleados WHERE activo = true"
if sync_table_to_bronze("empleados", query, f"{catalog}.bronze.empleados"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Inventario

# COMMAND ----------

tablas_totales += 1
query = "SELECT inventario_id, producto_id, sucursal_id, cantidad, fecha_actualizacion, fecha_caducidad FROM inventario WHERE activo = true"
if sync_table_to_bronze("inventario", query, f"{catalog}.bronze.inventario"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Ventas

# COMMAND ----------

tablas_totales += 1
query = "SELECT venta_id, fecha, cliente_id, sucursal_id, total, estado, metodo_pago FROM ventas WHERE estado = 'completada'"
if sync_table_to_bronze("ventas", query, f"{catalog}.bronze.ventas"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Detalle Ventas

# COMMAND ----------

tablas_totales += 1
query = "SELECT detalle_id, venta_id, producto_id, cantidad, precio_unitario, subtotal FROM detalle_ventas"
if sync_table_to_bronze("detalle_ventas", query, f"{catalog}.bronze.detalle_ventas"):
    tablas_exitosas += 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen

# COMMAND ----------

duracion = (datetime.now() - inicio).total_seconds()

print(f"Completado: {tablas_exitosas}/{tablas_totales} tablas en {duracion:.1f}s")

if tablas_exitosas == tablas_totales:
    dbutils.notebook.exit("SUCCESS")
else:
    dbutils.notebook.exit("PARTIAL_SUCCESS")
