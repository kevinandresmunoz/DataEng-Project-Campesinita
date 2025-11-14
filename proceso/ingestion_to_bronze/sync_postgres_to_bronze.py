# Databricks notebook source
"""
Sincronizacion PostgreSQL a Bronze Layer
Estrategia: Escribir a tabla temporal y swap atomico
"""

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from datetime import datetime

# COMMAND ----------

jdbc_hostname = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgreshost")
jdbc_port = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresport")
jdbc_database = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresdatabase")
jdbc_username = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresuser")
jdbc_password = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgrespassword")

try:
    catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
except:
    catalog = "adbslacampesinitadev"

print(f"Catalogo: {catalog}")

jdbc_url = f"jdbc:postgresql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

# COMMAND ----------

def sync_table(table_name, query, bronze_table):
    """
    Sincroniza tabla con estrategia segura:
    1. Lee desde PostgreSQL
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
            .option("driver", "org.postgresql.Driver") \
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

print("Iniciando sincronizacion PostgreSQL")
inicio = datetime.now()
exitosas = 0
totales = 7

# COMMAND ----------

if sync_table("sucursales", 
              "SELECT sucursal_id, nombre, ciudad, region, gerente FROM sucursales WHERE activa = true",
              f"{catalog}.bronze.sucursales"):
    exitosas += 1

# COMMAND ----------

if sync_table("clientes",
              "SELECT cliente_id, nombre, email, telefono, ciudad, fecha_registro, nivel_credito FROM clientes WHERE activo = true",
              f"{catalog}.bronze.clientes"):
    exitosas += 1

# COMMAND ----------

if sync_table("productos",
              "SELECT producto_id, nombre, categoria, precio, costo, proveedor_id FROM productos WHERE activo = true",
              f"{catalog}.bronze.productos"):
    exitosas += 1

# COMMAND ----------

if sync_table("empleados",
              "SELECT empleado_id, nombre, sucursal_id, cargo, salario, fecha_contratacion FROM empleados WHERE activo = true",
              f"{catalog}.bronze.empleados"):
    exitosas += 1

# COMMAND ----------

if sync_table("inventario",
              "SELECT inventario_id, producto_id, sucursal_id, cantidad, fecha_actualizacion, fecha_caducidad FROM inventario WHERE activo = true",
              f"{catalog}.bronze.inventario"):
    exitosas += 1

# COMMAND ----------

if sync_table("ventas",
              "SELECT venta_id, fecha, cliente_id, sucursal_id, total, estado, metodo_pago FROM ventas WHERE estado = 'completada'",
              f"{catalog}.bronze.ventas"):
    exitosas += 1

# COMMAND ----------

if sync_table("detalle_ventas",
              "SELECT detalle_id, venta_id, producto_id, cantidad, precio_unitario, subtotal FROM detalle_ventas",
              f"{catalog}.bronze.detalle_ventas"):
    exitosas += 1

# COMMAND ----------

duracion = (datetime.now() - inicio).total_seconds()
print(f"Completado: {exitosas}/{totales} tablas en {duracion:.1f}s")

if exitosas == totales:
    dbutils.notebook.exit("SUCCESS")
else:
    dbutils.notebook.exit(f"PARTIAL: {exitosas}/{totales}")
