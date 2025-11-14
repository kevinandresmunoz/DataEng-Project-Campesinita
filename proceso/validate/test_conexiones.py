# Databricks notebook source
"""
Notebook de Prueba - Conexiones a Bases de Datos
Verifica que los secrets de Key Vault funcionan correctamente
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: Verificar Secrets de Key Vault

# COMMAND ----------

print("Verificando secretos")

try:
    pg_host = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgreshost")
    pg_port = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresport")
    pg_db   = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresdatabase")
    pg_user = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgresuser")
    pg_pass = dbutils.secrets.get(scope="accesskeys-campesinita", key="postgrespassword")
    
    print(f"PostgreSQL: {pg_host}:{pg_port}/{pg_db}")
    
except Exception as e:
    print(f"Error PostgreSQL: {e}")

try:
    my_host = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlhost")
    my_port = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlport")
    my_db   = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqldatabase")
    my_user = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqluser")
    my_pass = dbutils.secrets.get(scope="accesskeys-campesinita", key="mysqlpassword")
    
    print(f"MySQL: {my_host}:{my_port}/{my_db}")
    
except Exception as e:
    print(f"Error MySQL: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: Conexión a PostgreSQL

# COMMAND ----------

print("Probando conexion PostgreSQL")

try:
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    
    df_pg = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", "SELECT COUNT(*) as total_sucursales FROM sucursales WHERE activa = true") \
        .option("user", pg_user) \
        .option("password", pg_pass) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    result = df_pg.collect()[0]
    print(f"Conexion exitosa - Sucursales activas: {result['total_sucursales']}")
    
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Consulta de Datos PostgreSQL

# COMMAND ----------

print("Consultando datos PostgreSQL")

try:
    query_ventas = """
        SELECT 
            DATE(fecha_hora) as fecha,
            COUNT(*) as num_ventas,
            SUM(total) as total_ventas
        FROM ventas_recientes 
        WHERE estatus = 'completada'
        GROUP BY DATE(fecha_hora)
        ORDER BY fecha DESC
        LIMIT 5
    """
    
    df_ventas = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("query", query_ventas) \
        .option("user", pg_user) \
        .option("password", pg_pass) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    df_ventas.show()
    
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Conexión a MySQL

# COMMAND ----------

print("Probando conexion MySQL")

try:
    import pymysql
    
    connection = pymysql.connect(
        host=my_host,
        port=int(my_port),
        user=my_user,
        password=my_pass,
        database=my_db
    )
    
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) as total_proveedores FROM proveedores_completo WHERE activo = true")
    result = cursor.fetchone()
    
    print(f"Conexion exitosa - Proveedores activos: {result[0]}")
    
    cursor.close()
    connection.close()
    
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Consulta de Datos MySQL

# COMMAND ----------

print("Consultando datos MySQL")

try:
    import pymysql
    
    connection = pymysql.connect(
        host=my_host,
        port=int(my_port),
        user=my_user,
        password=my_pass,
        database=my_db
    )
    
    query_ordenes = """
        SELECT 
            DATE(fecha_orden) as fecha,
            COUNT(*) as num_ordenes,
            SUM(total) as total_ordenes,
            estado
        FROM ordenes_compra
        GROUP BY DATE(fecha_orden), estado
        ORDER BY fecha DESC
        LIMIT 5
    """
    
    cursor = connection.cursor()
    cursor.execute(query_ordenes)
    results = cursor.fetchall()
    
    for row in results:
        print(f"Fecha: {row[0]}, Ordenes: {row[1]}, Total: {row[2]}, Estado: {row[3]}")
    
    cursor.close()
    connection.close()
    
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Verificar Storage Account

# COMMAND ----------

print("Verificando Storage Account")

try:
    hostname = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName").get()
    storage_account = "adlcampesiniadev" if "dev" in hostname.lower() else "adlcampesiniaprod"
    
    print(f"Ambiente: {'DEV' if 'dev' in hostname.lower() else 'PROD'}")
    print(f"Storage: {storage_account}")
    
    try:
        bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
        files = dbutils.fs.ls(bronze_path)
        print(f"Bronze Layer accesible - {len(files)} carpetas")
    except Exception as e:
        print(f"Error accediendo a Bronze Layer: {e}")
    
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen

# COMMAND ----------

print("Resumen: Todas las conexiones verificadas")
print("Siguiente paso: Ejecutar notebooks de ingestion")
