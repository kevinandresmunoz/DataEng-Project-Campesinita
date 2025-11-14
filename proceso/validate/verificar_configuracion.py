# Databricks notebook source
"""
Verificacion de Configuracion - Databricks
Verifica que todo este configurado correctamente antes de procesar
"""

# COMMAND ----------

print("Verificacion de configuracion Databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar Secrets

# COMMAND ----------

print("1. Verificando secrets")

secrets_ok = True

try:
    account_name = dbutils.secrets.get(scope="azure-storage", key="account-name")
    if account_name and account_name != "your_storage_account":
        print("OK: account-name configurado")
    else:
        print("Error: account-name no configurado")
        secrets_ok = False
except:
    print("Error: No se puede acceder a secret account-name")
    secrets_ok = False

try:
    account_key = dbutils.secrets.get(scope="azure-storage", key="account-key")
    if account_key:
        print("OK: account-key configurado")
    else:
        print("Error: account-key no configurado")
        secrets_ok = False
except:
    print("Error: No se puede acceder a secret account-key")
    secrets_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verificar Montajes

# COMMAND ----------

print("2. Verificando montajes")

montajes_ok = True
required_mounts = ["/mnt/bronze"]

mounts = {mount.mountPoint: mount.source for mount in dbutils.fs.mounts()}

for mount_point in required_mounts:
    if mount_point in mounts:
        print(f"OK: {mount_point} montado - {mounts[mount_point]}")
    else:
        print(f"Error: {mount_point} no montado")
        montajes_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar Databases

# COMMAND ----------

print("3. Verificando databases")

databases_ok = True
required_dbs = ["silver", "gold"]

existing_dbs = [db.databaseName for db in spark.sql("SHOW DATABASES").collect()]

for db in required_dbs:
    if db in existing_dbs:
        print(f"OK: Database {db} existe")
    else:
        print(f"Error: Database {db} no existe")
        databases_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar Datos en Bronze

# COMMAND ----------

print("4. Verificando datos en Bronze")

bronze_ok = True

try:
    folders = dbutils.fs.ls("/mnt/bronze")
    
    if len(folders) == 0:
        print("Error: Bronze Layer vacio - ejecutar sincronizacion")
        bronze_ok = False
    else:
        print(f"OK: {len(folders)} carpetas en Bronze")
        for folder in folders:
            print(f"  {folder.name}")
                
except Exception as e:
    print(f"Error accediendo a Bronze: {e}")
    bronze_ok = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumen

# COMMAND ----------

all_ok = secrets_ok and montajes_ok and databases_ok and bronze_ok

if all_ok:
    print("Configuracion completa - listo para ejecutar pipeline")
else:
    print("Configuracion incompleta:")
    if not secrets_ok:
        print("  Secrets no configurados")
    if not montajes_ok:
        print("  Montajes faltantes")
    if not databases_ok:
        print("  Databases faltantes")
    if not bronze_ok:
        print("  Datos en Bronze faltantes")
    print("Ejecutar setup_databricks.py y sincronizar datos")
