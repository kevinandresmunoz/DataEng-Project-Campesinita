# Databricks notebook source
"""
Configuración inicial del entorno Databricks.
Monta el almacenamiento y prepara las bases de datos necesarias.
"""

# COMMAND ----------

print("Setup de Databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación de Secrets

# COMMAND ----------

print("\nVerificando secrets...")

try:
    account_name = dbutils.secrets.get(scope="azure-storage", key="account-name")
    account_key = dbutils.secrets.get(scope="azure-storage", key="account-key")
    
    if not account_name or account_name == "your_storage_account":
        raise ValueError("Secret 'account-name' no configurado")
    
    if not account_key:
        raise ValueError("Secret 'account-key' no configurado")
    
    print("   Secrets configurados")
    print(f"   Storage Account: {account_name}")
    
except Exception as e:
    print(f"   Error: {e}")
    print("\n   Acciones requeridas:")
    print("   1. Crear secrets scope:")
    print("      databricks secrets create-scope --scope azure-storage")
    print("\n   2. Agregar secrets:")
    print("      databricks secrets put --scope azure-storage --key account-name")
    print("      databricks secrets put --scope azure-storage --key account-key")
    dbutils.notebook.exit("Error: Secrets no configurados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montaje de Storage

# COMMAND ----------

print("\nMontando capas de almacenamiento...")

configs = {
    f"fs.azure.account.key.{account_name}.dfs.core.windows.net": account_key
}

# Montar Bronze Layer
mount_point_bronze = "/mnt/bronze"
if any(mount.mountPoint == mount_point_bronze for mount in dbutils.fs.mounts()):
    print(f"   Bronze ya montado, remontando...")
    dbutils.fs.unmount(mount_point_bronze)

try:
    dbutils.fs.mount(
        source=f"abfss://bronze@{account_name}.dfs.core.windows.net/",
        mount_point=mount_point_bronze,
        extra_configs=configs
    )
    print(f"   [OK] Bronze Layer montado en {mount_point_bronze}")
except Exception as e:
    print(f"   [ERROR] No se pudo montar Bronze: {e}")
    dbutils.notebook.exit("Error: No se pudo montar Bronze Layer")

# Montar Silver Layer
mount_point_silver = "/mnt/silver"
if any(mount.mountPoint == mount_point_silver for mount in dbutils.fs.mounts()):
    print(f"   Silver ya montado, remontando...")
    dbutils.fs.unmount(mount_point_silver)

try:
    dbutils.fs.mount(
        source=f"abfss://silver@{account_name}.dfs.core.windows.net/",
        mount_point=mount_point_silver,
        extra_configs=configs
    )
    print(f"   [OK] Silver Layer montado en {mount_point_silver}")
except Exception as e:
    print(f"   [ERROR] No se pudo montar Silver: {e}")

# Montar Gold Layer
mount_point_gold = "/mnt/gold"
if any(mount.mountPoint == mount_point_gold for mount in dbutils.fs.mounts()):
    print(f"   Gold ya montado, remontando...")
    dbutils.fs.unmount(mount_point_gold)

try:
    dbutils.fs.mount(
        source=f"abfss://gold@{account_name}.dfs.core.windows.net/",
        mount_point=mount_point_gold,
        extra_configs=configs
    )
    print(f"   [OK] Gold Layer montado en {mount_point_gold}")
except Exception as e:
    print(f"   [ERROR] No se pudo montar Gold: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar Contenido de Capas

# COMMAND ----------

print("\n3. Verificando contenido de capas...")

# Verificar Bronze
try:
    folders_bronze = dbutils.fs.ls("/mnt/bronze")
    
    if len(folders_bronze) == 0:
        print("   [ADVERTENCIA] Bronze Layer esta vacio")
        print("   Ejecuta notebooks de ingestion: 00_ingestion_to_bronze/INGESTION_MASTER.py")
        print("   O configura Azure Data Factory para sincronizacion automatica")
    else:
        print(f"   [OK] Bronze: {len(folders_bronze)} carpetas")
        for folder in folders_bronze[:3]:  # Mostrar solo primeras 3
            print(f"      - {folder.name}")
        if len(folders_bronze) > 3:
            print(f"      ... y {len(folders_bronze) - 3} mas")
            
except Exception as e:
    print(f"   [ERROR] No se pudo leer Bronze: {e}")

# Verificar Silver
try:
    folders_silver = dbutils.fs.ls("/mnt/silver")
    print(f"   [OK] Silver: {len(folders_silver)} carpetas")
except:
    print("   [INFO] Silver vacio (normal en primera ejecucion)")

# Verificar Gold
try:
    folders_gold = dbutils.fs.ls("/mnt/gold")
    print(f"   [OK] Gold: {len(folders_gold)} carpetas")
except:
    print("   [INFO] Gold vacio (normal en primera ejecucion)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crear Databases

# COMMAND ----------

print("\n4. Creando databases...")

databases = ["silver", "gold"]

for db in databases:
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        print(f"   [OK] Database '{db}' creada/verificada")
    except Exception as e:
        print(f"   [ERROR] No se pudo crear '{db}': {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar Configuracion

# COMMAND ----------

print("\n5. Verificacion final...")

# Verificar databases
print("\n   Databases disponibles:")
databases = spark.sql("SHOW DATABASES").collect()
for db in databases:
    print(f"      - {db.databaseName}")

# Verificar montajes
print("\n   Montajes activos:")
mounts = dbutils.fs.mounts()
for mount in mounts:
    if "bronze" in mount.mountPoint or "silver" in mount.mountPoint or "gold" in mount.mountPoint:
        print(f"      - {mount.mountPoint} -> {mount.source}")

# COMMAND ----------

print("\n" + "=" * 60)
print("Setup completado")
print("=" * 60)
print("\nCapas montadas:")
print("  - /mnt/bronze (lectura de datos crudos)")
print("  - /mnt/silver (escritura de datos limpios)")
print("  - /mnt/gold (escritura de modelo dimensional)")
print("\nDatabases creadas:")
print("  - silver (tablas limpias)")
print("  - gold (dimensiones y hechos)")
print("\nSiguiente paso:")
print("  1. Sincronizar datos a Bronze:")
print("     - Opcion A: Ejecutar notebooks de ingestion manualmente")
print("     - Opcion B: Configurar Azure Data Factory (cada 6h)")
print("  2. Ejecutar scripts/orquestador_setup.py para transformaciones")
