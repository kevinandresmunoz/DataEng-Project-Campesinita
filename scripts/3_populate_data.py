# Databricks notebook source
# MAGIC %md
# MAGIC # Populate Data - La Campesinita
# MAGIC Ejecuta pipeline completo para poblar datos

# COMMAND ----------

from datetime import datetime

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion a Bronze

# COMMAND ----------

log("Iniciando ingestion a Bronze")

try:
    dbutils.notebook.run("../proceso/ingestion_to_bronze/sync_postgres_to_bronze", 1200)
    log("PostgreSQL sincronizado")
except Exception as e:
    log(f"Error en PostgreSQL: {e}")
    dbutils.notebook.exit("FAILED")

try:
    dbutils.notebook.run("../proceso/ingestion_to_bronze/sync_mysql_to_bronze", 1200)
    log("MySQL sincronizado")
except Exception as e:
    log(f"Error en MySQL: {e}")
    dbutils.notebook.exit("FAILED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze to Silver

# COMMAND ----------

log("Iniciando transformacion Bronze to Silver")

notebooks_silver = [
    "../proceso/bronze_to_silver/ventas_consolidadas",
    "../proceso/bronze_to_silver/clientes_segmentados",
    "../proceso/bronze_to_silver/inventario_productos"
]

for nb in notebooks_silver:
    try:
        dbutils.notebook.run(nb, 1200)
        log(f"Completado: {nb.split('/')[-1]}")
    except Exception as e:
        log(f"Error en {nb}: {e}")
        dbutils.notebook.exit("FAILED")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver to Gold

# COMMAND ----------

log("Iniciando transformacion Silver to Gold")

notebooks_gold = [
    "../proceso/silver_to_gold/modelo_dimensional",
    "../proceso/silver_to_gold/metricas_ventas",
    "../proceso/silver_to_gold/metricas_inventario"
]

for nb in notebooks_gold:
    try:
        dbutils.notebook.run(nb, 1200)
        log(f"Completado: {nb.split('/')[-1]}")
    except Exception as e:
        log(f"Error en {nb}: {e}")
        dbutils.notebook.exit("FAILED")

# COMMAND ----------

log("Pipeline completado exitosamente")
dbutils.notebook.exit("SUCCESS")
