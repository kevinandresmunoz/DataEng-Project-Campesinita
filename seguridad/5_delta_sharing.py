# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing - La Campesinita
# MAGIC Configuracion basica de comparticion de datos

# COMMAND ----------

# Widgets para parametrizacion
dbutils.widgets.text("catalog", "")

# Obtener parametro o usar catalogo actual
catalog_input = dbutils.widgets.get("catalog")

if not catalog_input:
    catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    print(f"Catalogo detectado automaticamente: {catalog}")
else:
    catalog = catalog_input
    print(f"Catalogo proporcionado: {catalog}")

# Inferir ambiente del catalogo
if "dev" in catalog.lower():
    ambiente = "dev"
elif "prod" in catalog.lower():
    ambiente = "prod"
else:
    ambiente = "dev"
    
print(f"Ambiente inferido: {ambiente}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Share

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SHARE IF NOT EXISTS campesinita_kpis_share

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregar Tablas al Share

# COMMAND ----------

spark.sql(f"ALTER SHARE campesinita_kpis_share ADD TABLE {catalog}.gold.fact_kpis_diarios")
spark.sql(f"ALTER SHARE campesinita_kpis_share ADD TABLE {catalog}.gold.fact_kpis_inventario")

print("Tablas agregadas al share")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SHARES

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW ALL IN SHARE campesinita_kpis_share
