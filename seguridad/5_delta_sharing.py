# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing - La Campesinita
# MAGIC Configuracion basica de comparticion de datos

# COMMAND ----------

# Widgets para parametrizacion
dbutils.widgets.dropdown("ambiente", "dev", ["dev", "prod"], "Ambiente")

# Obtener parametros
ambiente = dbutils.widgets.get("ambiente")
catalog = f"adbslacampesinita{ambiente}"

print(f"Ambiente: {ambiente}")
print(f"Catalogo: {catalog}")

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
