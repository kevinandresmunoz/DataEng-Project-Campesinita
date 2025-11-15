# Databricks notebook source
# MAGIC %md
# MAGIC # Reversion de Permisos - La Campesinita
# MAGIC Revoca todos los permisos del grupo analitica

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametros

# COMMAND ----------

dbutils.widgets.text("catalog", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables

# COMMAND ----------

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
# MAGIC ## Revocar Permisos en Tablas

# COMMAND ----------

spark.sql(f"REVOKE SELECT ON ALL TABLES IN SCHEMA {catalog}.bronze FROM analitica")
spark.sql(f"REVOKE SELECT, MODIFY ON ALL TABLES IN SCHEMA {catalog}.silver FROM analitica")
spark.sql(f"REVOKE SELECT, MODIFY ON ALL TABLES IN SCHEMA {catalog}.gold FROM analitica")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revocar Permisos en Schemas

# COMMAND ----------

spark.sql(f"REVOKE SELECT ON SCHEMA {catalog}.bronze FROM analitica")
spark.sql(f"REVOKE USAGE ON SCHEMA {catalog}.bronze FROM analitica")

spark.sql(f"REVOKE CREATE TABLE ON SCHEMA {catalog}.silver FROM analitica")
spark.sql(f"REVOKE MODIFY ON SCHEMA {catalog}.silver FROM analitica")
spark.sql(f"REVOKE SELECT ON SCHEMA {catalog}.silver FROM analitica")
spark.sql(f"REVOKE USAGE ON SCHEMA {catalog}.silver FROM analitica")

spark.sql(f"REVOKE CREATE TABLE ON SCHEMA {catalog}.gold FROM analitica")
spark.sql(f"REVOKE MODIFY ON SCHEMA {catalog}.gold FROM analitica")
spark.sql(f"REVOKE SELECT ON SCHEMA {catalog}.gold FROM analitica")
spark.sql(f"REVOKE USAGE ON SCHEMA {catalog}.gold FROM analitica")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revocar Permisos en Catalogo

# COMMAND ----------

spark.sql(f"REVOKE USAGE ON CATALOG {catalog} FROM analitica")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revocar Permisos en External Locations

# COMMAND ----------

spark.sql(f"REVOKE CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-{ambiente}-bronze` FROM analitica")
spark.sql(f"REVOKE CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-{ambiente}-silver` FROM analitica")
spark.sql(f"REVOKE CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-{ambiente}-gold` FROM analitica")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificacion

# COMMAND ----------

spark.sql(f"SHOW GRANTS ON CATALOG {catalog} TO analitica").show()

# COMMAND ----------

spark.sql(f"SHOW GRANTS ON SCHEMA {catalog}.bronze TO analitica").show()

# COMMAND ----------

spark.sql(f"SHOW GRANTS ON SCHEMA {catalog}.silver TO analitica").show()

# COMMAND ----------

spark.sql(f"SHOW GRANTS ON SCHEMA {catalog}.gold TO analitica").show()

# COMMAND ----------

print("Permisos revocados exitosamente")
