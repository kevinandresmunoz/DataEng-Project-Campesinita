# Databricks notebook source
# MAGIC %md
# MAGIC # Reversion de Permisos - La Campesinita
# MAGIC Revoca todos los permisos del grupo analitica

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametros

# COMMAND ----------

dbutils.widgets.dropdown("ambiente", "dev", ["dev", "prod"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables

# COMMAND ----------

ambiente = dbutils.widgets.get("ambiente")
catalog = f"adbslacampesinita{ambiente}"
print(f"Catalogo: {catalog}")
print(f"Ambiente: {ambiente}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revocar Permisos en Tablas

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE SELECT ON ALL TABLES IN SCHEMA ${catalog}.bronze FROM analitica;
# MAGIC REVOKE SELECT, MODIFY ON ALL TABLES IN SCHEMA ${catalog}.silver FROM analitica;
# MAGIC REVOKE SELECT, MODIFY ON ALL TABLES IN SCHEMA ${catalog}.gold FROM analitica;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revocar Permisos en Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE SELECT ON SCHEMA ${catalog}.bronze FROM analitica;
# MAGIC REVOKE USAGE ON SCHEMA ${catalog}.bronze FROM analitica;
# MAGIC
# MAGIC REVOKE CREATE TABLE ON SCHEMA ${catalog}.silver FROM analitica;
# MAGIC REVOKE MODIFY ON SCHEMA ${catalog}.silver FROM analitica;
# MAGIC REVOKE SELECT ON SCHEMA ${catalog}.silver FROM analitica;
# MAGIC REVOKE USAGE ON SCHEMA ${catalog}.silver FROM analitica;
# MAGIC
# MAGIC REVOKE CREATE TABLE ON SCHEMA ${catalog}.gold FROM analitica;
# MAGIC REVOKE MODIFY ON SCHEMA ${catalog}.gold FROM analitica;
# MAGIC REVOKE SELECT ON SCHEMA ${catalog}.gold FROM analitica;
# MAGIC REVOKE USAGE ON SCHEMA ${catalog}.gold FROM analitica;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revocar Permisos en Catalogo

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE USAGE ON CATALOG ${catalog} FROM analitica;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revocar Permisos en External Locations

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-${ambiente}-bronze` FROM analitica;
# MAGIC REVOKE CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-${ambiente}-silver` FROM analitica;
# MAGIC REVOKE CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-${ambiente}-gold` FROM analitica;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificacion

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON CATALOG ${catalog} TO analitica;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA ${catalog}.bronze TO analitica;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA ${catalog}.silver TO analitica;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA ${catalog}.gold TO analitica;

# COMMAND ----------

print("Permisos revocados exitosamente")
