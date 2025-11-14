# Databricks notebook source
# MAGIC %md
# MAGIC # Grants Medallion - La Campesinita
# MAGIC Configura permisos para usuarios y grupos

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
# MAGIC ## Crear Grupo Analitica

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE GROUP IF NOT EXISTS analitica;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER GROUP analitica ADD USER `kandres4488@hotmail.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permisos en Catalogo

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USAGE ON CATALOG ${catalog} TO analitica;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permisos en Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USAGE ON SCHEMA ${catalog}.bronze TO analitica;
# MAGIC GRANT SELECT ON SCHEMA ${catalog}.bronze TO analitica;
# MAGIC
# MAGIC GRANT USAGE ON SCHEMA ${catalog}.silver TO analitica;
# MAGIC GRANT SELECT ON SCHEMA ${catalog}.silver TO analitica;
# MAGIC GRANT MODIFY ON SCHEMA ${catalog}.silver TO analitica;
# MAGIC GRANT CREATE TABLE ON SCHEMA ${catalog}.silver TO analitica;
# MAGIC
# MAGIC GRANT USAGE ON SCHEMA ${catalog}.gold TO analitica;
# MAGIC GRANT SELECT ON SCHEMA ${catalog}.gold TO analitica;
# MAGIC GRANT MODIFY ON SCHEMA ${catalog}.gold TO analitica;
# MAGIC GRANT CREATE TABLE ON SCHEMA ${catalog}.gold TO analitica;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permisos en External Locations

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-${ambiente}-bronze` TO analitica;
# MAGIC GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-${ambiente}-silver` TO analitica;
# MAGIC GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-${ambiente}-gold` TO analitica;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permisos Usuario Normal (solo DEV)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USAGE ON CATALOG ${catalog} TO `well2chat@outlook.com`;
# MAGIC GRANT CREATE SCHEMA ON CATALOG ${catalog} TO `well2chat@outlook.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.bronze TO `well2chat@outlook.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.silver TO `well2chat@outlook.com`;
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA ${catalog}.gold TO `well2chat@outlook.com`;
# MAGIC GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-${ambiente}-bronze` TO `well2chat@outlook.com`;
# MAGIC GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-${ambiente}-silver` TO `well2chat@outlook.com`;
# MAGIC GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-${ambiente}-gold` TO `well2chat@outlook.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON CATALOG ${catalog};

# COMMAND ----------

print("Permisos configurados exitosamente")
