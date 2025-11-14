# Databricks notebook source
# MAGIC %md
# MAGIC # Drop Medallion - La Campesinita
# MAGIC Elimina catalogos, schemas y tablas

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
print(f"Catalogo a eliminar: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS ${catalog}.gold CASCADE;
# MAGIC DROP SCHEMA IF EXISTS ${catalog}.silver CASCADE;
# MAGIC DROP SCHEMA IF EXISTS ${catalog}.bronze CASCADE;

# COMMAND ----------

print(f"Schemas eliminados del catalogo: {catalog}")
