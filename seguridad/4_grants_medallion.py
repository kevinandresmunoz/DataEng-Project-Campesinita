# Databricks notebook source
# MAGIC %md
# MAGIC # Grants Medallion - La Campesinita
# MAGIC Configura permisos para usuarios y grupos

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
# MAGIC ## Crear Grupo Analitica

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE GROUP IF NOT EXISTS analitica;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregar Usuarios al Grupo
# MAGIC 
# MAGIC Los usuarios deben estar previamente habilitados en Azure Active Directory
# MAGIC y sincronizados con el Databricks Workspace.
# MAGIC 
# MAGIC Gestion de usuarios:
# MAGIC 1. Habilitar usuario en Azure Active Directory
# MAGIC 2. Sincronizar con Databricks (automatico o manual desde Admin Console)
# MAGIC 3. Agregar usuario al grupo analitica mediante Admin Console o SQL
# MAGIC 
# MAGIC Ejemplo SQL para agregar usuario:
# MAGIC ALTER GROUP analitica ADD USER `usuario@dominio.com`;
# MAGIC 
# MAGIC Los permisos se asignan al grupo, no a usuarios individuales.

# COMMAND ----------

# Descomentar y actualizar con el email del usuario a agregar
# spark.sql("ALTER GROUP analitica ADD USER `usuario@dominio.com`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permisos en Catalogo

# COMMAND ----------

spark.sql(f"GRANT USAGE ON CATALOG {catalog} TO analitica")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permisos en Schemas

# COMMAND ----------

spark.sql(f"GRANT USAGE ON SCHEMA {catalog}.bronze TO analitica")
spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.bronze TO analitica")

spark.sql(f"GRANT USAGE ON SCHEMA {catalog}.silver TO analitica")
spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.silver TO analitica")
spark.sql(f"GRANT MODIFY ON SCHEMA {catalog}.silver TO analitica")
spark.sql(f"GRANT CREATE TABLE ON SCHEMA {catalog}.silver TO analitica")

spark.sql(f"GRANT USAGE ON SCHEMA {catalog}.gold TO analitica")
spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.gold TO analitica")
spark.sql(f"GRANT MODIFY ON SCHEMA {catalog}.gold TO analitica")
spark.sql(f"GRANT CREATE TABLE ON SCHEMA {catalog}.gold TO analitica")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permisos en External Locations

# COMMAND ----------

spark.sql(f"GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-{ambiente}-bronze` TO analitica")
spark.sql(f"GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-{ambiente}-silver` TO analitica")
spark.sql(f"GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION `extl-campesinita-{ambiente}-gold` TO analitica")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar

# COMMAND ----------

spark.sql(f"SHOW GRANTS ON CATALOG {catalog}").show()

# COMMAND ----------

print("Permisos configurados exitosamente")
