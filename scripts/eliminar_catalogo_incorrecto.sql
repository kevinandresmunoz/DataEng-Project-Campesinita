-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Eliminar Catálogo Incorrecto
-- MAGIC Este script elimina el catálogo temporal creado por error: adb_campesinita_dev

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verificar catálogos existentes

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Eliminar catálogo incorrecto (si existe)

-- COMMAND ----------

-- Eliminar el catálogo incorrecto con CASCADE para eliminar todo su contenido
DROP CATALOG IF EXISTS adb_campesinita_dev CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verificar eliminación

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Resultado esperado
-- MAGIC
-- MAGIC Deberías ver solo los catálogos correctos:
-- MAGIC - adbslacampesinitadev (desarrollo)
-- MAGIC - adbslacampesinitaprod (producción)
-- MAGIC
-- MAGIC El catálogo adb_campesinita_dev ya no debe aparecer.
