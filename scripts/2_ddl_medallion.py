# Databricks notebook source
# MAGIC %md
# MAGIC # DDL Medallion - La Campesinita
# MAGIC Crea catalogos, schemas y tablas

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
storage = f"adlcampesinita{ambiente}"
print(f"Catalogo: {catalog}")
print(f"Storage Account: {storage}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Catalogo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${catalog};

# COMMAND ----------

print(f"Catalogo creado: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog}.gold;

# COMMAND ----------

print("Schemas creados: bronze, silver, gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Tablas Bronze (PostgreSQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.ventas (
# MAGIC     venta_id BIGINT,
# MAGIC     fecha TIMESTAMP,
# MAGIC     cliente_id BIGINT,
# MAGIC     sucursal_id BIGINT,
# MAGIC     total DECIMAL(10,2),
# MAGIC     estado STRING,
# MAGIC     metodo_pago STRING,
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/postgres/ventas';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.detalle_ventas (
# MAGIC     detalle_id BIGINT,
# MAGIC     venta_id BIGINT,
# MAGIC     producto_id BIGINT,
# MAGIC     cantidad INT,
# MAGIC     precio_unitario DECIMAL(10,2),
# MAGIC     subtotal DECIMAL(10,2),
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/postgres/detalle_ventas';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.clientes (
# MAGIC     cliente_id BIGINT,
# MAGIC     nombre STRING,
# MAGIC     email STRING,
# MAGIC     telefono STRING,
# MAGIC     ciudad STRING,
# MAGIC     fecha_registro TIMESTAMP,
# MAGIC     nivel_credito STRING,
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/postgres/clientes';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.productos (
# MAGIC     producto_id BIGINT,
# MAGIC     nombre STRING,
# MAGIC     categoria STRING,
# MAGIC     precio DECIMAL(10,2),
# MAGIC     costo DECIMAL(10,2),
# MAGIC     proveedor_id BIGINT,
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/postgres/productos';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.sucursales (
# MAGIC     sucursal_id BIGINT,
# MAGIC     nombre STRING,
# MAGIC     ciudad STRING,
# MAGIC     region STRING,
# MAGIC     gerente STRING,
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/postgres/sucursales';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.inventario (
# MAGIC     inventario_id BIGINT,
# MAGIC     producto_id BIGINT,
# MAGIC     sucursal_id BIGINT,
# MAGIC     cantidad INT,
# MAGIC     fecha_actualizacion TIMESTAMP,
# MAGIC     fecha_caducidad DATE,
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/postgres/inventario';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.empleados (
# MAGIC     empleado_id BIGINT,
# MAGIC     nombre STRING,
# MAGIC     sucursal_id BIGINT,
# MAGIC     cargo STRING,
# MAGIC     salario DECIMAL(10,2),
# MAGIC     fecha_contratacion DATE,
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/postgres/empleados';

# COMMAND ----------

print("Tablas Bronze PostgreSQL creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Tablas Bronze (MySQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.proveedores (
# MAGIC     proveedor_id BIGINT,
# MAGIC     nombre STRING,
# MAGIC     contacto STRING,
# MAGIC     telefono STRING,
# MAGIC     email STRING,
# MAGIC     ciudad STRING,
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/mysql_erp/proveedores';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.ordenes_compra (
# MAGIC     orden_id BIGINT,
# MAGIC     proveedor_id BIGINT,
# MAGIC     fecha_orden TIMESTAMP,
# MAGIC     estado STRING,
# MAGIC     total DECIMAL(10,2),
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/mysql_erp/ordenes_compra';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.detalle_ordenes (
# MAGIC     detalle_orden_id BIGINT,
# MAGIC     orden_id BIGINT,
# MAGIC     producto_id BIGINT,
# MAGIC     cantidad INT,
# MAGIC     precio_unitario DECIMAL(10,2),
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/mysql_erp/detalle_ordenes';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.recepciones (
# MAGIC     recepcion_id BIGINT,
# MAGIC     orden_id BIGINT,
# MAGIC     fecha_recepcion TIMESTAMP,
# MAGIC     sucursal_id BIGINT,
# MAGIC     estado STRING,
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/mysql_erp/recepciones';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.bronze.productos_erp (
# MAGIC     producto_id BIGINT,
# MAGIC     codigo_barras STRING,
# MAGIC     nombre STRING,
# MAGIC     categoria STRING,
# MAGIC     unidad_medida STRING,
# MAGIC     _sync_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://bronze@${storage}.dfs.core.windows.net/mysql_erp/productos_erp';

# COMMAND ----------

print("Tablas Bronze MySQL creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN ${catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ${catalog}.bronze;

# COMMAND ----------

print("DDL completado exitosamente")
