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
# MAGIC ## Crear Tablas Silver (EXTERNAL)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.silver.clientes_clean (
# MAGIC     cliente_id BIGINT,
# MAGIC     nombre STRING,
# MAGIC     email STRING,
# MAGIC     telefono STRING,
# MAGIC     ciudad STRING,
# MAGIC     fecha_registro TIMESTAMP,
# MAGIC     nivel_credito STRING,
# MAGIC     antiguedad_dias INT,
# MAGIC     segmento_antiguedad STRING,
# MAGIC     nivel_credito_num INT,
# MAGIC     _transform_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://silver@${storage}.dfs.core.windows.net/clientes_clean';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.silver.productos_clean (
# MAGIC     producto_id BIGINT,
# MAGIC     nombre STRING,
# MAGIC     categoria STRING,
# MAGIC     precio DECIMAL(10,2),
# MAGIC     costo DECIMAL(10,2),
# MAGIC     proveedor_id BIGINT,
# MAGIC     margen DECIMAL(10,2),
# MAGIC     categoria_grupo STRING,
# MAGIC     _transform_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://silver@${storage}.dfs.core.windows.net/productos_clean';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.silver.proveedores_clean (
# MAGIC     proveedor_id BIGINT,
# MAGIC     nombre STRING,
# MAGIC     contacto STRING,
# MAGIC     telefono STRING,
# MAGIC     email STRING,
# MAGIC     ciudad STRING,
# MAGIC     _transform_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://silver@${storage}.dfs.core.windows.net/proveedores_clean';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.silver.inventario_clean (
# MAGIC     inventario_id BIGINT,
# MAGIC     producto_id BIGINT,
# MAGIC     sucursal_id BIGINT,
# MAGIC     cantidad INT,
# MAGIC     fecha_actualizacion TIMESTAMP,
# MAGIC     fecha_caducidad DATE,
# MAGIC     dias_hasta_caducidad INT,
# MAGIC     estado_caducidad STRING,
# MAGIC     alerta_stock STRING,
# MAGIC     _transform_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (sucursal_id)
# MAGIC LOCATION 'abfss://silver@${storage}.dfs.core.windows.net/inventario_clean';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.silver.ventas_clean (
# MAGIC     venta_id BIGINT,
# MAGIC     fecha TIMESTAMP,
# MAGIC     cliente_id BIGINT,
# MAGIC     sucursal_id BIGINT,
# MAGIC     total DECIMAL(10,2),
# MAGIC     estado STRING,
# MAGIC     metodo_pago STRING,
# MAGIC     fecha_date DATE,
# MAGIC     hora INT,
# MAGIC     dia_semana INT,
# MAGIC     es_fin_semana BOOLEAN,
# MAGIC     periodo_dia STRING,
# MAGIC     _transform_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (fecha_date)
# MAGIC LOCATION 'abfss://silver@${storage}.dfs.core.windows.net/ventas_clean';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.silver.detalle_ventas_clean (
# MAGIC     detalle_id BIGINT,
# MAGIC     venta_id BIGINT,
# MAGIC     producto_id BIGINT,
# MAGIC     cantidad INT,
# MAGIC     precio_unitario DECIMAL(10,2),
# MAGIC     subtotal DECIMAL(10,2),
# MAGIC     _transform_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://silver@${storage}.dfs.core.windows.net/detalle_ventas_clean';

# COMMAND ----------

print("Tablas Silver creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Tablas Gold - Dimensiones (EXTERNAL)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_tiempo (
# MAGIC     fecha_key DATE,
# MAGIC     anio INT,
# MAGIC     mes INT,
# MAGIC     dia INT,
# MAGIC     dia_semana INT,
# MAGIC     semana_anio INT,
# MAGIC     trimestre INT,
# MAGIC     mes_nombre STRING,
# MAGIC     dia_semana_nombre STRING,
# MAGIC     tipo_dia STRING,
# MAGIC     temporada STRING
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/dim_tiempo';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_clientes (
# MAGIC     cliente_key BIGINT,
# MAGIC     nombre STRING,
# MAGIC     email STRING,
# MAGIC     telefono STRING,
# MAGIC     ciudad STRING,
# MAGIC     fecha_registro TIMESTAMP,
# MAGIC     nivel_credito STRING,
# MAGIC     antiguedad_dias INT,
# MAGIC     segmento_antiguedad STRING,
# MAGIC     nivel_credito_num INT,
# MAGIC     fecha_actualizacion TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/dim_clientes';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_productos (
# MAGIC     producto_key BIGINT,
# MAGIC     producto_nombre STRING,
# MAGIC     categoria STRING,
# MAGIC     precio DECIMAL(10,2),
# MAGIC     costo DECIMAL(10,2),
# MAGIC     proveedor_id BIGINT,
# MAGIC     margen DECIMAL(10,2),
# MAGIC     categoria_grupo STRING,
# MAGIC     fecha_actualizacion TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/dim_productos';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_sucursales (
# MAGIC     sucursal_key BIGINT,
# MAGIC     sucursal_nombre STRING,
# MAGIC     ciudad STRING,
# MAGIC     region STRING,
# MAGIC     gerente STRING,
# MAGIC     fecha_actualizacion TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/dim_sucursales';

# COMMAND ----------

print("Dimensiones Gold creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Tablas Gold - Hechos (EXTERNAL)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_ventas (
# MAGIC     venta_id BIGINT,
# MAGIC     fecha_key DATE,
# MAGIC     cliente_key BIGINT,
# MAGIC     sucursal_key BIGINT,
# MAGIC     total DECIMAL(10,2),
# MAGIC     metodo_pago STRING,
# MAGIC     hora INT,
# MAGIC     dia_semana INT,
# MAGIC     es_fin_semana BOOLEAN,
# MAGIC     periodo_dia STRING,
# MAGIC     cantidad_total_items INT,
# MAGIC     num_items_distintos INT,
# MAGIC     margen_total DECIMAL(10,2),
# MAGIC     ticket_promedio_item DECIMAL(10,2),
# MAGIC     fecha_carga TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (fecha_key)
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/fact_ventas';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_kpis_diarios (
# MAGIC     fecha_key DATE,
# MAGIC     sucursal_key BIGINT,
# MAGIC     ventas_totales DECIMAL(10,2),
# MAGIC     numero_transacciones BIGINT,
# MAGIC     ticket_promedio DECIMAL(10,2),
# MAGIC     margen_bruto_total DECIMAL(10,2),
# MAGIC     clientes_unicos BIGINT,
# MAGIC     items_vendidos BIGINT,
# MAGIC     ventas_fin_semana BIGINT,
# MAGIC     ventas_manana DECIMAL(10,2),
# MAGIC     ventas_tarde DECIMAL(10,2),
# MAGIC     ventas_noche DECIMAL(10,2)
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (fecha_key)
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/fact_kpis_diarios';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_analisis_clientes (
# MAGIC     cliente_key BIGINT,
# MAGIC     ultima_compra DATE,
# MAGIC     frecuencia_compras BIGINT,
# MAGIC     valor_total_compras DECIMAL(10,2),
# MAGIC     ticket_promedio_cliente DECIMAL(10,2),
# MAGIC     items_totales BIGINT,
# MAGIC     sucursales_visitadas BIGINT,
# MAGIC     recency_dias INT,
# MAGIC     segmento_rfm STRING
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/fact_analisis_clientes';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_rendimiento_productos (
# MAGIC     producto_id BIGINT,
# MAGIC     unidades_vendidas BIGINT,
# MAGIC     ingresos_totales DECIMAL(10,2),
# MAGIC     margen_total DECIMAL(10,2),
# MAGIC     numero_ventas BIGINT,
# MAGIC     transacciones_unicas BIGINT
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/fact_rendimiento_productos';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_inventario (
# MAGIC     inventario_id BIGINT,
# MAGIC     producto_key BIGINT,
# MAGIC     sucursal_key BIGINT,
# MAGIC     fecha_key DATE,
# MAGIC     cantidad INT,
# MAGIC     valor_inventario DECIMAL(10,2),
# MAGIC     valor_venta_potencial DECIMAL(10,2),
# MAGIC     fecha_actualizacion TIMESTAMP,
# MAGIC     fecha_caducidad DATE,
# MAGIC     dias_hasta_caducidad INT,
# MAGIC     estado_caducidad STRING,
# MAGIC     alerta_stock STRING,
# MAGIC     requiere_accion BOOLEAN,
# MAGIC     fecha_carga TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (sucursal_key)
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/fact_inventario';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_kpis_inventario (
# MAGIC     sucursal_key BIGINT,
# MAGIC     stock_total BIGINT,
# MAGIC     valor_total_inventario DECIMAL(10,2),
# MAGIC     valor_venta_potencial_total DECIMAL(10,2),
# MAGIC     items_unicos BIGINT,
# MAGIC     items_criticos BIGINT,
# MAGIC     items_alerta BIGINT,
# MAGIC     items_sin_stock BIGINT,
# MAGIC     items_stock_bajo BIGINT,
# MAGIC     dias_promedio_caducidad DOUBLE
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/fact_kpis_inventario';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_alertas_inventario (
# MAGIC     inventario_id BIGINT,
# MAGIC     producto_key BIGINT,
# MAGIC     sucursal_key BIGINT,
# MAGIC     cantidad INT,
# MAGIC     estado_caducidad STRING,
# MAGIC     dias_hasta_caducidad INT,
# MAGIC     alerta_stock STRING,
# MAGIC     valor_inventario DECIMAL(10,2),
# MAGIC     prioridad STRING,
# MAGIC     accion_recomendada STRING,
# MAGIC     fecha_alerta TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@${storage}.dfs.core.windows.net/fact_alertas_inventario';

# COMMAND ----------

print("Hechos Gold creados")

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

# MAGIC %sql
# MAGIC SHOW TABLES IN ${catalog}.silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ${catalog}.gold;

# COMMAND ----------

print("DDL completado exitosamente")
