# Databricks notebook source
# MAGIC %md
# MAGIC # DDL Medallion - La Campesinita
# MAGIC Crea catalogos, schemas y tablas

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametros

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("storage", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables

# COMMAND ----------

# Obtener parametros
catalog_input = dbutils.widgets.get("catalog")
storage_input = dbutils.widgets.get("storage")

# Detectar o crear catalogo correcto
if catalog_input:
    # Si se proporciona catalogo, usarlo
    catalog = catalog_input
    print(f"Catalogo proporcionado: {catalog}")
else:
    # Buscar catalogo que contenga 'campesinita'
    print("Buscando catalogo correcto...")
    catalogs = spark.sql("SHOW CATALOGS").collect()
    catalog = None
    
    for row in catalogs:
        cat_name = row[0]
        if "campesinita" in cat_name.lower():
            catalog = cat_name
            print(f"Catalogo encontrado: {catalog}")
            break
    
    # Si no encuentra ninguno, crear cata_campesinita_dev por defecto
    if not catalog:
        catalog = "cata_campesinita_dev"
        print(f"No se encontro catalogo, se creara: {catalog}")

# Inferir storage del catalogo
if storage_input:
    storage = storage_input
    print(f"Storage proporcionado: {storage}")
else:
    if "dev" in catalog.lower():
        storage = "adlcampesinitadev"
    elif "prod" in catalog.lower():
        storage = "adlcampesinitaprod"
    else:
        storage = "adlcampesinitadev"
    print(f"Storage inferido: {storage}")

print(f"\nCatalogo: {catalog}")
print(f"Storage: {storage}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Catalogo

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
print(f"Catalogo creado: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Schemas

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")
print("Schemas creados: bronze, silver, gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Tablas Bronze (PostgreSQL)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.ventas (
    id INT,
    folio STRING,
    fecha_hora TIMESTAMP,
    cliente_id INT,
    empleado_id INT,
    sucursal_id INT,
    subtotal DECIMAL(12,2),
    impuestos DECIMAL(12,2),
    descuento DECIMAL(12,2),
    total DECIMAL(12,2),
    metodo_pago STRING,
    estatus STRING,
    num_productos INT,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/postgres/ventas'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.detalle_ventas (
    id INT,
    venta_id INT,
    producto_id INT,
    cantidad DECIMAL(10,3),
    precio_unitario DECIMAL(10,2),
    subtotal DECIMAL(12,2),
    costo_unitario DECIMAL(10,2),
    margen_item DECIMAL(12,2),
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/postgres/detalle_ventas'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.clientes (
    id INT,
    codigo_cliente STRING,
    nombre STRING,
    apellidos STRING,
    telefono STRING,
    email STRING,
    direccion STRING,
    ciudad STRING,
    fecha_registro DATE,
    tipo_cliente STRING,
    credito_limite DECIMAL(12,2),
    puntos_acumulados INT,
    activo BOOLEAN,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/postgres/clientes'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.productos (
    id INT,
    codigo_producto STRING,
    nombre STRING,
    categoria_id INT,
    precio_compra DECIMAL(10,2),
    precio_venta DECIMAL(10,2),
    unidad_medida_id INT,
    stock_minimo INT,
    perecedero BOOLEAN,
    activo BOOLEAN,
    fecha_creacion DATE,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/postgres/productos'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.sucursales (
    id INT,
    nombre STRING,
    direccion STRING,
    telefono STRING,
    ciudad STRING,
    region STRING,
    gerente STRING,
    fecha_apertura DATE,
    area_metros DECIMAL(10,2),
    activa BOOLEAN,
    tipo_zona STRING,
    nivel_poblacion STRING,
    tipo_ciudad STRING,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/postgres/sucursales'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.inventario (
    id INT,
    producto_id INT,
    sucursal_id INT,
    cantidad_actual INT,
    fecha_ultimo_conteo DATE,
    ubicacion STRING,
    lote STRING,
    fecha_caducidad DATE,
    costo_promedio DECIMAL(10,2),
    activo BOOLEAN,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/postgres/inventario'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.empleados (
    id INT,
    codigo_empleado STRING,
    nombre STRING,
    apellidos STRING,
    cargo_id INT,
    departamento_id INT,
    puede_vender BOOLEAN,
    salario DECIMAL(10,2),
    fecha_ingreso DATE,
    sucursal_id INT,
    activo BOOLEAN,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/postgres/empleados'
""")

print("Tablas Bronze PostgreSQL creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Tablas Bronze (MySQL)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.proveedores (
    id INT,
    codigo_proveedor STRING,
    nombre_empresa STRING,
    nit STRING,
    contacto_principal STRING,
    cargo_contacto STRING,
    telefono_principal STRING,
    telefono_secundario STRING,
    email_principal STRING,
    email_secundario STRING,
    direccion STRING,
    ciudad STRING,
    pais STRING,
    pagina_web STRING,
    tipo_productos STRING,
    categoria_especialidad STRING,
    calificacion DECIMAL(3,2),
    estado STRING,
    fecha_registro DATE,
    fecha_ultima_evaluacion DATE,
    plazo_pago_dias INT,
    descuento_volumen DECIMAL(5,4),
    limite_credito DECIMAL(15,2),
    moneda STRING,
    observaciones STRING,
    activo BOOLEAN,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/mysql_erp/proveedores'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.ordenes_compra (
    id INT,
    numero_orden STRING,
    fecha_orden DATE,
    proveedor_id INT,
    estado STRING,
    subtotal DECIMAL(15,2),
    descuento DECIMAL(15,2),
    impuestos DECIMAL(15,2),
    total DECIMAL(15,2),
    moneda STRING,
    fecha_entrega_esperada DATE,
    fecha_entrega_real DATE,
    direccion_entrega STRING,
    sucursal_destino_id INT,
    terminos_pago STRING,
    observaciones STRING,
    usuario_creacion STRING,
    fecha_creacion DATE,
    usuario_aprobacion STRING,
    fecha_aprobacion DATE,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/mysql_erp/ordenes_compra'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.movimientos_inventario (
    id INT,
    numero_movimiento STRING,
    fecha_movimiento TIMESTAMP,
    tipo_movimiento STRING,
    producto_id INT,
    sucursal_id INT,
    cantidad INT,
    costo_unitario DECIMAL(10,2),
    costo_total DECIMAL(12,2),
    lote STRING,
    fecha_vencimiento DATE,
    documento_referencia STRING,
    observaciones STRING,
    usuario STRING,
    fecha_registro TIMESTAMP,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/mysql_erp/movimientos_inventario'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.recepciones (
    id INT,
    numero_recepcion STRING,
    fecha_recepcion DATE,
    orden_compra_id INT,
    numero_orden_referencia STRING,
    proveedor_id INT,
    sucursal_id INT,
    estado STRING,
    porcentaje_recibido DECIMAL(5,2),
    valor_esperado DECIMAL(15,2),
    valor_recibido DECIMAL(15,2),
    numero_guia_transportadora STRING,
    transportadora STRING,
    responsable_recepcion STRING,
    observaciones STRING,
    danos_reportados STRING,
    motivo_rechazo STRING,
    requiere_seguimiento BOOLEAN,
    fecha_registro DATE,
    usuario_registro STRING,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/mysql_erp/recepciones'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.productos_erp (
    id INT,
    codigo_producto STRING,
    codigo_barras STRING,
    nombre STRING,
    descripcion STRING,
    categoria_id INT,
    subcategoria STRING,
    marca STRING,
    modelo STRING,
    proveedor_id INT,
    precio_compra DECIMAL(10,2),
    precio_venta DECIMAL(10,2),
    precio_mayorista DECIMAL(10,2),
    unidad_medida_id INT,
    peso_gramos INT,
    dimensiones STRING,
    stock_minimo INT,
    stock_maximo INT,
    punto_reorden INT,
    perecedero BOOLEAN,
    dias_caducidad INT,
    requiere_refrigeracion BOOLEAN,
    temperatura_almacenamiento STRING,
    estado STRING,
    activo BOOLEAN,
    fecha_creacion DATE,
    fecha_ultima_actualizacion DATE,
    impuesto_iva DECIMAL(4,2),
    observaciones STRING,
    _sync_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://bronze@{storage}.dfs.core.windows.net/mysql_erp/productos_erp'
""")

print("Tablas Bronze MySQL creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Tablas Silver (EXTERNAL)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.clientes_clean (
    cliente_id BIGINT,
    nombre STRING,
    email STRING,
    telefono STRING,
    ciudad STRING,
    fecha_registro TIMESTAMP,
    nivel_credito STRING,
    antiguedad_dias INT,
    segmento_antiguedad STRING,
    nivel_credito_num INT,
    _transform_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/clientes_clean'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.productos_clean (
    producto_id BIGINT,
    nombre STRING,
    categoria STRING,
    precio DECIMAL(10,2),
    costo DECIMAL(10,2),
    proveedor_id BIGINT,
    margen DECIMAL(10,2),
    categoria_grupo STRING,
    _transform_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/productos_clean'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.proveedores_clean (
    proveedor_id BIGINT,
    nombre STRING,
    contacto STRING,
    telefono STRING,
    email STRING,
    ciudad STRING,
    _transform_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/proveedores_clean'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.inventario_clean (
    inventario_id BIGINT,
    producto_id BIGINT,
    sucursal_id BIGINT,
    cantidad INT,
    fecha_actualizacion TIMESTAMP,
    fecha_caducidad DATE,
    dias_hasta_caducidad INT,
    estado_caducidad STRING,
    alerta_stock STRING,
    _transform_timestamp TIMESTAMP
) USING DELTA
PARTITIONED BY (sucursal_id)
LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/inventario_clean'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.ventas_clean (
    venta_id BIGINT,
    fecha TIMESTAMP,
    cliente_id BIGINT,
    sucursal_id BIGINT,
    total DECIMAL(10,2),
    estado STRING,
    metodo_pago STRING,
    fecha_date DATE,
    hora INT,
    dia_semana INT,
    es_fin_semana BOOLEAN,
    periodo_dia STRING,
    _transform_timestamp TIMESTAMP
) USING DELTA
PARTITIONED BY (fecha_date)
LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/ventas_clean'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.detalle_ventas_clean (
    detalle_id BIGINT,
    venta_id BIGINT,
    producto_id BIGINT,
    cantidad INT,
    precio_unitario DECIMAL(10,2),
    subtotal DECIMAL(10,2),
    _transform_timestamp TIMESTAMP
) USING DELTA
LOCATION 'abfss://silver@{storage}.dfs.core.windows.net/detalle_ventas_clean'
""")

print("Tablas Silver creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Tablas Gold - Dimensiones (EXTERNAL)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_tiempo (
    fecha_key DATE,
    anio INT,
    mes INT,
    dia INT,
    dia_semana INT,
    semana_anio INT,
    trimestre INT,
    mes_nombre STRING,
    dia_semana_nombre STRING,
    tipo_dia STRING,
    temporada STRING
) USING DELTA
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/dim_tiempo'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_clientes (
    cliente_key BIGINT,
    nombre STRING,
    email STRING,
    telefono STRING,
    ciudad STRING,
    fecha_registro TIMESTAMP,
    nivel_credito STRING,
    antiguedad_dias INT,
    segmento_antiguedad STRING,
    nivel_credito_num INT,
    fecha_actualizacion TIMESTAMP
) USING DELTA
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/dim_clientes'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_productos (
    producto_key BIGINT,
    producto_nombre STRING,
    categoria STRING,
    precio DECIMAL(10,2),
    costo DECIMAL(10,2),
    proveedor_id BIGINT,
    margen DECIMAL(10,2),
    categoria_grupo STRING,
    fecha_actualizacion TIMESTAMP
) USING DELTA
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/dim_productos'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.dim_sucursales (
    sucursal_key BIGINT,
    sucursal_nombre STRING,
    ciudad STRING,
    region STRING,
    gerente STRING,
    fecha_actualizacion TIMESTAMP
) USING DELTA
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/dim_sucursales'
""")

print("Dimensiones Gold creadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Tablas Gold - Hechos (EXTERNAL)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_ventas (
    venta_id BIGINT,
    fecha_key DATE,
    cliente_key BIGINT,
    sucursal_key BIGINT,
    total DECIMAL(10,2),
    metodo_pago STRING,
    hora INT,
    dia_semana INT,
    es_fin_semana BOOLEAN,
    periodo_dia STRING,
    cantidad_total_items INT,
    num_items_distintos INT,
    margen_total DECIMAL(10,2),
    ticket_promedio_item DECIMAL(10,2),
    fecha_carga TIMESTAMP
) USING DELTA
PARTITIONED BY (fecha_key)
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/fact_ventas'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_kpis_diarios (
    fecha_key DATE,
    sucursal_key BIGINT,
    ventas_totales DECIMAL(10,2),
    numero_transacciones BIGINT,
    ticket_promedio DECIMAL(10,2),
    margen_bruto_total DECIMAL(10,2),
    clientes_unicos BIGINT,
    items_vendidos BIGINT,
    ventas_fin_semana BIGINT,
    ventas_manana DECIMAL(10,2),
    ventas_tarde DECIMAL(10,2),
    ventas_noche DECIMAL(10,2)
) USING DELTA
PARTITIONED BY (fecha_key)
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/fact_kpis_diarios'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_analisis_clientes (
    cliente_key BIGINT,
    ultima_compra DATE,
    frecuencia_compras BIGINT,
    valor_total_compras DECIMAL(10,2),
    ticket_promedio_cliente DECIMAL(10,2),
    items_totales BIGINT,
    sucursales_visitadas BIGINT,
    recency_dias INT,
    segmento_rfm STRING
) USING DELTA
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/fact_analisis_clientes'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_rendimiento_productos (
    producto_id BIGINT,
    unidades_vendidas BIGINT,
    ingresos_totales DECIMAL(18,2),
    margen_total DECIMAL(18,2),
    numero_ventas BIGINT,
    transacciones_unicas BIGINT
) USING DELTA
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/fact_rendimiento_productos'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_inventario (
    inventario_id BIGINT,
    producto_key BIGINT,
    sucursal_key BIGINT,
    fecha_key DATE,
    cantidad INT,
    valor_inventario DECIMAL(15,2),
    valor_venta_potencial DECIMAL(15,2),
    fecha_actualizacion TIMESTAMP,
    fecha_caducidad DATE,
    dias_hasta_caducidad INT,
    estado_caducidad STRING,
    alerta_stock STRING,
    requiere_accion BOOLEAN,
    fecha_carga TIMESTAMP
) USING DELTA
PARTITIONED BY (sucursal_key)
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/fact_inventario'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_kpis_inventario (
    sucursal_key BIGINT,
    stock_total BIGINT,
    valor_total_inventario DECIMAL(18,2),
    valor_venta_potencial_total DECIMAL(18,2),
    items_unicos BIGINT,
    items_criticos BIGINT,
    items_alerta BIGINT,
    items_sin_stock BIGINT,
    items_stock_bajo BIGINT,
    dias_promedio_caducidad DOUBLE
) USING DELTA
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/fact_kpis_inventario'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.fact_alertas_inventario (
    inventario_id BIGINT,
    producto_key BIGINT,
    sucursal_key BIGINT,
    cantidad INT,
    estado_caducidad STRING,
    dias_hasta_caducidad INT,
    alerta_stock STRING,
    valor_inventario DECIMAL(15,2),
    prioridad STRING,
    accion_recomendada STRING,
    fecha_alerta TIMESTAMP
) USING DELTA
LOCATION 'abfss://gold@{storage}.dfs.core.windows.net/fact_alertas_inventario'
""")

print("Hechos Gold creados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar

# COMMAND ----------

spark.sql(f"SHOW SCHEMAS IN {catalog}").show()

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {catalog}.bronze").show()

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {catalog}.silver").show()

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {catalog}.gold").show()

# COMMAND ----------

print("DDL completado exitosamente")
