# Descripcion del Proceso ETL

Documentacion tecnica del proceso de Extraccion, Transformacion y Carga implementado en Databricks con arquitectura Medallion (Bronze → Silver → Gold).

---

## Descripcion General

El proceso ETL transforma datos desde Bronze Layer (datos crudos sincronizados desde bases de datos operacionales) hasta Gold Layer (modelo dimensional optimizado para analisis de negocio).

**Caracteristicas principales:**
- Ejecucion paralela de notebooks Bronze to Silver con ThreadPoolExecutor
- Transformaciones con PySpark para procesamiento distribuido
- Formato Delta Lake con versionamiento y time travel
- Optimizaciones con particionado, Z-ORDER y OPTIMIZE
- Validaciones de calidad de datos en cada capa

---

## Estructura de Notebooks

```
proceso/
├── Include/
│   └── utils.py                        # Funciones compartidas
├── ingestion_to_bronze/                # Sincronizacion BD → Bronze
│   ├── sync_postgres_to_bronze.py
│   └── sync_mysql_to_bronze.py
├── bronze_to_silver/                   # Limpieza y validacion
│   ├── clientes_segmentados.py
│   ├── inventario_productos.py
│   └── ventas_consolidadas.py
├── silver_to_gold/                     # Modelo dimensional
│   ├── modelo_dimensional.py
│   ├── metricas_ventas.py
│   └── metricas_inventario.py
└── validate/                           # Scripts de validacion
    ├── setup_databricks.py             # Configuracion inicial
    ├── test_conexiones.py              # Prueba de conectividad
    └── verificar_configuracion.py      # Validacion de setup
```

---

## Capas de Datos

### Bronze Layer
Datos crudos sincronizados desde PostgreSQL (7 tablas) y MySQL (5 tablas). Formato Parquet comprimido con particionado por fecha. Sincronizacion automatica 2 veces al dia (1 PM y 9 PM hora Colombia) via Azure Data Factory o manual ejecutando notebooks de ingestion durante setup inicial.

### Silver Layer
Datos limpios y validados con 6 tablas: clientes_clean, productos_clean, inventario_clean, proveedores_clean, ventas_clean, detalle_ventas_clean. Incluye deduplicacion, normalizacion, validaciones de calidad y enriquecimiento con metricas calculadas.

### Gold Layer
Modelo dimensional con 4 dimensiones (tiempo, clientes, productos, sucursales) y 7 tablas de hechos (ventas, inventario, KPIs). Optimizado con particionado y Z-ORDER para consultas analiticas.

---

## Notebooks de Procesamiento

### Include/utils.py
Funciones compartidas: log, get_storage_account, read_bronze, write_silver, write_gold, optimize_table, create_database_if_not_exists. Incluye validaciones y manejo de excepciones.

### bronze_to_silver/

**ventas_consolidadas.py**
Limpia y valida ventas con detalles. Filtra ventas completadas, valida consistencia matematica, enriquece con dimensiones temporales y calcula metricas. Salida: ventas_clean, detalle_ventas_clean.

**clientes_segmentados.py**
Segmenta clientes con logica RFM. Normaliza datos, calcula antiguedad, clasifica por nivel credito y asigna segmento de negocio. Salida: clientes_clean.

**inventario_productos.py**
Consolida productos de PostgreSQL y MySQL con deduplicacion. Calcula margenes, clasifica por categoria y procesa inventario con alertas de caducidad. Salida: productos_clean, inventario_clean, proveedores_clean.

### silver_to_gold/

**modelo_dimensional.py**
Construye las 4 dimensiones del modelo. dim_tiempo se genera desde codigo, dim_clientes y dim_productos se transforman desde Silver, dim_sucursales se lee desde Bronze. Aplica OPTIMIZE y Z-ORDER. Debe ejecutarse primero en fase Gold.

**metricas_ventas.py**
Calcula 4 tablas de hechos de ventas: fact_ventas (transaccional), fact_kpis_diarios (agregados), fact_analisis_clientes (RFM), fact_rendimiento_productos (performance). Aplica optimizaciones.

**metricas_inventario.py**
Calcula 3 tablas de hechos de inventario: fact_inventario (stock actual), fact_kpis_inventario (metricas por sucursal), fact_alertas_inventario (items criticos). Aplica optimizaciones.

---

## Ejecucion del Pipeline

### Setup Inicial (Primera Vez)

**Prerequisito: Vincular Repositorio en Databricks**

1. Databricks → Repos → Add Repo
2. Git repository URL: URL de tu repositorio GitHub
3. Repository name: project_campesinita
4. Create Repo

Los notebooks quedaran en: `/Repos/[tu_usuario]/project_campesinita/project/`

**Ejecutar desde Databricks en orden:**

1. `scripts/1_drop_medallion.py` - Widget ambiente: dev
2. `scripts/2_ddl_medallion.py` - Widget ambiente: dev
3. `scripts/3_populate_data.py` - Ejecuta pipeline completo
4. `seguridad/4_grants_medallion.py` - Widget ambiente: dev

**Cuando ejecutar:**
- Deploy inicial a dev o prod
- Recreacion completa de infraestructura

### Operacion Automatica (Continua)

Una vez completado el setup, el sistema opera automaticamente:

**1. Azure Data Factory (1 PM y 9 PM hora Colombia):**
- Ejecuta notebooks de ingestion: `sync_postgres_to_bronze` y `sync_mysql_to_bronze`
- Sincroniza datos desde PostgreSQL y MySQL a Bronze Layer
- Al completar exitosamente, dispara Databricks Jobs

**2. Databricks Jobs (automatico):**
- Job: `Processing_Bronze_to_Gold`
- Ejecuta 6 tasks con dependencias desde Repos
- Tasks 1-3 (paralelo): Bronze → Silver
- Task 4 (secuencial): Silver → Gold (dimensiones)
- Tasks 5-6 (paralelo): Silver → Gold (metricas)

### Ejecucion Manual (Desarrollo/Testing)

Para pruebas o desarrollo, ejecutar notebooks desde Repos en orden:
1. Ingestion: `/Repos/[tu_usuario]/project_campesinita/project/proceso/ingestion_to_bronze/sync_postgres_to_bronze` y `sync_mysql_to_bronze`
2. Bronze to Silver: `clientes_segmentados`, `inventario_productos`, `ventas_consolidadas`
3. Silver to Gold: `modelo_dimensional`, `metricas_ventas`, `metricas_inventario`

---

## Validacion y Optimizaciones

### Verificacion de Tablas
Ejecutar queries SQL para verificar creacion de tablas y conteo de registros en silver y gold. Validar calidad de datos consultando ventas por dia y KPIs por sucursal.

### Optimizaciones Aplicadas
- **Particionado**: Silver por dimensiones clave, Gold por fecha_key
- **Z-ORDER**: Co-localiza datos relacionados para mejorar performance de filtros y joins
- **OPTIMIZE**: Compacta archivos pequenos despues de cada escritura
- **Delta Lake**: Versionamiento, transacciones ACID, schema evolution

---

## Metricas de Performance

- **Tiempo de ejecucion**: Bronze to Silver 10-15 min (paralelo), Silver to Gold 15-20 min (secuencial), pipeline completo 25-35 min
- **Volumetria**: Bronze 1.85 GB, Silver 1.2 GB, Gold 800 MB
- **Recursos recomendados**: Cluster Standard_DS3_v2 o superior, 2-4 workers, Spark 3.4+, DBR 13.3 LTS

---

## Arquitectura de Tablas

### Todas las Tablas son EXTERNAL

**Tablas External:**
- Todas las tablas (Bronze, Silver, Gold) se crean como EXTERNAL
- Definidas con LOCATION explícita en Azure Storage
- DDL (2_ddl_medallion.py) crea todas las tablas con schema completo

**Ventajas:**
- DROP elimina solo metadatos, datos persisten en Azure Storage
- Protección contra eliminación accidental
- Recuperación ante errores de catálogo
- Auditoría y compliance (datos siempre disponibles)
- Disaster recovery simplificado

### Estrategias de Escritura

**Bronze Layer (Ingestion):**
- Método: TRUNCATE + INSERT INTO
- Razón: Tablas ya creadas por DDL, solo insertar datos desde BD
- Flujo: Lee BD → Crea vista temporal → TRUNCATE → INSERT INTO
- Ventaja: Control total del schema, no hay duplicados ni acumulación

**Silver Layer (Transformación):**
- Método: INSERT OVERWRITE
- Razón: Tablas ya creadas por DDL, reemplazar datos completos
- Flujo: Lee Bronze → Transforma → INSERT OVERWRITE
- Ventaja: Reemplaza datos completos, no acumula, siempre consistente

**Gold Layer (Agregación):**
- Método: INSERT OVERWRITE
- Razón: Tablas ya creadas por DDL, reemplazar métricas completas
- Flujo: Lee Silver → Calcula métricas → INSERT OVERWRITE
- Ventaja: Métricas siempre actualizadas, no hay datos parciales

**Por qué NO saveAsTable:**
- saveAsTable recrearía las tablas cada vez que se guardan datos
- Tablas tipo EXTERNAL para mayor protección
- DDL define schema explícito una vez
- Notebooks solo insertan/reemplazan datos

## Notas Tecnicas

### Origen de Dimensiones
- dim_tiempo: Generada desde codigo en Databricks
- dim_clientes y dim_productos: Transformadas desde Silver con logica de negocio
- dim_sucursales: Leida desde Bronze (datos maestros sincronizados desde PostgreSQL)

### Ciclo de Vida de Datos

**Escenario: DROP → DDL → Populate → DROP**

1. DROP SCHEMA CASCADE: Elimina metadatos (tablas), datos persisten en Storage
2. CREATE EXTERNAL TABLE: Recrea metadatos apuntando a LOCATION
3. INSERT OVERWRITE: Reemplaza datos en LOCATION (no acumula)
4. DROP SCHEMA CASCADE: Elimina metadatos, datos persisten

**Resultado:** No hay acumulación de datos, todo limpio y profesional

### Prerequisitos de Ejecucion
- DDL debe ejecutarse primero para crear tablas EXTERNAL
- Bronze Layer debe contener datos antes de ejecutar pipeline
- Azure Data Factory configurado para sincronizacion automatica
- Databricks Jobs creados y configurados con dependencias correctas
- Verificar conectividad a Azure Storage con Access Connector configurado
- Key Vault configurado con secrets de bases de datos (scope: accesskeys-campesinita)

### Ejecucion Paralela en Jobs
Los Databricks Jobs ejecutan tasks en paralelo cuando no tienen dependencias:
- Tasks 1-3 (Bronze to Silver): Ejecucion paralela simultanea
- Tasks 5-6 (metricas Gold): Ejecucion paralela despues de Task 4

Esto reduce significativamente el tiempo total de ejecucion del pipeline.
