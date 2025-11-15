# La Campesinita - Ingenieria de Datos

Sistema de ingenieria de datos para retail con arquitectura Medallion en Azure Databricks.

## Descripcion

Pipeline completo de datos que simula el ecosistema de informacion de una cadena de supermercados colombiana. El sistema genera datos sinteticos realistas mediante Apache Airflow, los almacena en bases de datos operacionales (PostgreSQL y MySQL), y los procesa a traves de arquitectura Medallion hasta un modelo dimensional optimizado para analisis de negocio.

## Nota Importante sobre los Datos

**Datos Sinteticos y Bootstrapping:**

Este proyecto utiliza datos completamente sinteticos generados mediante tecnicas de bootstrapping (similar a [DataCamp Bootstrapping](https://www.datacamp.com/tutorial/bootstrapping)). Los datos han sido aumentados a partir de muestras base para crear volumenes considerables que permitan probar la arquitectura de datos a escala.

**Enfoque del Proyecto:**

El objetivo principal NO es el analisis de datos, sino demostrar y validar:
- Arquitectura Medallion en entorno cloud
- Pipeline completo de ingenieria de datos
- Procesamiento distribuido con Spark
- Orquestacion automatizada
- Integracion de multiples tecnologias (Airflow, Databricks, Azure Data Factory)
- Manejo de volumenes significativos de datos

Los datos sinteticos permiten simular escenarios realistas sin comprometer informacion sensible, facilitando pruebas de rendimiento, escalabilidad y validacion de la arquitectura propuesta.

## Generacion de Datos Sinteticos

El proyecto utiliza Apache Airflow para simular operaciones realistas de una cadena de retail:

**Datos base generados:**
- 25,000 clientes con datos demograficos
- 8,000 productos activos con precios y costos
- 30 sucursales distribuidas en Colombia
- 500,000 ventas historicas (1 año)
- 6.5M detalles de venta (promedio 13 productos por venta)
- 30,000 registros de inventario

**Simuladores Airflow (7 AM - 8 PM):**
- Ventas continuas cada hora
- Deteccion de reabastecimiento cada 2 horas
- Recepcion de mercancia (10 AM y 3 PM)
- Ajustes de inventario (8:30 PM)
- Actualizacion de precios (6 AM)
- Cierre diario (8 PM)

Los simuladores generan transacciones con patrones realistas (picos en almuerzo y salida del trabajo) y factores estacionales colombianos, cargando datos continuamente a PostgreSQL y MySQL.

## Arquitectura

```
Apache Airflow (Simuladores)
    ↓
PostgreSQL + MySQL (Datos Sinteticos)
    ↓
Azure Data Factory (1 PM y 9 PM)
    ↓
Bronze Layer (datos crudos)
    ↓
Databricks Jobs (automatico)
    ↓
Silver Layer (datos limpios)
    ↓
Gold Layer (modelo dimensional)
    ↓
Databricks Dashboards (visualizacion)
```

## Estructura

```
project_campesinita/
├── proceso/
│   ├── ingestion_to_bronze/     # Sincronizacion BD → Bronze
│   ├── bronze_to_silver/        # Limpieza y validacion
│   ├── silver_to_gold/          # Modelo dimensional
│   ├── Include/                 # Utilidades compartidas
│   └── validate/                # Scripts de validacion
├── scripts/
│   ├── 1_drop_medallion.py      # Elimina catalogos y schemas
│   ├── 2_ddl_medallion.py       # Crea catalogos y schemas
│   └── 3_populate_data.py       # Ejecuta pipeline completo
├── seguridad/
│   ├── 4_grants_medallion.py    # Permisos Unity Catalog
│   └── 5_delta_sharing.py       # Comparticion de datos
├── reversion/
│   └── revocar_permisos.py      # Revoca permisos
├── dashboards/
│   ├── campesinita.lvdash.json  # Dashboard Databricks
│   └── Dashboardcampesinita.PNG # Captura del dashboard
└── images/                      # Capturas de configuracion Azure
```

## Modelo de Datos Gold Layer

**Dimensiones:**
- dim_tiempo: Calendario 2024-2025 (generada en Databricks)
- dim_clientes: Segmentacion RFM (transformada desde Silver)
- dim_productos: Catalogo con margenes (transformada desde Silver)
- dim_sucursales: 30 sucursales (leida desde Bronze)

**Hechos:**
- fact_ventas: Transacciones detalladas
- fact_kpis_diarios: KPIs por dia y sucursal
- fact_analisis_clientes: Segmentacion RFM
- fact_rendimiento_productos: Performance por sucursal
- fact_inventario: Stock actual con alertas
- fact_kpis_inventario: Metricas por sucursal
- fact_alertas_inventario: Items criticos

## CI/CD con GitHub Actions

El proyecto implementa despliegue automatico mediante GitHub Actions.

### Workflow Configurado

**Archivo:** `.github/workflows/databricks-deploy.yml`

**Triggers:**
- Push a rama `main`: Despliega a ambiente Dev
- Create Release: Despliega a ambiente Prod

### Flujo de Desarrollo

**Rama updates (desarrollo):**
- Commits y push a `updates` no disparan despliegue
- Permite desarrollo iterativo sin afectar ambiente Dev

**Rama main (produccion Dev):**
- Merge de Pull Request `updates` → `main` dispara workflow automaticamente
- GitHub Actions ejecuta:
  1. Sube notebooks a `/Users/well2chat_outlook.com#ext#@well2chatoutlook.onmicrosoft.com/DataEng-Project-Campesinita/` en Databricks
  2. Ejecuta `1_drop_medallion.py` (elimina schemas existentes)
  3. Ejecuta `2_ddl_medallion.py` (crea catalogos y schemas)
  4. Ejecuta `3_populate_data.py` (ejecuta pipeline completo de datos)
  5. Ejecuta `4_grants_medallion.py` (configura permisos Unity Catalog)

**Release (produccion Prod):**
- Creacion de release en GitHub despliega notebooks a ambiente Prod
- Setup se ejecuta manualmente desde Databricks con widget ambiente: prod

### Secrets Configurados

El workflow utiliza secrets de GitHub para autenticacion:
- `DATABRICKS_HOST`: URL del workspace Databricks
- `DATABRICKS_TOKEN_DEV`: Token de acceso para catalogo dev
- `DATABRICKS_TOKEN_PROD`: Token de acceso para catalogo prod

## Orquestacion y Procesamiento

### Azure Data Factory

**Pipeline:** `Pipeline_Ingestion_and_Processing`

**Activities:**
1. Sync_PostgreSQL: Notebook sync_postgres_to_bronze (paralelo con Activity 2)
2. Sync_MySQL: Notebook sync_mysql_to_bronze (paralelo con Activity 1)
3. Trigger_Processing_Bronze_to_Gold: Job Databricks ID 963768162698377 (depende de 1 y 2)

**Configuracion:**
- Timeout ingestion: 20 minutos por notebook
- Timeout processing: 1 hora
- Retry ingestion: 2 intentos
- Retry processing: 1 intento

**Trigger:** `trigger_daily_ingestion`
- Tipo: Schedule
- Frecuencia: Diaria
- Horarios: 13:00 y 21:00
- Zona horaria: SA Pacific Standard Time (UTC-05:00 Bogota)
- Estado: Started

### Databricks Job

**Job:** `Processing_Bronze_to_Gold_Dev`
**Job ID:** 963768162698377
**Cluster:** 1113-193707-hexeup3u (existing cluster con auto-termination 1 hora)

**Tasks:**

Fase 1 - Bronze to Silver (3 tasks paralelas):
- bronze_to_silver_ventas: ventas_consolidadas.py
- bronze_to_silver_clientes: clientes_segmentados.py
- bronze_to_silver_inventario: inventario_productos.py

Fase 2 - Silver to Gold Dimensiones (1 task secuencial):
- silver_to_gold_modelo_dimensional: modelo_dimensional.py (depende de Fase 1)

Fase 3 - Silver to Gold Metricas (2 tasks paralelas):
- silver_to_gold_metricas_ventas: metricas_ventas.py (depende de Fase 2)
- silver_to_gold_metricas_inventario: metricas_inventario.py (depende de Fase 2)

**Rutas notebooks:**
`/Users/well2chat_outlook.com#ext#@well2chatoutlook.onmicrosoft.com/DataEng-Project-Campesinita/proceso/`

### Flujo de Ejecucion

**Trigger diario (1 PM y 9 PM):**
1. ADF ejecuta Sync_PostgreSQL y Sync_MySQL en paralelo (15-20 min)
2. ADF dispara Job Databricks cuando ambos terminan exitosamente
3. Databricks ejecuta 6 tasks con dependencias (30-40 min)
4. Datos actualizados en Gold Layer

**Tiempo total:** 45-60 minutos por ejecucion

### Arquitectura de Tablas y Estrategias de Escritura

**Todas las tablas son EXTERNAL:**
- Definidas con LOCATION explícita en Azure Storage
- DROP elimina solo metadatos, datos persisten
- Protección contra eliminación accidental
- Recuperación ante errores de catálogo

**Bronze Layer (Ingestion):**
- Estrategia: TRUNCATE + INSERT INTO
- Razón: Tablas ya creadas por DDL, solo insertar datos
- Ventaja: Control total del schema, no hay duplicados

**Silver y Gold (Transformación):**
- Estrategia: INSERT OVERWRITE
- Razón: Tablas ya creadas por DDL, reemplazar datos completos
- Ventaja: No acumula datos, siempre refleja estado actual

**Por qué no saveAsTable:**
- saveAsTable crearía tablas MANAGED (elimina datos en DROP)
- EXTERNAL para protección
- DDL define schema explícito, notebooks solo insertan datos

## Dashboard de Business Intelligence

El proyecto incluye un dashboard interactivo creado en Databricks que visualiza los datos procesados en Gold Layer:

### Dashboard La Campesinita

Dashboard operacional que proporciona metricas clave del negocio:

**Visualizaciones incluidas:**
- Sumatoria de ventas por jornada (mañana, tarde, noche) - grafico de area semanal
- Numero de transacciones por dia - tendencia temporal
- Numero de articulos vendidos por hora del dia - distribucion por periodo
- Total ganancia - metrica agregada de ingresos totales

**Datasets utilizados:**
- fact_kpis_diarios: Metricas agregadas diarias por sucursal
- fact_ventas: Transacciones detalladas con dimensiones temporales
- fact_rendimiento_productos: Performance de productos y rentabilidad
- fact_kpis_inventario: Metricas de inventario por sucursal
- fact_alertas_inventario: Items criticos que requieren atencion

**Acceso al dashboard:**
- Archivo: `dashboards/campesinita.lvdash.json`
- Captura: `dashboards/Dashboardcampesinita.PNG`
- Importar en Databricks: Workspace → Dashboards → Import Dashboard
- Catalogo: `adb_campesinita_dev.gold`

**Uso:** Gerencia general, directores comerciales, gerentes de sucursal, analistas de negocio

## Volumetria

**Datos Sinteticos:**
- Bronze: 1.85 GB (12 tablas) - Datos crudos sincronizados
- Silver: 1.2 GB (6 tablas) - Datos limpios y validados
- Gold: 800 MB (11 tablas) - Modelo dimensional optimizado
- Total: ~3.85 GB de datos sinteticos

**Procesamiento:**
- Pipeline completo: 25-35 minutos
- Frecuencia: 2 veces al dia (1 PM y 9 PM)
- Ejecucion paralela en Bronze to Silver
- Ejecucion secuencial en Silver to Gold

## Documentacion Tecnica

Ver [proceso/descripcionETL.md](proceso/descripcionETL.md) para detalles del proceso ETL.

## Servicios Azure Requeridos

Este proyecto requiere la siguiente infraestructura en Azure para su implementacion:

### 1. Azure Databricks Workspace
- Tier: Premium o Enterprise (requerido para Unity Catalog)
- Runtime: DBR 13.3 LTS o superior
- Spark: 3.4+
- Cluster recomendado: Standard_DS3_v2 o superior

### 2. Unity Catalog
- 2 catalogos configurados (dev y prod)
- Metastore asociado al workspace
- Permisos configurados a nivel de catalogo, schema y tabla

### 3. Azure Data Lake Storage Gen2 (ADLS)
- 2 Storage Accounts (uno por ambiente: dev y prod)
- Contenedores por ambiente:
  - bronze: Datos crudos sincronizados desde bases de datos
  - silver: Datos limpios y validados
  - gold: Modelo dimensional optimizado
- Configuracion de red y firewall segun politicas de seguridad

### 4. Access Connectors for Azure Databricks
- 2 Access Connectors (uno por ambiente)
- Managed Identity habilitada
- Roles asignados:
  - Storage Blob Data Contributor en los storage accounts correspondientes
  - Permisos de lectura/escritura en contenedores bronze, silver, gold

### 5. External Locations (Unity Catalog)
- 6 External Locations configuradas (3 por ambiente):
  - extl-campesinita-dev-bronze
  - extl-campesinita-dev-silver
  - extl-campesinita-dev-gold
  - extl-campesinita-prod-bronze
  - extl-campesinita-prod-silver
  - extl-campesinita-prod-gold
- Cada External Location vinculada a su Access Connector correspondiente

### 6. Azure Data Factory
- Pipeline: Pipeline_Ingestion_and_Processing
- Linked Service: ls_adbcampesinita (conexion a Databricks con Access Token)
- Trigger: trigger_daily_ingestion (Schedule diario 13:00 y 21:00 UTC-05:00)
- Activities:
  - 2 notebooks Databricks (ingestion paralela)
  - 1 job Databricks (procesamiento con dependencias)

### 7. Azure Key Vault
- Secrets configurados para conexiones a bases de datos:
  - PostgreSQL: host, port, database, user, password
  - MySQL: host, port, database, user, password
- Scope en Databricks: accesskeys-campesinita
- Permisos de lectura para Service Principal de Databricks

### 8. Bases de Datos Operacionales (Fuentes de Datos)
- PostgreSQL: 7 tablas (ventas, clientes, productos, sucursales, inventario, empleados, detalle_ventas)
- MySQL: 5 tablas (proveedores, ordenes_compra, movimientos_inventario, recepciones, productos_erp)
- Datos generados por Apache Airflow (ver directorio data_source)

### 9. Azure Active Directory
- Usuarios habilitados en Azure AD
- Sincronizacion con Databricks Workspace
- Grupos de seguridad configurados

### Configuracion de Usuarios y Permisos

La gestion de usuarios y permisos se realiza mediante:

1. Habilitar usuarios en Azure Active Directory
2. Sincronizar usuarios con Databricks Workspace
3. Acceder al panel de administracion de Databricks (Admin Console)
4. Crear grupo de seguridad: analitica
5. Agregar usuarios al grupo analitica
6. Ejecutar script de permisos: seguridad/4_grants_medallion.py
7. Los permisos se asignan a nivel de grupo, no a usuarios individuales

Permisos del grupo analitica:
- USAGE y SELECT en schema bronze
- USAGE, SELECT, MODIFY y CREATE TABLE en schemas silver y gold
- CREATE EXTERNAL TABLE en External Locations

### Referencia Visual

Ver capturas de pantalla de configuraciones en el directorio `images/`:
- Configuracion de Azure Storage Accounts
- Contenedores en ADLS (bronze, silver, gold)
- External Locations en Unity Catalog
- Access Connectors y Managed Identity
- Databricks Credentials y Scopes
- Servicios Azure desplegados

Estas imagenes sirven como guia visual para replicar la configuracion en otros ambientes.

---

Proyecto educativo - Ingenieria de Datos
