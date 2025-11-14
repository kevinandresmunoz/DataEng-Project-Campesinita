# La Campesinita - Ingenieria de Datos

Sistema de ingenieria de datos para retail con arquitectura Medallion en Azure Databricks.

## Descripcion

Pipeline completo de datos que simula el ecosistema de informacion de una cadena de supermercados colombiana. El sistema genera datos sinteticos realistas mediante Apache Airflow, los almacena en bases de datos operacionales (PostgreSQL y MySQL), y los procesa a traves de arquitectura Medallion hasta un modelo dimensional optimizado para analisis de negocio.

## Generacion de Datos

El proyecto utiliza Apache Airflow para simular operaciones realistas de una cadena de retail:

**Datos base:**
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
Apache Superset (dashboards)
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
│   ├── dashboard_ventas_ejecutivo.json
│   └── dashboard_inventario_operacional.json
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
  1. Sube notebooks a `/Workspace/la_campesinita/` en Databricks
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

El sistema utiliza Azure Data Factory para orquestar la ingestion y procesamiento de datos:

**Pipeline Configurado:** `Pipeline_Ingestion_and_Processing_Dev`

**Activities:**
1. Sync_PostgreSQL: Ejecuta notebook de ingestion desde PostgreSQL a Bronze Layer
2. Sync_MySQL: Ejecuta notebook de ingestion desde MySQL a Bronze Layer (secuencial)
3. Trigger_Processing_Job: Dispara Job de Databricks para procesamiento completo (secuencial)

**Trigger Programado:**
- Frecuencia: 2 veces al dia
- Horarios: 1:00 PM y 9:00 PM (hora Colombia)
- Zona horaria: UTC-05:00 Bogota

### Databricks Job

**Job Configurado:** `Processing_Bronze_to_Gold`

**Tasks de Procesamiento:**

Fase 1 - Bronze to Silver (paralelo):
- Task 1: Procesa ventas consolidadas
- Task 2: Procesa clientes segmentados
- Task 3: Procesa inventario y productos

Fase 2 - Silver to Gold (secuencial):
- Task 4: Construye modelo dimensional (dimensiones)
- Task 5: Calcula metricas de ventas (paralelo con Task 6)
- Task 6: Calcula metricas de inventario (paralelo con Task 5)

**Dependencias:**
- Tasks 1-3 se ejecutan en paralelo
- Task 4 espera a que terminen Tasks 1-3
- Tasks 5-6 se ejecutan en paralelo despues de Task 4

### Operacion Automatica

El sistema opera de forma completamente automatica:

1. **1 PM y 9 PM**: Data Factory inicia pipeline de ingestion
2. **5-10 min**: Sincronizacion de datos a Bronze Layer
3. **Automatico**: Data Factory dispara Databricks Job
4. **25-35 min**: Procesamiento Bronze → Silver → Gold
5. **Resultado**: Datos actualizados disponibles en Gold Layer para dashboards

## Dashboards de Business Intelligence

El proyecto incluye 2 dashboards predefinidos que visualizan los datos procesados en Gold Layer:

### Dashboard Ejecutivo de Ventas

Proporciona vision estrategica del negocio para toma de decisiones ejecutivas:

- Ventas totales y tendencias mensuales
- Top 10 sucursales por ingresos
- Margen bruto total y promedio
- Ticket promedio de compra
- Numero de transacciones y clientes unicos
- Top 20 productos por ingresos
- Segmentacion RFM de clientes (Recencia, Frecuencia, Valor Monetario)
- Patron de ventas por hora y dia de la semana
- Comparativa ventas fin de semana vs entre semana

**Uso:** Gerencia general, directores comerciales, analistas de negocio

### Dashboard Operacional de Inventario

Monitoreo en tiempo real para gestion operativa de inventario:

- Valor total de inventario por sucursal
- Items criticos que requieren atencion inmediata
- Items en alerta por bajo stock
- Items a reabastecer por sucursal
- Alertas por prioridad (Alta, Media, Baja)
- Acciones recomendadas automaticas
- Top 30 productos criticos con detalles
- Estado de caducidad por producto
- Dias promedio hasta caducidad
- Timeline de alertas criticas

**Uso:** Gerentes de sucursal, jefes de inventario, compradores

**Importar dashboards:**
- `dashboards/dashboard_ventas_ejecutivo.json`
- `dashboards/dashboard_inventario_operacional.json`

Configurar conexion a Databricks en Superset:
```
databricks://token:<TOKEN>@<HOST>:443/<CATALOG>?http_path=<HTTP_PATH>
```

## Volumetria

- Bronze: 1.85 GB (12 tablas)
- Silver: 1.2 GB (6 tablas)
- Gold: 800 MB (11 tablas)
- Pipeline: 25-35 minutos
- Frecuencia: 2 veces al dia

## Recursos Recomendados

- Cluster: Standard_DS3_v2 o superior
- Spark: 3.4+
- DBR: 13.3 LTS

## Documentacion Tecnica

Ver [proceso/descripcionETL.md](proceso/descripcionETL.md) para detalles del proceso ETL.

## Prerequisitos Azure

Infraestructura configurada:
- Databricks Workspace: adb-campesinita-dev
- Unity Catalog: 2 catalogos (dev y prod)
- Storage Accounts: adlcampesinitadev y adlcampesinitaprod
- Contenedores: bronze, silver, gold
- Access Connectors con Managed Identity
- External Locations: 6 (3 por ambiente)
- Data Factory: adf-campesinita
- Key Vault: keys-campesinita

Ver imagenes en carpeta `images/` para referencia visual de la configuracion.

---

Proyecto educativo - Ingenieria de Datos
