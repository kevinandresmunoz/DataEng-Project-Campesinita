# Despliegue del Proyecto

El despliegue se realiza automaticamente via GitHub Actions que sube los notebooks a `/Workspace/la_campesinita/` en Databricks.

## GitHub Actions Workflow

El workflow `.github/workflows/databricks-deploy.yml` automatiza el despliegue:

**Push a main:**
- Sube notebooks a `/Workspace/la_campesinita/` en ambiente dev
- Ejecuta scripts de setup automaticamente
- Configura permisos

**Create Release:**
- Sube notebooks a `/Workspace/la_campesinita/` en ambiente prod
- Scripts se ejecutan manualmente desde Databricks

## Configurar Secrets en GitHub

1. GitHub → Settings → Secrets and variables → Actions
2. Crear 3 secrets:

**DATABRICKS_HOST:**
```
https://adb-xxxxx.azuredatabricks.net
```

**DATABRICKS_TOKEN_DEV:**
Token de acceso para catalogo dev (generar en Databricks → User Settings → Access Tokens)

**DATABRICKS_TOKEN_PROD:**
Token de acceso para catalogo prod (generar en Databricks → User Settings → Access Tokens)

## Deploy a Dev

```bash
cd project_campesinita
git add .
git commit -m "Actualizar notebooks"
git push origin main
```

GitHub Actions ejecuta automaticamente:
1. Sube notebooks a `/Workspace/la_campesinita/`
2. Ejecuta `1_drop_medallion.py` con ambiente: dev
3. Ejecuta `2_ddl_medallion.py` con ambiente: dev
4. Ejecuta `3_populate_data.py`
5. Ejecuta `4_grants_medallion.py` con ambiente: dev

## Deploy a Prod

```bash
cd project_campesinita
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin v1.0.0
```

Luego crear release en GitHub UI.

GitHub Actions ejecuta:
1. Sube notebooks a `/Workspace/la_campesinita/`
2. Notifica que scripts deben ejecutarse manualmente

Ejecutar manualmente desde Databricks con widget ambiente: prod

## Configurar Data Factory

Despues del primer deploy, configurar Data Factory y Jobs apuntando a:
- Notebooks: `/Workspace/la_campesinita/proceso/...`
- Jobs: Tasks con rutas en `/Workspace/la_campesinita/proceso/...`

Ver README.md principal para detalles.

## Verificacion

Ver logs en GitHub → Actions tab

Verificar notebooks en Databricks → Workspace → /Workspace/la_campesinita

## Renovacion de Tokens

Tokens expiran cada 90 dias. Actualizar en GitHub → Settings → Secrets.
