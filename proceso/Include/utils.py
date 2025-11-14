# Databricks notebook source
"""
Utilidades Compartidas - La Campesinita
Lectura directa desde Bronze usando Access Connector (Managed Identity)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.utils import AnalysisException
from datetime import datetime


def log(message: str):
    """Log con timestamp"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


def get_storage_account():
    """
    Obtener cuenta de storage según catalogo activo
    Detecta automáticamente si es dev o prod desde el catalogo configurado
    """
    try:
        # Obtener catalogo actual
        current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
        
        if "dev" in current_catalog.lower():
            return "adlcampesinitadev"
        elif "prod" in current_catalog.lower():
            return "adlcampesinitaprod"
        else:
            return "adlcampesinitadev"
    except Exception as e:
        log(f"Error detectando ambiente, usando dev: {e}")
        return "adlcampesinitadev"


def get_container_name(layer: str) -> str:
    """Obtener nombre de contenedor por capa"""
    containers = {
        "bronze": "bronze",
        "silver": "silver",
        "gold": "gold"
    }
    return containers.get(layer, f"{layer}-layer")


def construct_path(container: str, path: str) -> str:
    """
    Construir ruta Azure Blob Storage
    Usa Access Connector (Managed Identity) para autenticación automática
    """
    account = get_storage_account()
    return f"abfss://{container}@{account}.dfs.core.windows.net/{path}"


def read_bronze(source: str, table: str) -> DataFrame:
    """
    Leer desde Bronze Layer (datos ya sincronizados desde BD)
    
    Args:
        source: postgres, mysql_erp
        table: nombre de la tabla
    
    Nota: Si Bronze está vacío, ejecuta notebooks de ingestion:
          - 00_ingestion_to_bronze/INGESTION_MASTER.py
          O configura Azure Data Factory para sincronización automática
    """
    container = get_container_name("bronze")
    path = construct_path(container, f"{source}/{table}/*/*.parquet")
    log(f"Leyendo Bronze: {source}/{table}")
    
    try:
        # Intentar leer datos
        df = spark.read.parquet(path)
        count = df.count()
        
        if count == 0:
            error_msg = (
                f"Bronze Layer vacio para {source}/{table}.\n"
                f"SOLUCION:\n"
                f"  1. Verifica que las bases de datos tengan datos\n"
                f"  2. Ejecuta notebooks de ingestion en Databricks\n"
                f"  3. O configura Azure Data Factory (ver config_azure/CONFIGURACION_DATA_FACTORY.md)\n"
                f"  4. Valida: python config_azure/validar_configuracion.py"
            )
            log(f"ERROR: {error_msg}")
            raise ValueError(error_msg)
        
        log(f"Registros leídos: {count:,}")
        return df
        
    except AnalysisException as e:
        if "Path does not exist" in str(e):
            error_msg = (
                f"Ruta no existe en Bronze: {source}/{table}\n"
                f"SOLUCION:\n"
                f"  1. Verifica que Azure Storage este configurado\n"
                f"  2. Valida configuracion: python config_azure/validar_configuracion.py\n"
                f"  3. Ejecuta notebooks de ingestion en Databricks\n"
                f"  4. O configura Azure Data Factory (ver config_azure/CONFIGURACION_DATA_FACTORY.md)"
            )
            log(f"ERROR: {error_msg}")
            raise ValueError(error_msg)
        else:
            log(f"ERROR leyendo Bronze: {e}")
            raise
    except Exception as e:
        log(f"ERROR inesperado: {e}")
        raise


def write_silver(df: DataFrame, table: str, partition_by: list = None):
    """
    Escribir a Silver Layer con metadatos de auditoría
    
    Args:
        df: DataFrame a escribir
        table: nombre de la tabla
        partition_by: lista de columnas para particionar (opcional)
    """
    # Agregar timestamp de transformación
    df_audit = df.withColumn("_transform_timestamp", current_timestamp())
    
    container = get_container_name("silver")
    path = construct_path(container, table)
    full_table = f"silver.{table}"
    
    log(f"Escribiendo Silver: {full_table}")
    
    writer = df_audit.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
        log(f"Particionado por: {', '.join(partition_by)}")
    
    writer.option("path", path).saveAsTable(full_table)
    log(f"Escritos: {df_audit.count():,} registros")


def write_gold(df: DataFrame, table: str, partition_by: list = None):
    """
    Escribir a Gold Layer
    
    Args:
        df: DataFrame a escribir
        table: nombre de la tabla
        partition_by: lista de columnas para particionar (opcional)
    """
    container = get_container_name("gold")
    path = construct_path(container, table)
    full_table = f"gold.{table}"
    
    log(f"Escribiendo Gold: {full_table}")
    
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
        log(f"Particionado por: {', '.join(partition_by)}")
    
    writer.option("path", path).saveAsTable(full_table)
    log(f"Escritos: {df.count():,} registros")


def optimize_table(table: str, zorder_cols: list = None):
    """
    Optimizar tabla Delta con OPTIMIZE y Z-ORDER
    
    Args:
        table: nombre completo de la tabla (ej: silver.ventas_clean)
        zorder_cols: lista de columnas para Z-ORDER (opcional)
    
    Nota: OPTIMIZE compacta archivos pequeños y mejora performance de lectura
          Z-ORDER co-localiza datos relacionados para consultas más rápidas
    """
    log(f"Optimizando: {table}")
    spark.sql(f"OPTIMIZE {table}")
    
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({cols})")
        log(f"Z-ORDER aplicado en: {cols}")
    
    log("Optimización completada")


def create_database_if_not_exists(database: str):
    """Crear database si no existe"""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    log(f"Database verificada: {database}")
