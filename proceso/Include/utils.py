# Databricks notebook source
"""
Utilidades Compartidas - La Campesinita
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from datetime import datetime


def log(message: str):
    """Log con timestamp"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")


def get_catalog():
    """
    Obtener catalogo correcto automaticamente
    Busca cualquier catalogo que contenga 'campesinita' y 'dev' o 'prod'
    """
    # Primero intentar con el catalogo actual
    current = spark.sql("SELECT current_catalog()").collect()[0][0]
    
    # Si el catalogo actual contiene 'campesinita', usarlo
    if "campesinita" in current.lower():
        return current
    
    # Si no, buscar en todos los catalogos disponibles
    catalogs = spark.sql("SHOW CATALOGS").collect()
    
    # Buscar catalogo con 'campesinita' y 'dev'
    for row in catalogs:
        cat_name = row[0]
        if "campesinita" in cat_name.lower() and "dev" in cat_name.lower():
            log(f"Catalogo detectado: {cat_name}")
            spark.sql(f"USE CATALOG {cat_name}")
            return cat_name
    
    # Si no encuentra dev, buscar prod
    for row in catalogs:
        cat_name = row[0]
        if "campesinita" in cat_name.lower() and "prod" in cat_name.lower():
            log(f"Catalogo detectado: {cat_name}")
            spark.sql(f"USE CATALOG {cat_name}")
            return cat_name
    
    # Si no encuentra ninguno, buscar cualquiera con 'campesinita'
    for row in catalogs:
        cat_name = row[0]
        if "campesinita" in cat_name.lower():
            log(f"Catalogo detectado: {cat_name}")
            spark.sql(f"USE CATALOG {cat_name}")
            return cat_name
    
    # Si no encuentra nada, usar el actual
    log(f"Usando catalogo actual: {current}")
    return current


def read_bronze_table(table: str) -> DataFrame:
    """
    Leer tabla desde Bronze Layer
    
    Args:
        table: nombre de la tabla (ej: ventas, clientes)
    """
    catalog = get_catalog()
    full_table = f"{catalog}.bronze.{table}"
    log(f"Leyendo Bronze: {full_table}")
    
    try:
        df = spark.table(full_table)
        count = df.count()
        log(f"Registros leidos: {count:,}")
        return df
    except Exception as e:
        log(f"Error leyendo {full_table}: {e}")
        log("Verificando si la tabla existe...")
        
        # Intentar listar tablas en bronze para debug
        try:
            tables = spark.sql(f"SHOW TABLES IN {catalog}.bronze").collect()
            log(f"Tablas disponibles en {catalog}.bronze:")
            for t in tables:
                log(f"  - {t.tableName}")
        except:
            pass
        
        raise Exception(f"No se pudo leer la tabla {full_table}. Verifica que el DDL se haya ejecutado correctamente.")


def write_silver(df: DataFrame, table: str, partition_by: list = None):
    """
    Escribir a Silver Layer (EXTERNAL TABLE)
    Usa INSERT OVERWRITE para reemplazar datos sin eliminarlos en DROP
    
    Args:
        df: DataFrame a escribir
        table: nombre de la tabla
        partition_by: lista de columnas para particionar (opcional)
    """
    catalog = get_catalog()
    full_table = f"{catalog}.silver.{table}"
    
    df_audit = df.withColumn("_transform_timestamp", current_timestamp())
    
    log(f"Escribiendo Silver: {full_table}")
    
    df_audit.createOrReplaceTempView("temp_write")
    
    spark.sql(f"INSERT OVERWRITE TABLE {full_table} SELECT * FROM temp_write")
    
    log(f"Escritos: {df_audit.count():,} registros")


def write_gold(df: DataFrame, table: str, partition_by: list = None):
    """
    Escribir a Gold Layer (EXTERNAL TABLE)
    Usa INSERT OVERWRITE para reemplazar datos sin eliminarlos en DROP
    
    Args:
        df: DataFrame a escribir
        table: nombre de la tabla
        partition_by: lista de columnas para particionar (opcional)
    """
    catalog = get_catalog()
    full_table = f"{catalog}.gold.{table}"
    
    log(f"Escribiendo Gold: {full_table}")
    
    df.createOrReplaceTempView("temp_write")
    
    spark.sql(f"INSERT OVERWRITE TABLE {full_table} SELECT * FROM temp_write")
    
    log(f"Escritos: {df.count():,} registros")


def merge_to_silver(df: DataFrame, table: str, merge_keys: list, partition_by: list = None):
    """
    Merge incremental a Silver Layer (para casos que requieren UPSERT)
    
    Args:
        df: DataFrame con datos nuevos
        table: nombre de la tabla
        merge_keys: columnas para hacer match (ej: ["cliente_id"])
        partition_by: lista de columnas para particionar (opcional)
    """
    catalog = get_catalog()
    full_table = f"{catalog}.silver.{table}"
    
    df_audit = df.withColumn("_transform_timestamp", current_timestamp())
    
    log(f"Merge a Silver: {full_table}")
    
    df_audit.createOrReplaceTempView("updates")
    
    merge_condition = " AND ".join([f"target.{k} = updates.{k}" for k in merge_keys])
    
    merge_sql = f"""
    MERGE INTO {full_table} AS target
    USING updates
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
    log(f"Merge completado")


def merge_to_gold(df: DataFrame, table: str, merge_keys: list, partition_by: list = None):
    """
    Merge incremental a Gold Layer (para casos que requieren UPSERT)
    
    Args:
        df: DataFrame con datos nuevos
        table: nombre de la tabla
        merge_keys: columnas para hacer match
        partition_by: lista de columnas para particionar (opcional)
    """
    catalog = get_catalog()
    full_table = f"{catalog}.gold.{table}"
    
    log(f"Merge a Gold: {full_table}")
    
    df.createOrReplaceTempView("updates")
    
    merge_condition = " AND ".join([f"target.{k} = updates.{k}" for k in merge_keys])
    
    merge_sql = f"""
    MERGE INTO {full_table} AS target
    USING updates
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
    log(f"Merge completado")


def optimize_table(table: str, zorder_cols: list = None):
    """
    Optimizar tabla Delta con OPTIMIZE y Z-ORDER
    
    Args:
        table: nombre completo de la tabla (ej: silver.ventas_clean)
        zorder_cols: lista de columnas para Z-ORDER (opcional)
    """
    catalog = get_catalog()
    full_table = f"{catalog}.{table}"
    
    log(f"Optimizando: {full_table}")
    spark.sql(f"OPTIMIZE {full_table}")
    
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE {full_table} ZORDER BY ({cols})")
        log(f"Z-ORDER aplicado en: {cols}")
    
    log("Optimizacion completada")
