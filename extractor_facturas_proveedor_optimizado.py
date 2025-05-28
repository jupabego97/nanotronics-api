#!/usr/bin/env python3
"""
Extractor optimizado de facturas de proveedor (bills) desde la API de Alegra
--------------------------------------------------------------------------

▶ **Funcionalidades principales:**
   1. Extrae facturas de proveedor (bills) desde la API de Alegra
   2. Procesa y aplana los ítems de cada factura de proveedor
   3. Guarda en CSV y PostgreSQL con tipos de datos optimizados
   4. Manejo incremental: solo procesa facturas nuevas
   5. Soporte para múltiples formatos de salida

▶ **Mejoras respecto al notebook original:**
   - Manejo robusto de errores y reintentos
   - Configuración centralizada y flexible
   - Logging detallado para seguimiento
   - Validación de datos y tipos
   - Optimización de consultas a la API
   - Soporte para variables de entorno

Requisitos:
```bash
pip install pandas requests sqlalchemy psycopg2-binary python-dotenv
```
"""
from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# Importaciones opcionales para PostgreSQL
try:
    from sqlalchemy import create_engine, types as sa_types
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    logging.warning("SQLAlchemy no está disponible. Solo se guardará en CSV.")


# ---------------------------------------------------------------------------
# Configuración global
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class Config:
    """Configuración centralizada del extractor de facturas de proveedor."""
    
    # API Configuration
    base_url: str = "https://api.alegra.com/api/v1/bills"
    api_key_env: str = "ALEGRA_API_KEY"
    api_key_default: str = "bmFub3Ryb25pY3NhbHNvbmRlbGF0ZWNub2xvZ2lhQGdtYWlsLmNvbTphMmM4OTA3YjE1M2VmYTc0ODE5ZA=="
    
    # Request settings
    page_size: int = 30
    max_retries: int = 3
    backoff_factor: float = 1.5
    timeout_seconds: int = 30
    
    # File settings
    csv_filename: str = "facturas_proveedor.csv"
    workspace_dir: str = "."
    
    # Database settings
    db_url_env: str = "DATABASE_URL"
    db_table_name: str = "bills"
    
    # Processing settings
    log_level: int = logging.INFO


CFG = Config()

# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def setup_logging(level: int = logging.INFO):
    """Configura el sistema de logging."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


class ExtractorError(Exception):
    """Excepción personalizada para errores del extractor."""


def get_api_key() -> str:
    """Obtiene la API key desde variable de entorno o usa la por defecto."""
    api_key = os.getenv(CFG.api_key_env)
    if not api_key:
        logging.warning(f"Variable {CFG.api_key_env} no encontrada, usando clave por defecto")
        return CFG.api_key_default
    return api_key


def create_session(api_key: str) -> requests.Session:
    """Crea una sesión HTTP con autenticación configurada."""
    session = requests.Session()
    session.headers.update({
        "accept": "application/json",
        "authorization": f"Basic {api_key}"
    })
    return session


def safe_request(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """Realiza una petición HTTP con reintentos y manejo de errores."""
    for attempt in range(1, CFG.max_retries + 1):
        try:
            response = session.get(url, params=params, timeout=CFG.timeout_seconds)
            response.raise_for_status()
            
            if not response.text.strip():
                logging.warning("Respuesta vacía recibida")
                return None
                
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if attempt == CFG.max_retries:
                raise ExtractorError(f"Error en petición después de {CFG.max_retries} intentos: {e}")
            
            wait_time = CFG.backoff_factor ** attempt
            logging.warning(f"Intento {attempt} falló ({e}). Reintentando en {wait_time:.1f}s...")
            time.sleep(wait_time)


# ---------------------------------------------------------------------------
# Manejo de archivos CSV
# ---------------------------------------------------------------------------

def get_last_bill_id_from_csv() -> Optional[int]:
    """Obtiene el ID de la última factura de proveedor desde el archivo CSV."""
    csv_path = Path(CFG.workspace_dir) / CFG.csv_filename
    
    if not csv_path.exists():
        logging.info(f"Archivo {CFG.csv_filename} no encontrado. Primera ejecución.")
        return None
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            return None
        
        # Para facturas de proveedor, necesitamos el ID más alto entre las columnas 'id' que representan bill_id
        last_bill_id = df.groupby(['fecha', 'totalFact', 'proveedor'])['id'].first().max()
        logging.info(f"Último bill ID en CSV: {last_bill_id}")
        return int(last_bill_id)
        
    except Exception as e:
        logging.error(f"Error leyendo CSV: {e}")
        return None


def save_to_csv(df: pd.DataFrame, append: bool = True):
    """Guarda el DataFrame en CSV."""
    csv_path = Path(CFG.workspace_dir) / CFG.csv_filename
    
    try:
        if append and csv_path.exists():
            # Leer CSV existente y concatenar
            existing_df = pd.read_csv(csv_path)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_csv(csv_path, index=False)
            logging.info(f"Agregadas {len(df)} filas al CSV existente")
        else:
            # Crear nuevo CSV
            df.to_csv(csv_path, index=False)
            logging.info(f"Creado nuevo CSV con {len(df)} filas")
            
    except Exception as e:
        logging.error(f"Error guardando CSV: {e}")
        raise ExtractorError(f"No se pudo guardar en CSV: {e}")


# ---------------------------------------------------------------------------
# Descarga de facturas de proveedor
# ---------------------------------------------------------------------------

def get_total_bills(session: requests.Session) -> int:
    """Obtiene el total de facturas de proveedor disponibles."""
    params = {
        "metadata": "true",
        "limit": 1,
        "type": "bill"
    }
    
    data = safe_request(session, CFG.base_url, params)
    if not data or "metadata" not in data:
        raise ExtractorError("No se pudo obtener el total de facturas de proveedor")
    
    total = data["metadata"]["total"]
    logging.info(f"Total de facturas de proveedor disponibles: {total}")
    return total


def get_bill_range_to_process(session: requests.Session) -> Tuple[int, int]:
    """Determina el rango de facturas a procesar."""
    last_bill_id = get_last_bill_id_from_csv()
    total_bills = get_total_bills(session)
    
    if last_bill_id is None:
        # Primera ejecución: obtener la primera factura
        params = {
            "start": 0,
            "order_field": "date",
            "type": "bill",
            "limit": 1
        }
        
        data = safe_request(session, CFG.base_url, params)
        if not data:
            raise ExtractorError("No se encontraron facturas de proveedor")
        
        start_id = int(data[0]['id'])
        logging.info(f"Primera ejecución. Comenzando desde bill ID: {start_id}")
    else:
        start_id = last_bill_id
        logging.info(f"Continuando desde bill ID: {start_id}")
    
    # Obtener la última factura disponible
    params = {
        "start": 0,
        "order_field": "date",
        "order_direction": "DESC",
        "type": "bill",
        "limit": 1
    }
    
    data = safe_request(session, CFG.base_url, params)
    if not data:
        end_id = start_id
    else:
        end_id = int(data[0]['id'])
    
    logging.info(f"Rango de procesamiento: {start_id} - {end_id}")
    return start_id, end_id


def fetch_bills_batch(session: requests.Session, start_id: int, end_id: int) -> List[Dict[str, Any]]:
    """Descarga facturas de proveedor en lotes."""
    all_bills = []
    current_start = 0
    
    while True:
        params = {
            "start": current_start,
            "limit": CFG.page_size,
            "order_field": "date",
            "type": "bill"
        }
        
        logging.info(f"Descargando lote desde posición {current_start}")
        data = safe_request(session, CFG.base_url, params)
        
        if not data:
            logging.info("No hay más datos disponibles")
            break
        
        # Filtrar solo facturas en el rango deseado y que son nuevas
        batch_bills = []
        for bill in data:
            bill_id = int(bill.get('id', 0))
            if bill_id > start_id and bill_id <= end_id:
                batch_bills.append(bill)
        
        if batch_bills:
            all_bills.extend(batch_bills)
        
        # Si el lote es menor que el tamaño de página, hemos terminado
        if len(data) < CFG.page_size:
            break
        
        current_start += CFG.page_size
    
    logging.info(f"Descargadas {len(all_bills)} facturas de proveedor nuevas")
    return all_bills


# ---------------------------------------------------------------------------
# Procesamiento de datos
# ---------------------------------------------------------------------------

def clean_bill_data(bill: Dict[str, Any]) -> Dict[str, Any]:
    """Limpia y normaliza los datos de una factura de proveedor."""
    # Extraer información del proveedor
    provider_data = bill.get('provider', {})
    provider_name = provider_data.get('name', 'Sin proveedor') if provider_data else 'Sin proveedor'
    
    return {
        'bill_id': int(bill.get('id', 0)),
        'date': bill.get('date', ''),
        'total': float(bill.get('total', 0)),
        'provider': provider_name,
        'purchases': bill.get('purchases', {})
    }


def flatten_bill_items(bills: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convierte las facturas de proveedor con ítems anidados en un DataFrame plano."""
    records = []
    
    for bill in bills:
        cleaned_bill = clean_bill_data(bill)
        purchases = cleaned_bill['purchases']
        
        # Verificar si hay ítems en purchases
        if not purchases or 'items' not in purchases:
            logging.debug(f"Factura de proveedor {cleaned_bill['bill_id']} no tiene ítems")
            continue
        
        items = purchases['items']
        if not items:
            logging.debug(f"Factura de proveedor {cleaned_bill['bill_id']} tiene purchases vacíos")
            continue
        
        for item in items:
            record = {
                'id': item.get('id', ''),  # Item ID del proveedor
                'fecha': cleaned_bill['date'],
                'nombre': item.get('name', ''),
                'cantidad': int(item.get('quantity', 1)),
                'precio': float(item.get('price', 0)),
                'total': float(item.get('total', 0)),
                'totalFact': cleaned_bill['total'],
                'proveedor': cleaned_bill['provider']
            }
            records.append(record)
    
    df = pd.DataFrame(records)
    logging.info(f"Procesados {len(records)} ítems de {len(bills)} facturas de proveedor")
    return df


# ---------------------------------------------------------------------------
# Base de datos PostgreSQL
# ---------------------------------------------------------------------------

def get_database_engine():
    """Crea el engine de SQLAlchemy para PostgreSQL."""
    if not SQLALCHEMY_AVAILABLE:
        return None
    
    db_url = os.getenv(CFG.db_url_env)
    if not db_url:
        logging.warning(f"Variable {CFG.db_url_env} no encontrada. Solo se guardará en CSV.")
        return None
    
    try:
        engine = create_engine(db_url)
        # Probar conexión
        with engine.connect():
            pass
        logging.info("Conexión a PostgreSQL establecida")
        return engine
    except Exception as e:
        logging.error(f"Error conectando a PostgreSQL: {e}")
        return None


def save_to_database(df: pd.DataFrame, engine):
    """Guarda el DataFrame en PostgreSQL con tipos de datos optimizados."""
    if engine is None or df.empty:
        return
    
    # Mapeo de tipos de columna para optimización
    dtype_mapping = {
        'id': sa_types.String(length=50),  # Item ID puede ser string
        'fecha': sa_types.DATE(),
        'nombre': sa_types.String(length=300),  # Nombres de productos pueden ser largos
        'cantidad': sa_types.INTEGER(),
        'precio': sa_types.NUMERIC(precision=12, scale=2),
        'total': sa_types.NUMERIC(precision=12, scale=2),
        'totalFact': sa_types.NUMERIC(precision=12, scale=2),
        'proveedor': sa_types.String(length=200)
    }
    
    try:
        df.to_sql(
            CFG.db_table_name,
            engine,
            if_exists="append",
            index=False,
            dtype=dtype_mapping
        )
        logging.info(f"Guardadas {len(df)} filas en PostgreSQL (tabla {CFG.db_table_name})")
    except Exception as e:
        logging.error(f"Error guardando en PostgreSQL: {e}")


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def main():
    """Función principal del extractor de facturas de proveedor."""
    setup_logging(CFG.log_level)
    logging.info("Iniciando extractor de facturas de proveedor optimizado")
    
    try:
        # Configurar sesión HTTP
        api_key = get_api_key()
        session = create_session(api_key)
        
        # Obtener rango de facturas a procesar
        start_id, end_id = get_bill_range_to_process(session)
        
        if start_id >= end_id:
            logging.info("No hay facturas de proveedor nuevas para procesar")
            return
        
        # Descargar facturas de proveedor
        bills = fetch_bills_batch(session, start_id, end_id)
        
        if not bills:
            logging.info("No se descargaron facturas de proveedor nuevas")
            return
        
        # Procesar datos
        df = flatten_bill_items(bills)
        
        if df.empty:
            logging.info("No se generaron ítems para procesar")
            return
        
        # Guardar en CSV
        save_to_csv(df, append=True)
        
        # Intentar guardar en PostgreSQL
        engine = get_database_engine()
        if engine:
            save_to_database(df, engine)
        
        logging.info(f"Proceso completado exitosamente. Procesados {len(df)} ítems.")
        
    except ExtractorError as e:
        logging.error(f"Error del extractor: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error inesperado: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    load_dotenv()
    main() 