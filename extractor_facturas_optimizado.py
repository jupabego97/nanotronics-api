#!/usr/bin/env python3
"""
Extractor optimizado de facturas (invoices) desde la API de Alegra
-----------------------------------------------------------------

▶ **Funcionalidades principales:**
   1. Extrae facturas de venta (invoices) desde la API de Alegra
   2. Procesa y aplana los ítems de cada factura
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
    """Configuración centralizada del extractor."""
    
    # API Configuration
    base_url: str = "https://api.alegra.com/api/v1/invoices"
    api_key_env: str = "ALEGRA_API_KEY"
    api_key_default: str = "bmFub3Ryb25pY3NhbHNvbmRlbGF0ZWNub2xvZ2lhQGdtYWlsLmNvbTphMmM4OTA3YjE1M2VmYTc0ODE5ZA=="
    
    # Request settings
    page_size: int = 30
    max_retries: int = 3
    backoff_factor: float = 1.5
    timeout_seconds: int = 30
    
    # File settings
    csv_filename: str = "facturas.csv"
    workspace_dir: str = "."
    
    # Database settings
    db_url_env: str = "DATABASE_URL"
    db_table_name: str = "facturas"
    
    # Processing settings
    days_buffer: int = 3  # Días hacia atrás para buscar facturas recientes
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


def safe_request(session: requests.Session, url: str, params: Dict[str, Any] = None) -> Any:
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

def get_last_invoice_id_from_csv() -> Optional[int]:
    """Obtiene el ID de la última factura desde el archivo CSV."""
    csv_path = Path(CFG.workspace_dir) / CFG.csv_filename
    
    if not csv_path.exists():
        logging.info(f"Archivo {CFG.csv_filename} no encontrado. Primera ejecución.")
        return None
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            return None
        
        last_id = int(df['id'].max())
        logging.info(f"Último ID en CSV: {last_id}")
        return last_id
        
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
# Descarga de facturas
# ---------------------------------------------------------------------------

def get_initial_invoice_range(session: requests.Session, start_date: str = None) -> Tuple[int, int]:
    """Obtiene el rango de IDs de facturas para procesar."""
    last_id_csv = get_last_invoice_id_from_csv()
    
    if last_id_csv is not None:
        # Hay datos previos, buscar desde la última ID
        start_id = last_id_csv
        logging.info(f"Continuando desde ID {start_id}")
    else:
        # Primera ejecución, pedir fecha inicial o usar fecha por defecto
        if start_date is None:
            start_date = input("Ingrese la fecha inicial (YYYY-MM-DD) o presione Enter para últimos 30 días: ").strip()
            if not start_date:
                start_date = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        logging.info(f"Primera ejecución, buscando desde {start_date}")
        
        # Obtener primera factura de esa fecha
        params = {
            "order_direction": "ASC",
            "date_afterOrNow": start_date,
            "limit": 1
        }
        
        data = safe_request(session, CFG.base_url, params)
        if not data or not data:
            raise ExtractorError(f"No se encontraron facturas desde {start_date}")
        
        start_id = int(data[0]['id'])
        logging.info(f"Primera factura encontrada: ID {start_id}")
    
    # Obtener última factura disponible (últimos días)
    recent_date = (date.today() - timedelta(days=CFG.days_buffer)).strftime("%Y-%m-%d")
    params = {
        "date_afterOrNow": recent_date,
        "limit": 1,
        "order_direction": "DESC"
    }
    
    data = safe_request(session, CFG.base_url, params)
    if not data:
        logging.warning("No se encontraron facturas recientes")
        end_id = start_id
    else:
        end_id = int(data[0]['id'])
    
    logging.info(f"Rango de procesamiento: {start_id} - {end_id}")
    return start_id, end_id


def fetch_invoices_batch(session: requests.Session, start_id: int, end_id: int) -> List[Dict[str, Any]]:
    """Descarga facturas en lotes desde start_id hasta end_id."""
    all_invoices = []
    current_start = start_id
    
    while current_start <= end_id:
        params = {
            "start": current_start,
            "limit": CFG.page_size,
            "order_direction": "ASC",
            "order_field": "id"
        }
        
        logging.info(f"Descargando lote desde ID {current_start}")
        data = safe_request(session, CFG.base_url, params)
        
        if not data:
            logging.info("No hay más datos disponibles")
            break
        
        # Filtrar solo facturas en el rango deseado
        batch_invoices = [inv for inv in data if start_id < int(inv.get('id', 0)) <= end_id]
        
        if not batch_invoices:
            logging.info("No hay facturas nuevas en este lote")
            break
        
        all_invoices.extend(batch_invoices)
        
        # Si el lote es menor que el tamaño de página, hemos terminado
        if len(data) < CFG.page_size:
            break
        
        current_start += CFG.page_size
    
    logging.info(f"Descargadas {len(all_invoices)} facturas nuevas")
    return all_invoices


# ---------------------------------------------------------------------------
# Procesamiento de datos
# ---------------------------------------------------------------------------

def clean_invoice_data(invoice: Dict[str, Any]) -> Dict[str, Any]:
    """Limpia y normaliza los datos de una factura."""
    # Extraer información del cliente
    client_data = invoice.get('client', {})
    client_name = client_data.get('name', 'Sin cliente') if client_data else 'Sin cliente'
    
    # Extraer información del vendedor
    seller_data = invoice.get('seller', {})
    seller_name = seller_data.get('name', 'No se ha registrado un vendedor') if seller_data else 'No se ha registrado un vendedor'
    
    return {
        'id': int(invoice.get('id', 0)),
        'date': invoice.get('date', ''),
        'datetime': invoice.get('datetime', ''),
        'client': client_name,
        'totalPaid': float(invoice.get('totalPaid', 0)),
        'paymentMethod': invoice.get('paymentMethod', ''),
        'seller': seller_name,
        'items': invoice.get('items', [])
    }


def flatten_invoice_items(invoices: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convierte las facturas con ítems anidados en un DataFrame plano."""
    records = []
    
    for invoice in invoices:
        cleaned_invoice = clean_invoice_data(invoice)
        items = cleaned_invoice['items']
        
        if not items:
            logging.debug(f"Factura {cleaned_invoice['id']} no tiene ítems")
            continue
        
        for item in items:
            # Limpiar datos del ítem, removiendo campos innecesarios
            clean_item = {k: v for k, v in item.items() 
                         if k not in ['description', 'discount', 'productKey', 
                                    'unit', 'tax', 'reference', 'id']}
            
            record = {
                'id': cleaned_invoice['id'],
                'fecha': cleaned_invoice['date'],
                'hora': cleaned_invoice['datetime'],
                'nombre': clean_item.get('name', ''),
                'precio': float(clean_item.get('price', 0)),
                'cantidad': int(clean_item.get('quantity', 1)),
                'total': float(clean_item.get('total', 0)),
                'cliente': cleaned_invoice['client'],
                'totalfact': cleaned_invoice['totalPaid'],
                'metodo': cleaned_invoice['paymentMethod'],
                'vendedor': cleaned_invoice['seller']
            }
            records.append(record)
    
    df = pd.DataFrame(records)
    logging.info(f"Procesados {len(records)} ítems de {len(invoices)} facturas")
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
        'id': sa_types.INTEGER(),
        'fecha': sa_types.DATE(),
        'hora': sa_types.TIMESTAMP(),
        'nombre': sa_types.String(length=200),
        'precio': sa_types.NUMERIC(precision=10, scale=2),
        'cantidad': sa_types.INTEGER(),
        'total': sa_types.NUMERIC(precision=10, scale=2),
        'cliente': sa_types.String(length=200),
        'totalfact': sa_types.NUMERIC(precision=10, scale=2),
        'metodo': sa_types.String(length=50),
        'vendedor': sa_types.String(length=100)
    }
    
    try:
        df.to_sql(
            CFG.db_table_name,
            engine,
            if_exists="append",
            index=False,
            dtype=dtype_mapping
        )
        logging.info(f"Guardadas {len(df)} filas en PostgreSQL")
    except Exception as e:
        logging.error(f"Error guardando en PostgreSQL: {e}")


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def main(start_date: str = None):
    """Función principal del extractor."""
    setup_logging(CFG.log_level)
    logging.info("Iniciando extractor de facturas optimizado")
    
    try:
        # Configurar sesión HTTP
        api_key = get_api_key()
        session = create_session(api_key)
        
        # Obtener rango de facturas a procesar
        start_id, end_id = get_initial_invoice_range(session, start_date)
        
        if start_id >= end_id:
            logging.info("No hay facturas nuevas para procesar")
            return
        
        # Descargar facturas
        invoices = fetch_invoices_batch(session, start_id, end_id)
        
        if not invoices:
            logging.info("No se descargaron facturas nuevas")
            return
        
        # Procesar datos
        df = flatten_invoice_items(invoices)
        
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
    
    # Permitir fecha inicial como argumento
    start_date = None
    if len(sys.argv) > 1:
        start_date = sys.argv[1]
    
    main(start_date) 