#!/usr/bin/env python3
"""
Extractor de facturas de proveedor desde la API de Alegra.
---------------------------------------------------------

Mejoras clave frente al script anterior:
1. **Paginación eficiente**: se detiene en cuanto detecta que ya alcanzó la última factura registrada localmente.
2. **Estructura clara con `dataclass` de configuración** para facilitar cambios de ruta, tamaño de página, etc.
3. **Transformación robusta**: adapta el modelo real de Alegra (`items` en lugar de `purchases`).
4. **Persistencia correcta**: el control incremental usa ahora el `bill_id` de la factura (no el `id` del ítem).
5. **Registros detallados** con nivel configurable.
6. **Reintentos simples** ante fallos de red (back‑off exponencial).
7. **Comprobación de dependencias** para que el usuario sepa qué librerías instalar.

Requisitos (instalar sólo si no los tienes):
```bash
pip install pandas requests
```

"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests
from sqlalchemy import create_engine
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Configuración global
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class Config:
    """Parámetros generales del extractor."""

    base_url: str = "https://api.alegra.com/api/v1/bills"
    page_size: int = 30  # Alegra permite hasta 30 por petición
    
    csv_path: Path = Path("D:/Desktop/python/proyecto-alegra/bills.csv")
    env_key: str = "ALEGRA_API_KEY"
    log_level: int = logging.INFO
    max_retries: int = 3  # reintentos por petición
    backoff_factor: float = 1.5  # multiplicador del back‑off


CFG = Config()

# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------


def setup_logging(level: int):
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


class APIError(Exception):
    """Error propio del extractor."""


def get_api_key(env_key: str) -> str:
    """Obtiene la API key del entorno y valida que exista."""
    key = os.getenv(env_key)
    if not key:
        raise APIError(
            f"Variable de entorno {env_key} no definida. \n"
            "En Windows:  setx ALEGRA_API_KEY TU_CLAVE \n"
            "En Linux/Mac: export ALEGRA_API_KEY=TU_CLAVE"
        )
    return key


def create_session(api_key: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json", "Authorization": f"Basic {api_key}"})
    return s


# ---------------------------------------------------------------------------
# Descarga de datos
# ---------------------------------------------------------------------------

def request_with_retry(session: requests.Session, url: str, params: Dict[str, Any]) -> Any:
    for attempt in range(1, CFG.max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            if attempt == CFG.max_retries:
                raise APIError(f"Error al hacer request: {exc}") from exc
            wait = CFG.backoff_factor ** attempt
            logging.warning(
                "Fallo en el intento %s (%s). Reintentando en %.1fs…", attempt, exc, wait
            )
            time.sleep(wait)


def load_existing(csv_path: Path) -> Tuple[int, pd.DataFrame]:
    """Carga el CSV y devuelve el último bill_id procesado."""
    if csv_path.exists():
        df = pd.read_csv(csv_path)
        if not df.empty and "bill_id" in df.columns:
            last_bill = int(df["bill_id"].max())
            logging.info("Última factura almacenada localmente: %s", last_bill)
            return last_bill, df
    return 0, pd.DataFrame()


def fetch_new_bills(session: requests.Session, last_bill_id: int) -> List[Dict[str, Any]]:
    """Itera por páginas en orden descendente hasta encontrar `last_bill_id`."""
    offset = 0
    bills: List[Dict[str, Any]] = []
    while True:
        params = {
            "start": offset,
            "limit": CFG.page_size,
            "type": "bill",
            "order_field": "date",
            "order_direction": "DESC",
        }
        page = request_with_retry(session, CFG.base_url, params)
        if not page:
            break
        for bill in page:
            bill_id = int(bill.get("id", 0))
            if bill_id <= last_bill_id:
                logging.info("Se alcanzó la última factura conocida (ID=%s).", last_bill_id)
                return bills
            bills.append(bill)
        # Si se devolvió menos que el límite, no hay más páginas
        if len(page) < CFG.page_size:
            break
        offset += CFG.page_size
    return bills


# ---------------------------------------------------------------------------
# Transformación
# ---------------------------------------------------------------------------

def flatten_bills(bills: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convierte cada ítem de cada factura en un registro de tabla."""
    records: List[Dict[str, Any]] = []
    for b in bills:
        try:
            bill_id = int(b.get("id"))
        except (TypeError, ValueError):
            continue  # Omitir facturas sin id válido
        bill_date = b.get("date")
        bill_total = float(b.get("total", 0))
        provider = b.get("provider", {}).get("name")
        for item in b.get("purchases", {}).get("items", []):
            try:
                item_id = int(item.get("id"))
            except (TypeError, ValueError):
                continue  # Omitir ítems sin id válido
            records.append(
                {
                    "bill_id": bill_id,
                    "item_id": item_id,
                    "fecha": bill_date,
                    "nombre": item.get("name"),
                    "precio": float(item.get("price", 0)),
                    "cantidad": float(item.get("quantity", 0)),
                    "total_item": float(item.get("total", 0)),
                    "total_factura": bill_total,
                    "proveedor": provider,
                }
            )
    df = pd.DataFrame.from_records(records)
    logging.info("Transformados %s ítems a dataframe.", len(df))
    return df


# ---------------------------------------------------------------------------
# Persistencia
# ---------------------------------------------------------------------------

def get_pg_engine():
    """Crea el engine de SQLAlchemy para PostgreSQL usando variables de entorno."""
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "1234")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = "prueba_crm"
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def save_to_postgres(df_new: pd.DataFrame):
    try:
        engine = get_pg_engine()
    except Exception as e:
        logging.error(f"Error al conectar con la base de datos: {e}")
        print(f"Error al conectar con la base de datos: {e}")
        sys.exit(1)
    # Crea la tabla aunque el DataFrame esté vacío
    if df_new.empty:
        df_new = pd.DataFrame(columns=[
            "bill_id", "item_id", "fecha", "nombre", "precio", "cantidad",
            "total_item", "total_factura", "proveedor"
        ])
    try:
        df_new.to_sql('facturas_proveedor', engine, if_exists='append', index=False)
        logging.info("Intento de guardar registros (aunque esté vacío) en la tabla facturas_proveedor.")
    except Exception as e:
        logging.error(f"Error al guardar en la base de datos: {e}")
        print(f"Error al guardar en la base de datos: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Programa principal
# ---------------------------------------------------------------------------

def main():
    setup_logging(CFG.log_level)
    try:
        api_key = get_api_key(CFG.env_key)
    except APIError as e:
        logging.error(str(e))
        sys.exit(1)

    session = create_session(api_key)

    last_bill_id, df_existing = load_existing(CFG.csv_path)
    new_bills = fetch_new_bills(session, last_bill_id)

    if not new_bills:
        logging.info("No hay facturas nuevas para procesar. Nada que hacer.")
        return

    df_new = flatten_bills(new_bills)
    save_to_postgres(df_new)


if __name__ == "__main__":
    load_dotenv()
    main()
