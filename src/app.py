import os
import json
import logging
import tempfile
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from google.cloud import storage
from google.cloud import bigquery


# =========================
# Logging
# =========================
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# =========================
# Environment / Config
# =========================
GCP_PROJECT = os.environ["GCP_PROJECT"]
GCS_BUCKET = os.environ["GCS_BUCKET"]
BQ_DATASET = os.environ["BQ_DATASET"]

GCS_BASE_PREFIX = os.getenv("GCS_BASE_PREFIX", "caddis_batch")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

CADDIS_BASE_URL = os.getenv("CADDIS_BASE_URL", "https://www.caddis.com.ar/api/v1/").rstrip("/") + "/"
CADDIS_LOGIN_URL = os.getenv("CADDIS_LOGIN_URL", "https://www.caddis.com.ar/api/v1/login")
CADDIS_USER = os.environ["CADDIS_USER"]
CADDIS_PASSWORD = os.environ["CADDIS_PASSWORD"]

COMPANY_CODE = os.getenv("COMPANY_CODE", "1")
MODE = os.getenv("MODE", "auto").lower()
DATE_FROM = os.getenv("DATE_FROM")
DATE_TO = os.getenv("DATE_TO")
TZ_OFFSET = os.getenv("TZ_OFFSET", "-03:00")

API_TIMEOUT = int(os.getenv("API_TIMEOUT", "120"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "5000"))

RUN_ID = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

TABLES = {
    "ventas_header": "ventas_header_raw",
    "ventas_items": "ventas_items_raw",
    "dim_vendedores": "dim_vendedores_raw",
    "dim_puntos_venta": "dim_puntos_venta_raw",
    "clientes": "clientes_raw",
    "dim_productos": "dim_productos_raw",
    "stock_diario": "stock_diario_raw",
}

FOLDERS = {
    "ventas_header": "daily/ventas_header",
    "ventas_items": "daily/ventas_items",
    "dim_vendedores": "daily/dim_vendedores",
    "dim_puntos_venta": "daily/dim_puntos_venta",
    "clientes": "daily/clientes",
    "dim_productos": "daily/dim_productos",
    "stock_diario": "daily/stock_diario",
}

storage_client = storage.Client(project=GCP_PROJECT)
bq_client = bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)


# =========================
# Helpers - Dates
# =========================
def _today_local() -> dt.date:
    sign = -1 if TZ_OFFSET.startswith("-") else 1
    hh, mm = TZ_OFFSET[1:].split(":")
    delta = dt.timedelta(hours=int(hh) * sign, minutes=int(mm) * sign)
    return (dt.datetime.utcnow() + delta).date()


def _resolve_date_range() -> Tuple[str, str, dt.date]:
    if MODE == "manual" and DATE_FROM and DATE_TO:
        date_from = dt.date.fromisoformat(DATE_FROM)
        date_to = dt.date.fromisoformat(DATE_TO)
    else:
        yesterday = _today_local() - dt.timedelta(days=1)
        date_from = yesterday
        date_to = yesterday

    fecha_desde = f"{date_from.isoformat()}T00:00:00{TZ_OFFSET}"
    fecha_hasta = f"{date_to.isoformat()}T23:59:59{TZ_OFFSET}"
    return fecha_desde, fecha_hasta, date_to


# =========================
# Helpers - API
# =========================
def get_access_token() -> str:
    logger.info("Getting fresh Caddis token...")
    payload = {
        "usuario": CADDIS_USER,
        "password": CADDIS_PASSWORD,
    }
    response = requests.post(CADDIS_LOGIN_URL, json=payload, timeout=API_TIMEOUT)
    response.raise_for_status()

    data = response.json()

    for key in ("token", "access_token", "jwt", "bearer"):
        if isinstance(data.get(key), str) and data.get(key).strip():
            return data[key].strip()

    body = data.get("body")
    if isinstance(body, dict):
        for key in ("token", "access_token", "jwt", "bearer"):
            if isinstance(body.get(key), str) and body.get(key).strip():
                return body[key].strip()

    raise RuntimeError(f"Could not find token in login response: {data}")


def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _build_url(path: str, params: Optional[Dict[str, Any]] = None) -> str:
    path = path.lstrip("/")
    url = CADDIS_BASE_URL + path
    if params:
        url += "?" + urlencode(params, doseq=True)
    return url


def fetch_json(url: str, token: str) -> Dict[str, Any]:
    response = requests.get(url, headers=_headers(token), timeout=API_TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_ventas(token: str, fecha_desde: str, fecha_hasta: str) -> List[Dict[str, Any]]:
    logger.info("Fetching ventas from %s to %s", fecha_desde, fecha_hasta)

    page = 1
    all_rows: List[Dict[str, Any]] = []

    while True:
        params = {
            "fecha_desde": fecha_desde,
            "fecha_hasta": fecha_hasta,
            "pagina": page,
            "limite": PAGE_SIZE,
        }
        url = _build_url("ventas", params)
        payload = fetch_json(url, token)

        body = payload.get("body", [])
        if not isinstance(body, list):
            body = []

        all_rows.extend(body)

        pag = (payload.get("head") or {}).get("paginacion") or {}
        total = int(pag.get("total") or 0)
        limit = int(pag.get("limite") or PAGE_SIZE)
        current_page = int(pag.get("pagina") or page)

        logger.info("Ventas page %s fetched: %s rows", current_page, len(body))

        if total == 0:
            break

        total_pages = (total + limit - 1) // limit
        if current_page >= total_pages:
            break

        page += 1

    logger.info("Total ventas fetched: %s", len(all_rows))
    return all_rows


def fetch_paginated_endpoint(token: str, path: str) -> List[Dict[str, Any]]:
    logger.info("Fetching paginated endpoint: %s", path)

    page = 1
    all_rows: List[Dict[str, Any]] = []

    while True:
        params = {
            "pagina": page,
            "limite": PAGE_SIZE,
        }
        url = _build_url(path, params)

        try:
            payload = fetch_json(url, token)
        except requests.exceptions.HTTPError as e:
            status = None
            if getattr(e, "response", None) is not None:
                status = e.response.status_code

            # En Caddis, algunos endpoints paginados devuelven 404
            # cuando la página pedida ya no existe.
            if status == 404:
                logger.info("%s page %s returned 404 -> end of pagination", path, page)
                break

            raise

        body = payload.get("body", [])
        if not isinstance(body, list):
            body = []

        if not body:
            logger.info("%s page %s returned empty body -> end of pagination", path, page)
            break

        all_rows.extend(body)
        logger.info("%s page %s fetched: %s rows", path, page, len(body))
        page += 1

    logger.info("Total rows fetched for %s: %s", path, len(all_rows))
    return all_rows


# =========================
# Helpers - Normalization
# =========================
def _safe_round(value: Any, decimals: int = 2) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return round(float(value), decimals)
    except Exception:
        return None


def _to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text != "" else None


def _company_code() -> str:
    return COMPANY_CODE


# =========================
# Entity builders from ventas
# =========================
def build_ventas_header(ventas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    processed_at = dt.datetime.utcnow().isoformat() + "Z"

    for v in ventas:
        rows.append({
            "empresa": _company_code(),
            "venta_id": _to_str(v.get("id")),
            "fecha_creacion": _to_str(v.get("fecha_creacion")),
            "fecha_modificacion": _to_str(v.get("fecha_modificacion")),
            "usuario": _to_str(v.get("usuario")),
            "comprobante_tipo": _to_str(v.get("comprobante_tipo")),
            "comprobante_numero": _to_str(v.get("comprobante_numero")),
            "comprobante_asociado": _to_str(v.get("comprobante_asociado")),
            "total_neto": _safe_round(v.get("total_neto")),
            "total_iva": _safe_round(v.get("total_iva")),
            "total_percepciones": _safe_round(v.get("total_percepciones")),
            "total_impuestos_internos": _safe_round(v.get("total_impuestos_internos")),
            "total": _safe_round(v.get("total")),
            "_run_id": RUN_ID,
            "_processed_at": processed_at,
        })
    return rows


def build_ventas_items(ventas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    processed_at = dt.datetime.utcnow().isoformat() + "Z"

    for v in ventas:
        venta_id = _to_str(v.get("id"))
        fecha_creacion = _to_str(v.get("fecha_creacion"))
        deposito = v.get("deposito") or {}
        vendedor = v.get("vendedor") or {}

        for item in v.get("items", []) or []:
            articulo = item.get("articulo") or {}
            rows.append({
                "empresa": _company_code(),
                "venta_id": venta_id,
                "fecha_creacion": fecha_creacion,
                "item_id": _to_str(item.get("id")),
                "articulo_id": _to_str(articulo.get("id")),
                "sku": _to_str(articulo.get("sku")),
                "articulo_nombre": _to_str(articulo.get("nombre")),
                "articulo_nse": _to_str(articulo.get("nse")),
                "cantidad": item.get("cantidad"),
                "precio_unitario": _safe_round(item.get("precio_unitario")),
                "promo": _to_str(item.get("promo")),
                "voucher": _to_str(item.get("voucher")),
                "descuento_item": _safe_round(item.get("descuento_item")),
                "descuento_general": _safe_round(item.get("descuento_general")),
                "total_unitario": _safe_round(item.get("total_unitario")),
                "descuento_pago": _safe_round(item.get("descuento_pago")),
                "recargo_pago": _safe_round(item.get("recargo_pago")),
                "total_neto": _safe_round(item.get("total_neto")),
                "total_impuestos": _safe_round(item.get("total_impuestos")),
                "total": _safe_round(item.get("total")),
                "costo_unitario": _safe_round(item.get("costo_unitario")),
                "costo_total": _safe_round(item.get("costo_total")),
                "vendedor_id": _to_str(vendedor.get("id")),
                "vendedor_nombre": _to_str(vendedor.get("nombre")),
                "deposito_id": _to_str(deposito.get("id")),
                "deposito_nombre": _to_str(deposito.get("nombre")),
                "_run_id": RUN_ID,
                "_processed_at": processed_at,
            })
    return rows


def build_dim_vendedores(ventas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    processed_at = dt.datetime.utcnow().isoformat() + "Z"

    for v in ventas:
        vendedor = v.get("vendedor") or {}
        vendedor_id = _to_str(vendedor.get("id"))
        vendedor_nombre = _to_str(vendedor.get("nombre"))

        if not vendedor_id and not vendedor_nombre:
            continue

        key = (_company_code(), vendedor_id or vendedor_nombre)
        dedup[key] = {
            "empresa": _company_code(),
            "vendedor_id": vendedor_id,
            "vendedor_nombre": vendedor_nombre,
            "_run_id": RUN_ID,
            "_processed_at": processed_at,
        }

    return list(dedup.values())


def build_dim_puntos_venta(ventas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    processed_at = dt.datetime.utcnow().isoformat() + "Z"

    for v in ventas:
        deposito = v.get("deposito") or {}
        deposito_id = _to_str(deposito.get("id"))
        deposito_nombre = _to_str(deposito.get("nombre"))

        if not deposito_id and not deposito_nombre:
            continue

        key = (_company_code(), deposito_id or deposito_nombre)
        dedup[key] = {
            "empresa": _company_code(),
            "deposito_id": deposito_id,
            "deposito_nombre": deposito_nombre,
            "_run_id": RUN_ID,
            "_processed_at": processed_at,
        }

    return list(dedup.values())


def build_clientes(ventas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    dedup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    processed_at = dt.datetime.utcnow().isoformat() + "Z"

    for v in ventas:
        cliente = v.get("cliente") or {}
        iva = cliente.get("iva") or {}
        iibb = cliente.get("iibb") or {}

        cliente_id = _to_str(cliente.get("id"))
        if not cliente_id:
            continue

        key = (_company_code(), cliente_id)
        dedup[key] = {
            "empresa": _company_code(),
            "cliente_id": cliente_id,
            "cliente_nombre": _to_str(cliente.get("nombre")),
            "cliente_documento_tipo": _to_str(cliente.get("documento_tipo")),
            "cliente_documento": _to_str(cliente.get("documento")),
            "cliente_iva_codigo": _to_str(iva.get("codigo")),
            "cliente_iva_nombre": _to_str(iva.get("nombre")),
            "cliente_iibb_codigo": _to_str(iibb.get("codigo")),
            "cliente_iibb_nombre": _to_str(iibb.get("nombre")),
            "_run_id": RUN_ID,
            "_processed_at": processed_at,
        }

    return list(dedup.values())


# =========================
# Entity builders from articulos
# =========================
def build_dim_productos(articulos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    processed_at = dt.datetime.utcnow().isoformat() + "Z"

    for a in articulos:
        rows.append({
            "empresa": _company_code(),
            "articulo_id": _to_str(a.get("id")),
            "fecha_creacion": _to_str(a.get("fecha_creacion")),
            "fecha_modificacion": _to_str(a.get("fecha_modificacion")),
            "sku": _to_str(a.get("sku")),
            "nombre": _to_str(a.get("nombre")),
            "estado": _to_str(a.get("estado")),
            "ean": _to_str(a.get("ean")),
            "tipo": _to_str(a.get("tipo")),
            "marca": _to_str(a.get("marca")),
            "grupo": _to_str(a.get("grupo")),
            "nse": _to_str(a.get("nse")),
            "stock_seguridad": _safe_round(a.get("stock_seguridad")),
            "peso": _safe_round(a.get("peso")),
            "alto": _safe_round(a.get("alto")),
            "ancho": _safe_round(a.get("ancho")),
            "profundidad": _safe_round(a.get("profundidad")),
            "_run_id": RUN_ID,
            "_processed_at": processed_at,
        })

    return rows


# =========================
# Entity builders from stock
# =========================
def build_stock_diario(stock_rows: List[Dict[str, Any]], snapshot_date: dt.date) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    processed_at = dt.datetime.utcnow().isoformat() + "Z"

    for articulo in stock_rows:
        articulo_id = _to_str(articulo.get("id"))
        sku = _to_str(articulo.get("sku"))
        depositos = articulo.get("depositos") or []

        for dep in depositos:
            rows.append({
                "fecha_snapshot": snapshot_date.isoformat(),
                "empresa": _company_code(),
                "articulo_id": articulo_id,
                "sku": sku,
                "deposito_id": _to_str(dep.get("id")),
                "deposito_nombre": _to_str(dep.get("nombre")),
                "stock": _safe_round(dep.get("stock"), 4),
                "transito": _safe_round(dep.get("transito"), 4),
                "_run_id": RUN_ID,
                "_processed_at": processed_at,
            })

    return rows


# =========================
# GCS helpers
# =========================
def write_ndjson_temp(rows: List[Dict[str, Any]], entity: str) -> str:
    fd, temp_path = tempfile.mkstemp(prefix=f"{entity}_{RUN_ID}_", suffix=".ndjson")
    os.close(fd)

    with open(temp_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    logger.info("NDJSON created for %s: %s rows at %s", entity, len(rows), temp_path)
    return temp_path


def upload_to_gcs(local_path: str, entity: str, snapshot_date: dt.date) -> str:
    bucket = storage_client.bucket(GCS_BUCKET)
    filename = f"{entity}_{snapshot_date.isoformat()}_{RUN_ID}.ndjson"
    object_name = f"{GCS_BASE_PREFIX}/{FOLDERS[entity]}/{filename}"

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_path, content_type="application/x-ndjson")

    gcs_uri = f"gs://{GCS_BUCKET}/{object_name}"
    logger.info("Uploaded %s to %s", entity, gcs_uri)
    return gcs_uri


# =========================
# BigQuery helpers
# =========================
def ensure_dataset_exists() -> None:
    dataset_id = f"{GCP_PROJECT}.{BQ_DATASET}"
    try:
        bq_client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = BQ_LOCATION
        bq_client.create_dataset(dataset)
        logger.info("Created dataset: %s", dataset_id)


def load_ndjson_to_temp_table(gcs_uri: str, entity: str) -> str:
    temp_table = f"{GCP_PROJECT}.{BQ_DATASET}._tmp_{entity}_{RUN_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
    )

    job = bq_client.load_table_from_uri(gcs_uri, temp_table, job_config=job_config)
    job.result()

    logger.info("Loaded temp table for %s: %s", entity, temp_table)
    return temp_table


def run_query(sql: str) -> None:
    job = bq_client.query(sql)
    job.result()


def ensure_final_table_from_temp(temp_table: str, final_table: str) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{final_table}` AS
    SELECT * FROM `{temp_table}` WHERE 1=0
    """
    run_query(sql)


def _get_temp_columns(temp_table_name: str) -> List[str]:
    sql = f"""
    SELECT column_name
    FROM `{GCP_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{temp_table_name}'
    ORDER BY ordinal_position
    """
    return [row["column_name"] for row in bq_client.query(sql).result()]


def merge_table(entity: str, temp_table: str) -> None:
    final_table = f"{GCP_PROJECT}.{BQ_DATASET}.{TABLES[entity]}"
    ensure_final_table_from_temp(temp_table, final_table)

    if entity == "ventas_header":
        merge_condition = "T.empresa = S.empresa AND T.venta_id = S.venta_id"
    elif entity == "ventas_items":
        merge_condition = "T.empresa = S.empresa AND T.venta_id = S.venta_id AND T.item_id = S.item_id"
    elif entity == "dim_vendedores":
        merge_condition = "T.empresa = S.empresa AND T.vendedor_id = S.vendedor_id"
    elif entity == "dim_puntos_venta":
        merge_condition = "T.empresa = S.empresa AND T.deposito_id = S.deposito_id"
    elif entity == "clientes":
        merge_condition = "T.empresa = S.empresa AND T.cliente_id = S.cliente_id"
    elif entity == "dim_productos":
        merge_condition = "T.empresa = S.empresa AND T.articulo_id = S.articulo_id"
    else:
        raise ValueError(f"merge_table not supported for entity: {entity}")

    temp_table_name = temp_table.split(".")[-1]
    cols = _get_temp_columns(temp_table_name)

    set_clause = ", ".join([f"T.{c} = S.{c}" for c in cols])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"S.{c}" for c in cols])

    sql = f"""
    MERGE `{final_table}` T
    USING `{temp_table}` S
    ON {merge_condition}
    WHEN MATCHED THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})
    """
    run_query(sql)
    logger.info("Merged entity %s into %s", entity, final_table)


def append_table(entity: str, temp_table: str) -> None:
    final_table = f"{GCP_PROJECT}.{BQ_DATASET}.{TABLES[entity]}"
    ensure_final_table_from_temp(temp_table, final_table)

    if entity == "stock_diario":
        sql_delete = f"""
        DELETE FROM `{final_table}`
        WHERE (fecha_snapshot, empresa, sku, deposito_id) IN (
          SELECT fecha_snapshot, empresa, sku, deposito_id
          FROM `{temp_table}`
        )
        """
        run_query(sql_delete)

    sql_insert = f"""
    INSERT INTO `{final_table}`
    SELECT * FROM `{temp_table}`
    """
    run_query(sql_insert)
    logger.info("Appended entity %s into %s", entity, final_table)


def drop_temp_table(temp_table: str) -> None:
    try:
        bq_client.delete_table(temp_table, not_found_ok=True)
        logger.info("Dropped temp table: %s", temp_table)
    except Exception as e:
        logger.warning("Could not drop temp table %s: %s", temp_table, e)


def process_entity(entity: str, rows: List[Dict[str, Any]], snapshot_date: dt.date, append: bool = False) -> None:
    if not rows:
        logger.warning("Entity %s has 0 rows. Skipping.", entity)
        return

    local_path = write_ndjson_temp(rows, entity)
    try:
        gcs_uri = upload_to_gcs(local_path, entity, snapshot_date)
        temp_table = load_ndjson_to_temp_table(gcs_uri, entity)

        if append:
            append_table(entity, temp_table)
        else:
            merge_table(entity, temp_table)

        drop_temp_table(temp_table)
    finally:
        try:
            os.remove(local_path)
        except Exception:
            pass


# =========================
# Main
# =========================
def main() -> None:
    logger.info("Starting Caddis pipeline run_id=%s company=%s", RUN_ID, COMPANY_CODE)

    ensure_dataset_exists()

    fecha_desde, fecha_hasta, snapshot_date = _resolve_date_range()
    token = get_access_token()

    ventas = fetch_ventas(token, fecha_desde, fecha_hasta)

    ventas_header = build_ventas_header(ventas)
    ventas_items = build_ventas_items(ventas)
    dim_vendedores = build_dim_vendedores(ventas)
    dim_puntos_venta = build_dim_puntos_venta(ventas)
    clientes = build_clientes(ventas)

    articulos = fetch_paginated_endpoint(token, "articulos")
    dim_productos = build_dim_productos(articulos)

    stock_api_rows = fetch_paginated_endpoint(token, "articulos/stock")
    stock_diario = build_stock_diario(stock_api_rows, snapshot_date)

    process_entity("ventas_header", ventas_header, snapshot_date, append=False)
    process_entity("ventas_items", ventas_items, snapshot_date, append=False)
    process_entity("dim_vendedores", dim_vendedores, snapshot_date, append=False)
    process_entity("dim_puntos_venta", dim_puntos_venta, snapshot_date, append=False)
    process_entity("clientes", clientes, snapshot_date, append=False)
    process_entity("dim_productos", dim_productos, snapshot_date, append=False)
    process_entity("stock_diario", stock_diario, snapshot_date, append=True)

    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    main()