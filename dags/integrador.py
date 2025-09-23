from __future__ import annotations

import shutil


import os, zipfile, logging, json, re
from datetime import timedelta
from pathlib import Path
from typing import List, Dict

import pandas as pd
import numpy as np

from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

import re

_DT_NUM_RE = re.compile(
    r"""^\s*
        (?P<time>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})      # fecha y hora
        [\s,;|]+
        (?P<open>[-+]?\d+(?:\.\d+)?)
        [\s,;|]+
        (?P<high>[-+]?\d+(?:\.\d+)?)
        [\s,;|]+
        (?P<low>[-+]?\d+(?:\.\d+)?)
        [\s,;|]+
        (?P<close>[-+]?\d+(?:\.\d+)?)
        (?:[\s,;|]+(?P<volume>[-+]?\d+(?:\.\d+)?))?
        """,
    re.X,
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
RAW_DIR      = os.path.join(AIRFLOW_HOME, "include", "data", "raw")
INTERIM_DIR  = os.path.join(AIRFLOW_HOME, "include", "data", "interim")
OUTPUT_DIR   = os.path.join(AIRFLOW_HOME, "include", "output")  # <-- AHORA VA DENTRO DE include



DAG_ID = "etl_integrador_excel_csv"
DEFAULT_ARGS = dict(
    owner="Emanuel",
    retries=1,
    retry_delay=timedelta(minutes=2),
    depends_on_past=False,
    email_on_failure=False,
)

# =========================
# CONFIG POR CARPETA (GRUPO)
# =========================
# Cada clave es una subcarpeta de include/data/raw/
# =========================
# CONFIG POR CARPETA (GRUPO)
# =========================
# Cada clave es una subcarpeta de include/data/raw/
SCHEMA_RULES = {
    # noticias => Release Date, Time, Actual, Forecast, Previous
    "noticias": {
        "csv": {"sep": ",", "encoding": "utf-8"},
        "rename": {
            "Release Date": "release_date",
            "Time": "time",
            "Actual": "actual",
            "Forecast": "forecast",
            "Previous": "previous",
        },
        "dates": ["release_date"],
        "numbers": ["actual","forecast","previous"],  # quitamos % y comas
        "required": ["release_date"],
        "drop_duplicates": True,
    },

    # pares => OHLCV (Time, Open, High, Low, Close, Volume)
    # Muchos vienen en 1 sola columna separada por espacios/tabs/coma.
    "pares": {
        "csv": {
            # Dejá que pandas infiera el separador (requiere engine=python)
            "sep": None,
            "engine": "python",
            "header": None,
            # hasta 7 columnas por si aparece una extra (count)
            "names": ["time", "open", "high", "low", "close", "volume", "_extra"],
            "usecols": ["time", "open", "high", "low", "close", "volume"],
            "dtype": {"time": "string"},
            "na_values": ["", "null", "NaN"]
        }
    },

    # fallback si aparece otra carpeta
    "_default": {
        "csv": {"sep": ",", "encoding": "utf-8"},
        "rename": {},
        "dates": ["date","fecha","datetime"],
        "numbers": [],
        "required": [],
        "drop_duplicates": True,
    },
}


# =========================
# HELPERS GENÉRICOS
# =========================
def _snake_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.astype(str)
                  .str.strip()
                  .str.replace(" ", "_")
                  .str.replace(r"[^a-zA-Z0-9_]", "", regex=True)
                  .str.lower()
    )
    return df

def apply_rules(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    # renombrar si corresponde
    if rules.get("rename"):
        df = df.rename(columns=rules["rename"])
    # snake_case
    df = _snake_cols(df)
    # fechas
    for c in rules.get("dates", []):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    # numéricos (limpiando % y comas)
    for c in rules.get("numbers", []):
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                     .str.replace("%","", regex=False)
                     .str.replace(",","", regex=False)
            )
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # requeridos
    req = rules.get("required", [])
    if req:
        keep = pd.Series(True, index=df.index)
        for c in req:
            if c in df.columns:
                keep &= ~df[c].isna()
        df = df[keep]
    # duplicados
    if rules.get("drop_duplicates", False):
        df = df.drop_duplicates()
    return df

# =========================
# TRANSFORMACIONES ESPECÍFICAS POR GRUPO
# =========================
def _drop_meta_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Eliminá columnas internas que no querés en los outputs
    return df.drop(columns=["__source_sheet", "__group"], errors="ignore")

def transform_noticias(df: pd.DataFrame) -> pd.DataFrame:
    """
    Salida: 6 columnas -> fecha, hora, actual, forecast, previo, source_file.
    Nada de __source_sheet/__group.
    """
    df = _drop_meta_cols(df)

    # Si viene como una sola columna, separamos en 5 (fecha, hora, actual, forecast, previo)
    if df.shape[1] == 1:
        s = df.iloc[:, 0].astype("string").str.replace(";", ",", regex=False).str.strip()
        parts = s.str.split(",", n=4, expand=True)
        while parts.shape[1] < 5:
            parts[parts.shape[1]] = pd.NA
        parts = parts.iloc[:, :5]
        parts.columns = ["fecha", "hora", "actual", "forecast", "previo"]
        out = parts
    else:
        # normalizamos nombres para encontrar columnas conocidas
        cols_norm = {c: str(c).strip().lower().replace(" ", "_") for c in df.columns}
        df = df.rename(columns=cols_norm)

        # mapeos típicos de los archivos de "news"
        # (Release Date, Time, Actual, Forecast, Previous)
        fecha_col   = next((c for c in df.columns if c in ["release_date", "fecha"]), None)
        hora_col    = next((c for c in df.columns if c in ["time", "hora"]), None)
        actual_col  = next((c for c in df.columns if c in ["actual"]), None)
        forecast_col= next((c for c in df.columns if c in ["forecast"]), None)
        previo_col  = next((c for c in df.columns if c in ["previous", "previo"]), None)

        out = pd.DataFrame({
            "fecha":   df[fecha_col]   if fecha_col   else pd.NA,
            "hora":    df[hora_col]    if hora_col    else pd.NA,
            "actual":  df[actual_col]  if actual_col  else pd.NA,
            "forecast":df[forecast_col]if forecast_col else pd.NA,
            "previo":  df[previo_col]  if previo_col  else pd.NA,
        })

    # agregamos SOLO source_file (lo pediste)
    out["source_file"] = df["__source_file"] if "__source_file" in df.columns else pd.NA
    return out


def _parse_pairs_series(s: pd.Series) -> pd.DataFrame:
    """
    Intenta parsear cada línea de 'pares' tolerando separadores variados.
    - Primero intenta split por coma / ';' / tab / espacios múltiples.
    - Si no, usa regex que busca time + 5 números (y volume opcional).
    Devuelve DataFrame con columnas: time, open, high, low, close, volume
    y una máscara booleana de 'ok' para saber si se parseó.
    """
    records = []
    ok_flags = []

    for raw in s.fillna("").astype("string"):
        line = raw.strip()
        row = None

        # 1) splits más comunes
        for sep in [",", ";", "\t", r"\s+"]:
            if sep == r"\s+":
                parts = re.split(r"\s+", line) if line else []
            else:
                if sep in line:
                    parts = [p.strip() for p in line.split(sep)]
                else:
                    parts = []
            if len(parts) >= 6:
                # caso típico: time, open, high, low, close, volume
                row = {
                    "time": parts[0],
                    "open": parts[1],
                    "high": parts[2],
                    "low":  parts[3],
                    "close":parts[4],
                    "volume": parts[5],
                }
                break
            # caso “whitespace”: a veces es fecha, hora, open, high, low, close, volume
            if len(parts) >= 7 and sep == r"\s+":
                row = {
                    "time":  f"{parts[0]} {parts[1]}",
                    "open":  parts[2],
                    "high":  parts[3],
                    "low":   parts[4],
                    "close": parts[5],
                    "volume":parts[6],
                }
                break

        # 2) regex fallback si los splits no funcionaron
        if row is None:
            m = _DT_NUM_RE.match(line)
            if m:
                row = m.groupdict()
                # si no vino volume, déjalo vacío
                row.setdefault("volume", None)

        if row is None:
            # falló: devolvemos vacíos para esta fila
            records.append({"time": None, "open": None, "high": None, "low": None, "close": None, "volume": None, "_raw": raw})
            ok_flags.append(False)
        else:
            records.append({**row, "_raw": raw})
            ok_flags.append(True)

    out = pd.DataFrame.from_records(records)
    return out[["time","open","high","low","close","volume","_raw"]], pd.Series(ok_flags, name="_ok")

def transform_pares(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza 'pares' a: time, fecha, hora, pair, open, high, low, close, volume, source_file
    Soporta múltiples formatos de origen. Loguea filas que no se pudieron parsear.
    """
    # si vino en una sola columna, la tomamos como texto crudo
    if df.shape[1] == 1:
        raw = df.iloc[:, 0].astype("string")
        parsed, ok = _parse_pairs_series(raw)
    else:
        # ya vienen columnas: normalizamos nombres y nos quedamos con las que necesitamos
        rename = {c: str(c).strip().lower() for c in df.columns}
        tmp = df.rename(columns=rename)
        # si tiene fecha + hora en columnas separadas, únelas
        if {"fecha", "hora"}.issubset(tmp.columns) and "time" not in tmp.columns:
            tmp["time"] = (tmp["fecha"].astype("string") + " " + tmp["hora"].astype("string")).str.strip()
        need = ["time", "open", "high", "low", "close", "volume"]
        parsed = tmp.reindex(columns=need)
        parsed["_raw"] = parsed.astype(str).agg(",".join, axis=1)
        ok = pd.Series(True, index=parsed.index, name="_ok")

    # tipado numérico
    for c in ["open", "high", "low", "close", "volume"]:
        if c in parsed.columns:
            parsed[c] = pd.to_numeric(parsed[c], errors="coerce")

    # setear source_file (si viene en el df original)
    src_val = ""
    if "__source_file" in df.columns and len(df["__source_file"]):
        src_val = str(df["__source_file"].iloc[0])
    parsed["source_file"] = src_val if src_val else pd.NA

    # inferir 'pair' a partir del nombre de archivo (stem)
    if src_val:
        base = Path(src_val).stem
        # toma hasta el primer separador no-alnum (ej: EURUSD_1min -> EURUSD)
        pair = re.split(r"[^A-Za-z0-9]", base)[0]
        parsed["pair"] = pair if pair else pd.NA
    else:
        parsed["pair"] = pd.NA

    # parsear 'time' a fecha/hora separadas (si existe)
    if "time" in parsed.columns:
        dt = pd.to_datetime(parsed["time"], errors="coerce")
        # formato consistente
        parsed["fecha"] = dt.dt.strftime("%Y-%m-%d").astype("string")
        parsed["hora"] = dt.dt.strftime("%H:%M:%S").astype("string")
    else:
        parsed["fecha"] = pd.NA
        parsed["hora"] = pd.NA

    # Guardar filas problemáticas a un CSV (si existen)
    bad = parsed[~ok].copy() if isinstance(ok, pd.Series) else pd.DataFrame()
    if len(bad):
        out_bad = Path(OUTPUT_DIR) / f"pares_badrows_{pd.Timestamp.now().date()}.csv"
        out_bad.parent.mkdir(parents=True, exist_ok=True)
        # columnas útiles
        cols_to_write = [c for c in ["source_file", "_raw"] if c in bad.columns]
        bad[cols_to_write].to_csv(out_bad, index=False)
        logging.warning(f"[pares] {len(bad)} filas no parseadas. Ver: {out_bad}")

    # salida final ordenada — asegurá un orden legible
    desired = ["time", "fecha", "hora", "pair", "open", "high", "low", "close", "volume", "source_file", "_raw"]
    out_cols = [c for c in desired if c in parsed.columns]
    return parsed[out_cols]



GROUP_TRANSFORMS = {
    "noticias": transform_noticias,
    "pares": transform_pares,
}

# ========= Tareas (funciones) =========

def ensure_dirs():
    Path(RAW_DIR).mkdir(parents=True, exist_ok=True)
    Path(INTERIM_DIR).mkdir(parents=True, exist_ok=True)
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def list_input_files(ti=None):
    """
    Busca recursivamente .xlsx, .xlsm y .csv en include/data/raw y subcarpetas.
    Deja la lista en XCom.
    """
    root = Path(RAW_DIR)
    files: List[str] = []
    for ext in ["*.xlsx", "*.xlsm", "*.csv"]:
        files.extend([str(p) for p in root.rglob(ext)])
    files = sorted(files)

    if not files:
        raise FileNotFoundError(f"No se encontraron .xlsx/.xlsm/.csv en {root} (ni subcarpetas).")

    logging.info(f"Archivos detectados ({len(files)}): {files}")
    if ti:
        ti.xcom_push(key="input_files", value=json.dumps(files))


def read_and_merge_files(ti=None, **_):
    """
    Lee todos los archivos (excel/csv), aplica reglas y transformaciones por grupo,
    guarda intermedios parquet por grupo y publica sus rutas en XCom.
    Solo admite las carpetas: noticias y pares.
    """
    files = json.loads(ti.xcom_pull(key="input_files", task_ids="list_input_files"))

    root = Path(RAW_DIR)
    buckets: Dict[str, List[pd.DataFrame]] = {}

    for fpath in files:
        fpath = str(fpath)
        ext = Path(fpath).suffix.lower()

        # detectar grupo por subcarpeta (solo noticias y pares)
        rel = Path(fpath).relative_to(root)
        group = rel.parts[0] if len(rel.parts) > 1 else None

        if group not in ("noticias", "pares"):
            logging.warning(f"Archivo ignorado (fuera de noticias/pares): {fpath}")
            continue

        rules = SCHEMA_RULES.get(group, SCHEMA_RULES["_default"])

        # leer archivos Excel
        if ext in [".xlsx", ".xlsm"]:
            sheets: Dict[str, pd.DataFrame] = pd.read_excel(fpath, sheet_name=None, engine="openpyxl")
            for sheet_name, df in sheets.items():
                if df is None or df.empty:
                    continue
                df["__source_file"] = os.path.basename(fpath)
                df["__source_sheet"] = sheet_name
                df["__group"] = group

                # reglas genéricas
                df = apply_rules(df, rules)
                # transformación específica
                transform_fn = GROUP_TRANSFORMS.get(group)
                if transform_fn:
                    df = transform_fn(df)

                buckets.setdefault(group, []).append(df)

        # leer archivos CSV
        elif ext == ".csv":
            csv_opts = rules.get("csv", {})
            df = pd.read_csv(fpath, **csv_opts)
            if df is None or df.empty:
                continue
            df["__source_file"] = os.path.basename(fpath)
            df["__source_sheet"] = "csv"
            df["__group"] = group

            df = apply_rules(df, rules)
            transform_fn = GROUP_TRANSFORMS.get(group)
            if transform_fn:
                df = transform_fn(df)

            buckets.setdefault(group, []).append(df)

    if not buckets:
        raise ValueError("No se encontraron datos en ningún archivo válido (noticias/pares)")

    # Guardar intermedios por grupo
    inter_paths: List[str] = []
    Path(INTERIM_DIR).mkdir(parents=True, exist_ok=True)
    for group, frames in buckets.items():
        gdf = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        gpath = Path(INTERIM_DIR) / f"interim_{group}.parquet"

        # --- Arrow-safe: normalizar tipos antes de guardar ---
        if "time" in gdf.columns and not pd.api.types.is_datetime64_any_dtype(gdf["time"]):
            gdf["time"] = gdf["time"].astype("string")

        for col in gdf.select_dtypes(include=["object"]).columns:
            gdf[col] = gdf[col].astype("string")

        gdf = gdf.convert_dtypes()

        gdf = gdf.drop(columns=["__source_sheet", "__group"], errors="ignore")

        gdf.to_parquet(gpath, index=False)
        inter_paths.append(str(gpath))
        logging.info(f"[{group}] filas={len(gdf)} cols={list(gdf.columns)} -> {gpath}")

    if ti:
        ti.xcom_push(key="interim_paths", value=json.dumps(inter_paths))

import pandas as pd

def produce_outputs(ds: str, ti=None, **_):
    """
    Produce outputs por grupo y genera correlacion_final_{ds}:
    - filas = noticia (solo noticias que tienen al menos un par en la misma fecha+hora redondeada)
    - primeras columnas = columnas de noticia: fecha, hora (original), actual, forecast, previo, source_file (si existen)
    - luego por cada par (alfabéticamente) 5 columnas: <PAIR>_open, <PAIR>_high, <PAIR>_low, <PAIR>_close, <PAIR>_volume
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    inter_paths = json.loads(ti.xcom_pull(key="interim_paths", task_ids="read_and_merge_files"))
    inter_paths = [Path(p) for p in inter_paths if p]
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    final_paths: List[str] = []

    # ---------- helpers ----------
    def _parse_fecha_hora(df: pd.DataFrame) -> pd.DataFrame:
        """
        Devuelve copia con:
          - 'fecha' (YYYY-MM-DD string)
          - 'hora'  (HH:MM:SS string) <-- preserva/normaliza la hora original
          - 'ts'    (pd.Timestamp) para matching
        Tolerante ante columnas distintas ('fecha'+'hora', 'time', 'release_date'+ 'time').
        """
        df = df.copy()
        # Ya vienen fecha + hora
        if {"fecha", "hora"} <= set(df.columns):
            # guardamos fecha normalizada y hora normalizada (string)
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.strftime("%Y-%m-%d").astype("string")
            df["hora"] = pd.to_datetime(df["hora"].astype(str), errors="coerce").dt.strftime("%H:%M:%S").astype("string")
            df["ts"] = pd.to_datetime(df["fecha"].astype(str) + " " + df["hora"].astype(str), errors="coerce")
            return df

        # columna 'time' datetimelike
        if "time" in df.columns:
            dt = pd.to_datetime(df["time"], errors="coerce")
            df["fecha"] = dt.dt.strftime("%Y-%m-%d").astype("string")
            df["hora"] = dt.dt.strftime("%H:%M:%S").astype("string")
            df["ts"] = dt
            return df

        # release_date (+ opcional time)
        if "release_date" in df.columns:
            if "time" in df.columns:
                dt = pd.to_datetime(df["release_date"].astype(str) + " " + df["time"].astype(str), errors="coerce")
            else:
                dt = pd.to_datetime(df["release_date"], errors="coerce")
            df["fecha"] = dt.dt.strftime("%Y-%m-%d").astype("string")
            df["hora"] = dt.dt.strftime("%H:%M:%S").astype("string")
            df["ts"] = dt
            return df

        # fallback: columnas vacías
        df["fecha"] = pd.NA
        df["hora"] = pd.NA
        df["ts"] = pd.NaT
        return df

    def _ensure_pair_col(df: pd.DataFrame) -> pd.DataFrame:
        """
        Asegura columna 'pair' inferida desde __source_file o source_file si hace falta;
        normaliza a string MAYUS.
        """
        df = df.copy()
        if "pair" in df.columns:
            df["pair"] = df["pair"].astype("string").fillna("").str.upper()
            return df

        sf_col = "__source_file" if "__source_file" in df.columns else ("source_file" if "source_file" in df.columns else None)
        if sf_col:
            def _infer(x):
                try:
                    stem = Path(str(x)).stem
                    m = re.search(r"[A-Za-z0-9]+", stem)
                    return m.group(0).upper() if m else pd.NA
                except Exception:
                    return pd.NA
            df["pair"] = df[sf_col].fillna("").astype(str).apply(_infer).astype("string")
            return df

        df["pair"] = pd.NA
        return df

    def _clean_pair_name(s: str) -> str:
        """Limpiar para nombre de columna: solo A-Z0-9, guiones convertidos a _ (trim)."""
        if pd.isna(s):
            return ""
        s = str(s).upper()
        s = re.sub(r"[^A-Z0-9]+", "_", s)
        s = re.sub(r"^_+|_+$", "", s)
        return s or ""

    # =========================
    # 0) Leer y normalizar intermedios en memoria por grupo
    # =========================
    normalized: Dict[str, List[pd.DataFrame]] = {}
    for p in inter_paths:
        if not p.exists():
            continue
        group = p.stem.replace("interim_", "", 1)
        if group not in ("noticias", "pares"):
            logging.warning(f"[produce_outputs] Ignorado grupo desconocido: {group}")
            continue

        df = pd.read_parquet(p)
        # normalizamos y añadimos 'ts'
        df = _parse_fecha_hora(df)

        if group == "pares":
            df = _ensure_pair_col(df)
            # tipado numérico para métricas de pares
            for c in ["open", "high", "low", "close", "volume"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")

        normalized.setdefault(group, []).append(df)

    # Guardar outputs por grupo (finales) - quitamos columnas auxiliares 'time','ts','ts_hour' para inspección legible
    for group, frames in normalized.items():
        gdf = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        gdf_out = gdf.drop(columns=["time", "ts", "ts_hour", "__source_sheet", "__group"], errors="ignore")
        out_parquet = Path(OUTPUT_DIR) / f"final_{group}_{ds}.parquet"
        out_csv = Path(OUTPUT_DIR) / f"final_{group}_{ds}.csv"
        gdf_out.to_parquet(out_parquet, index=False)
        gdf_out.to_csv(out_csv, index=False)
        logging.info(f"[{group}] Guardado: {out_parquet} | {out_csv}")
        final_paths.extend([str(out_parquet), str(out_csv)])

    # =========================
    # 1) CSV GLOBAL (unión de columnas) - construcción desde normalized (streaming)
    # =========================
    global_cols: List[str] = []
    seen = set()
    for frames in normalized.values():
        for df in frames:
            for c in df.columns:
                if c not in seen:
                    seen.add(c)
                    global_cols.append(c)

    out_csv_g = Path(OUTPUT_DIR) / f"final_global_{ds}.csv"
    out_parquet_g = Path(OUTPUT_DIR) / f"final_global_{ds}.parquet"
    if out_csv_g.exists():
        out_csv_g.unlink()
    if out_parquet_g.exists():
        out_parquet_g.unlink()

    wrote_header = False
    with open(out_csv_g, "w", newline="", encoding="utf-8") as f:
        for group, frames in normalized.items():
            for df in frames:
                df_local = df.copy().drop(columns=["time", "ts", "ts_hour"], errors="ignore")
                for c in global_cols:
                    if c not in df_local.columns:
                        df_local[c] = pd.NA
                df_local = df_local.reindex(columns=global_cols)
                if not wrote_header:
                    df_local.to_csv(f, index=False, header=True, mode="w")
                    wrote_header = True
                else:
                    df_local.to_csv(f, index=False, header=False, mode="a")
    final_paths.append(str(out_csv_g))
    logging.info(f"[global] CSV guardado: {out_csv_g}")

    # =========================
    # 2) PARQUET GLOBAL (todo como string)
    # =========================
    arrow_fields = [pa.field(col, pa.string()) for col in global_cols]
    schema_arrow = pa.schema(arrow_fields)
    writer = pq.ParquetWriter(out_parquet_g, schema_arrow)
    try:
        for group, frames in normalized.items():
            for df in frames:
                df_local = df.copy().drop(columns=["time", "ts", "ts_hour"], errors="ignore")
                for c in global_cols:
                    if c not in df_local.columns:
                        df_local[c] = pd.NA
                df_local = df_local.reindex(columns=global_cols)
                df_string = df_local.copy()
                for c in df_string.columns:
                    if not pd.api.types.is_string_dtype(df_string[c]):
                        if pd.api.types.is_datetime64_any_dtype(df_string[c]):
                            df_string[c] = df_string[c].dt.strftime("%Y-%m-%d %H:%M:%S").astype("string")
                        else:
                            df_string[c] = df_string[c].astype("string")
                table = pa.Table.from_pandas(df_string, preserve_index=False, schema=schema_arrow)
                writer.write_table(table)
    finally:
        writer.close()
    final_paths.append(str(out_parquet_g))
    logging.info(f"[global] Parquet guardado: {out_parquet_g}")

    # =========================
    # 3) CORRELACION_FINAL (wide): una fila por noticia + bloques por pair
    # =========================
    noticias_df = pd.concat(normalized.get("noticias", []), ignore_index=True) if normalized.get("noticias") else pd.DataFrame()
    pares_df = pd.concat(normalized.get("pares", []), ignore_index=True) if normalized.get("pares") else pd.DataFrame()

    if noticias_df.empty or pares_df.empty:
        logging.warning("[produce_outputs] Faltan noticias o pares (vacío): no se generará correlacion_final.")
    else:
        # aseguramos ts y generamos clave horaria (ts_hour) con floor a la hora
        noticias_df = _parse_fecha_hora(noticias_df)
        pares_df = _parse_fecha_hora(pares_df)
        pares_df = _ensure_pair_col(pares_df)

        noticias_df["ts"] = pd.to_datetime(noticias_df["ts"], errors="coerce")
        noticias_df["ts_hour"] = noticias_df["ts"].dt.floor("H")

        pares_df["ts"] = pd.to_datetime(pares_df["ts"], errors="coerce")
        # los pares ya vienen en cortes de hora; igualmente floor para estabilidad
        pares_df["ts_hour"] = pares_df["ts"].dt.floor("H")

        # limpiar/normalizar nombre de pair para columnas
        pares_df["pair"] = pares_df["pair"].astype("string").fillna("").astype(str)
        pares_df["pair_clean"] = pares_df["pair"].apply(lambda s: _clean_pair_name(s))

        # preparar base de noticias (index news_idx) y seleccionar columnas de noticia (en orden)
        noticias_df = noticias_df.reset_index(drop=True)
        noticias_df["news_idx"] = noticias_df.index
        # seleccionar source_file si existe (prefiere 'source_file' sobre '__source_file')
        sf_col = "source_file" if "source_file" in noticias_df.columns else ("__source_file" if "__source_file" in noticias_df.columns else None)
        news_cols_order = ["fecha", "hora", "actual", "forecast", "previo", sf_col] if sf_col else ["fecha", "hora", "actual", "forecast", "previo"]
        # limpiar nombres reales existentes
        news_cols = [c for c in news_cols_order if c in noticias_df.columns and c is not None]
        base = noticias_df.set_index("news_idx")[news_cols].copy()

        # Merge inner por ts_hour; elegimos columnas necesarias de pares para evitar overlaps innecesarios
        pares_needed = [c for c in ["ts_hour", "pair", "pair_clean", "open", "high", "low", "close", "volume", "source_file"] if c in pares_df.columns]
        pares_sub = pares_df[pares_needed].copy()
        merged = pd.merge(
            noticias_df[["news_idx", "ts_hour"]],
            pares_sub,
            on=["ts_hour"],
            how="inner",
            suffixes=("_noticia", "_par"),
        )

        if merged.empty:
            logging.info("[produce_outputs] Merge noticia x par produjo 0 filas (no coincidencias por hora).")
        else:
            # pares únicos ordenados (descartar nombres vacíos)
            merged["pair_clean"] = merged["pair_clean"].astype(str)
            pares_list = sorted([p for p in merged["pair_clean"].dropna().unique() if p])

            metrics = ["open", "high", "low", "close", "volume"]
            pair_metric_cols: List[str] = []

            # Para cada par y métrica, extraer primer valor por news_idx y unir a base
            for pair in pares_list:
                sub = merged[merged["pair_clean"] == pair]
                if sub.empty:
                    for m in metrics:
                        colname = f"{pair}_{m}"
                        pair_metric_cols.append(colname)
                        base[colname] = pd.NA
                    continue

                grouped = sub.groupby("news_idx", as_index=True)
                for metric in metrics:
                    colname = f"{pair}_{metric}"
                    pair_metric_cols.append(colname)
                    if metric in sub.columns:
                        series = grouped[metric].first()
                        # forzar índice entero si es posible
                        try:
                            series.index = series.index.astype(int)
                        except Exception:
                            pass
                        series = series.rename(colname)
                        # join: base index = news_idx
                        base = base.join(series, how="left")
                    else:
                        base[colname] = pd.NA

            # eliminar noticias que no tienen ningún par asociado (todas las pair_* NA)
            if pair_metric_cols:
                before = len(base)
                base = base.dropna(subset=pair_metric_cols, how="all")
                after = len(base)
                logging.info(f"[produce_outputs] Eliminadas {before-after} noticias sin pares asociados (todas pair_* NA).")
            else:
                logging.info("[produce_outputs] No se detectaron métricas de pares para generar correlacion_final.")

            # reordenar columnas: news_cols seguidas por bloques por pair (alfabético)
            ordered_pair_cols = []
            for pair in pares_list:
                ordered_pair_cols.extend([f"{pair}_{m}" for m in metrics])
            final_columns = news_cols + ordered_pair_cols
            final_columns = [c for c in final_columns if c in base.columns]
            base = base[final_columns]

            # Guardar correlacion_final (CSV + parquet)
            correl_final_csv = Path(OUTPUT_DIR) / f"correlacion_final_{ds}.csv"
            correl_final_parquet = Path(OUTPUT_DIR) / f"correlacion_final_{ds}.parquet"
            base = base.reset_index(drop=True)
            base.to_csv(correl_final_csv, index=False)
            base.to_parquet(correl_final_parquet, index=False)
            logging.info(f"[correlacion_final] Guardado: {correl_final_csv} | {correl_final_parquet}")
            final_paths.extend([str(correl_final_csv), str(correl_final_parquet)])

    # =========================
    # 4) XCom con rutas finales
    # =========================
    if ti:
        ti.xcom_push(key="final_paths", value=json.dumps(final_paths))




def zip_logs(ds: str, dag_id: str, run_id: str, ti=None, **_):
    """
    Comprime logs reales de la corrida para auditoría.
    """
    airflow_home = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
    candidates = [
        os.path.join(airflow_home, "logs", f"dag_id={dag_id}"),
        os.path.join(airflow_home, "logs", dag_id),
    ]
    out_zip = Path(OUTPUT_DIR) / f"logs_{ds}.zip"
    out_zip.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        found = False
        for base in candidates:
            if not os.path.isdir(base):
                continue
            for root, _, files in os.walk(base):
                if ("run_id=" in root or run_id in root) and run_id not in root:
                    continue
                for name in files:
                    full = os.path.join(root, name)
                    arc = os.path.relpath(full, start=base)
                    zf.write(full, arcname=os.path.join(os.path.basename(base), arc))
                    found = True
        if not found:
            zf.writestr("README.txt", f"Sin logs para {dag_id} / {run_id} en {candidates}")

    logging.info(f"ZIP de logs: {out_zip}")
    if ti:
        ti.xcom_push(key="logs_zip", value=str(out_zip))


def mirror_outputs_to_include(ti=None, **_):
    """
    Copia TODO lo que se generó en /usr/local/airflow/output
    hacia /usr/local/airflow/include/output (que sí está montado al host).
    """
    airflow_home = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
    src = os.path.join(airflow_home, "output")
    dst = os.path.join(airflow_home, "include", "output")

    os.makedirs(dst, exist_ok=True)

    if not os.path.isdir(src):
        logging.info(f"[mirror] No existe carpeta de origen: {src}")
        return

    count = 0
    for name in os.listdir(src):
        s = os.path.join(src, name)
        d = os.path.join(dst, name)
        if os.path.isfile(s):
            shutil.copy2(s, d)
            count += 1
    logging.info(f"[mirror] Copiados {count} archivos de {src} -> {dst}")

    if ti:
        ti.xcom_push(key="mirror_dst", value=dst)

# ========= Definición del DAG =========

with DAG(
    dag_id=DAG_ID,
    description="Integrador - Ingesta Excel/CSV recursiva con reglas por carpeta + outputs por grupo",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date = pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["integrador", "excel", "csv", "etl"],
) as dag:

    t_ensure = PythonOperator(
        task_id="ensure_dirs",
        python_callable=ensure_dirs,
    )

    t_list = PythonOperator(
        task_id="list_input_files",
        python_callable=list_input_files,
    )

    t_read = PythonOperator(
        task_id="read_and_merge_files",
        python_callable=read_and_merge_files,
    )

    t_outputs = PythonOperator(
        task_id="produce_outputs",
        python_callable=produce_outputs,
        op_kwargs={"ds": "{{ ds }}"},
    )

    t_zip = PythonOperator(
        task_id="zip_logs",
        python_callable=zip_logs,
        op_kwargs={"ds": "{{ ds }}", "dag_id": "{{ dag.dag_id }}", "run_id": "{{ run_id }}"},
    )

    t_mirror = PythonOperator(
        task_id="mirror_outputs_to_include",
        python_callable=mirror_outputs_to_include,
    )


    t_ensure >> t_list >> t_read >> t_outputs >> t_mirror >> t_zip

