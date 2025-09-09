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
SCHEMA_RULES = {
    # calendario/new.csv => “gran” columna con coma: 2007/01/01,00:01,CNY,N,"Bank Holiday",,,,,
    "calendario": {
        "csv": {"sep": ",", "encoding": "utf-8", "header": None},  # entra como 1 sola columna
        "rename": {},
        "dates": [],
        "numbers": [],
        "required": [],
        "drop_duplicates": True,
    },

    # desastres => EM-DAT (Excel grande). Crearemos start_date/end_date.
    "desastres": {
        "csv": {"sep": ",", "encoding": "utf-8"},
        "rename": {},
        "dates": [],
        "numbers": [
            "total_deaths","no_injured","no_affected","no_homeless","total_affected",
            "reconstruction_costs_000_us","reconstruction_costs_adjusted_000_us",
            "insured_damage_000_us","insured_damage_adjusted_000_us",
            "total_damage_000_us","total_damage_adjusted_000_us",
            "cpi","magnitude","latitude","longitude","aid_contribution_000_us"
        ],
        "required": [],
        "drop_duplicates": True,
    },

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
# ---------- TRANSFORMACIONES POR GRUPO (COPIAR/PEGAR COMPLETO) ----------

def _drop_meta_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Eliminá columnas internas que no querés en los outputs
    return df.drop(columns=["__source_sheet", "__group"], errors="ignore")

def transform_calendario(df: pd.DataFrame) -> pd.DataFrame:
    """
    Esperado: 5 columnas -> fecha, hora, pais, impacto, nombre.
    El origen a veces entra como UNA sola columna con comas/; y comillas.
    """
    df = _drop_meta_cols(df)
    # trabajamos sobre la primera columna en forma robusta
    s = df.iloc[:, 0].astype("string").str.replace(";", ",", regex=False).str.strip()

    # tomamos solo los primeros 5 campos, ignorando lo que venga después
    parts = s.str.split(",", n=4, expand=True)
    # asegurar 5 columnas aunque falten campos
    while parts.shape[1] < 5:
        parts[parts.shape[1]] = pd.NA
    parts = parts.iloc[:, :5]
    parts.columns = ["fecha", "hora", "pais", "impacto", "nombre"]

    # tipitos suaves
    # (si querés fecha/hora como datetime, podés parsear acá)
    return parts

def transform_desastres(df: pd.DataFrame) -> pd.DataFrame:
    """
    Debe conservar 'tal cual' todas las columnas del original.
    A veces entra en una sola columna; intentamos dividir por separador probable.
    """
    df = _drop_meta_cols(df)
    if df.shape[1] == 1:
        raw = df.iloc[:, 0].astype("string")
        # intentá primero tab, luego ';', luego ','
        for sep in ["\t", ";", ","]:
            parts = raw.str.split(sep, expand=True)
            # si ‘parece’ que la primera fila es header (mucho texto), la usamos
            header = parts.iloc[0].tolist()
            if any(isinstance(x, str) and len(x) > 0 for x in header):
                parts.columns = [str(c).strip() for c in header]
                parts = parts.iloc[1:].reset_index(drop=True)
                return parts
        # si no detectamos, devolvemos la columna única (pero ya al menos no rompemos)
        parts = raw.to_frame(name="raw")
        return parts
    else:
        # ya viene con columnas – devolvelo como está
        return df.reset_index(drop=True)

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
    Normaliza 'pares' a: time, open, high, low, close, volume, source_file
    Soporta múltiples formatos de origen.
    Loguea filas que no se pudieron parsear (para inspección).
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
        if {"fecha","hora"}.issubset(tmp.columns) and "time" not in tmp.columns:
            tmp["time"] = (tmp["fecha"].astype("string") + " " + tmp["hora"].astype("string")).str.strip()
        need = ["time","open","high","low","close","volume"]
        parsed = tmp.reindex(columns=need)
        parsed["_raw"] = parsed.astype(str).agg(",".join, axis=1)
        ok = pd.Series(True, index=parsed.index, name="_ok")

    # tipado numérico
    for c in ["open","high","low","close","volume"]:
        parsed[c] = pd.to_numeric(parsed[c], errors="coerce")

    # setear source_file
    src = df["__source_file"].iloc[0] if "__source_file" in df.columns and len(df) else pd.NA
    parsed["source_file"] = src

    # Guardar filas problemáticas a un CSV (una sola vez por llamada; si no estás dentro de Airflow, ajustá la ruta)
    bad = parsed[~ok].copy()
    if len(bad):
        # no “rompas” la corrida: solo deja registro
        out_bad = Path(OUTPUT_DIR) / f"pares_badrows_{pd.Timestamp.now().date()}.csv"
        out_bad.parent.mkdir(parents=True, exist_ok=True)
        # columnas útiles
        bad[["source_file","_raw"]].to_csv(out_bad, index=False)
        logging.warning(f"[pares] {len(bad)} filas no parseadas. Ver: {out_bad}")

    # salida final ordenada
    return parsed[["time","open","high","low","close","volume","source_file"]]



GROUP_TRANSFORMS = {
    "calendario": transform_calendario,
    "desastres": transform_desastres,
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
    """
    files = json.loads(ti.xcom_pull(key="input_files", task_ids="list_input_files"))

    root = Path(RAW_DIR)
    buckets: Dict[str, List[pd.DataFrame]] = {}

    for fpath in files:
        fpath = str(fpath)
        ext = Path(fpath).suffix.lower()

        # detectar grupo por subcarpeta
        rel = Path(fpath).relative_to(root)
        group = rel.parts[0] if len(rel.parts) > 1 else "root"
        rules = SCHEMA_RULES.get(group, SCHEMA_RULES["_default"])

        # leer
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
        raise ValueError("No se encontraron datos en ningún archivo válido")

    # Guardar intermedios por grupo
    inter_paths: List[str] = []
    Path(INTERIM_DIR).mkdir(parents=True, exist_ok=True)
    for group, frames in buckets.items():
        gdf = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        gpath = Path(INTERIM_DIR) / f"interim_{group}.parquet"

        # --- Arrow-safe: normalizar tipos antes de guardar ---
        # 1) Si existe 'time' y NO es datetime64[ns], forzar a string uniforme
        if "time" in gdf.columns and not pd.api.types.is_datetime64_any_dtype(gdf["time"]):
            gdf["time"] = gdf["time"].astype("string")

        # 2) Cualquier columna object => string (evita inferencias raras a int)
        for col in gdf.select_dtypes(include=["object"]).columns:
            gdf[col] = gdf[col].astype("string")

        # 3) Opcional: convertir dtypes donde aplique (no convierte 'string' a int)
        gdf = gdf.convert_dtypes()

        gdf = gdf.drop(columns=["__source_sheet", "__group"], errors="ignore")

        gdf.to_parquet(gpath, index=False)
        inter_paths.append(str(gpath))
        logging.info(f"[{group}] filas={len(gdf)} cols={list(gdf.columns)} -> {gpath}")

    if ti:
        ti.xcom_push(key="interim_paths", value=json.dumps(inter_paths))

def produce_outputs(ds: str, ti=None, **_):
    """
    Lee los intermedios por grupo y escribe un output final por cada grupo.
    Además genera (opcional) un 'final_global_{ds}' concatenando todos.
    Normaliza dtypes para que Parquet (pyarrow) no se rompa con mixes.
    """
    inter_paths = json.loads(ti.xcom_pull(key="interim_paths", task_ids="read_and_merge_files"))
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    final_paths = []
    per_group_frames = []

    for p in inter_paths:
        p = Path(p)
        if not p.exists():
            continue

        # El nombre del grupo viene del archivo intermedio: interim_{group}.parquet
        group = p.stem.replace("interim_", "", 1)

        # Cargar intermedio de ese grupo
        gdf = pd.read_parquet(p)

        # --- Normalización segura para Parquet ---
        # En 'pares' queremos 'time' como datetime; en el resto, string
        if "time" in gdf.columns:
            if group == "pares":
                gdf["time"] = pd.to_datetime(gdf["time"], errors="coerce")
            else:
                gdf["time"] = gdf["time"].astype("string")

        # Todas las columnas object -> string (evita mixes raros)
        for c in gdf.select_dtypes(include=["object"]).columns:
            gdf[c] = gdf[c].astype("string")

        # Homogeneizar el resto de tipos donde sea posible
        gdf = gdf.convert_dtypes()

        # --- escribir finales por grupo ---
        out_parquet = Path(OUTPUT_DIR) / f"final_{group}_{ds}.parquet"
        out_csv = Path(OUTPUT_DIR) / f"final_{group}_{ds}.csv"

        gdf.to_parquet(out_parquet, index=False)
        gdf.to_csv(out_csv, index=False)

        logging.info(f"[{group}] Guardado: {out_parquet} | {out_csv}")
        final_paths.extend([str(out_parquet), str(out_csv)])

        per_group_frames.append(gdf)

    # (Opcional) también un final global
    if per_group_frames:
        df_global = pd.concat(per_group_frames, ignore_index=True)

        # --- Normalización global ---
        if "time" in df_global.columns:
            # Globalmente dejamos 'time' como string (grupos heterogéneos)
            df_global["time"] = df_global["time"].astype("string")

        for c in df_global.select_dtypes(include=["object"]).columns:
            df_global[c] = df_global[c].astype("string")

        df_global = df_global.convert_dtypes()

        out_parquet_g = Path(OUTPUT_DIR) / f"final_global_{ds}.parquet"
        out_csv_g = Path(OUTPUT_DIR) / f"final_global_{ds}.csv"
        df_global.to_parquet(out_parquet_g, index=False)
        df_global.to_csv(out_csv_g, index=False)
        logging.info(f"[global] Guardado: {out_parquet_g} | {out_csv_g}")
        final_paths.extend([str(out_parquet_g), str(out_csv_g)])

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

