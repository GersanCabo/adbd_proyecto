import os
import sys
import json
import time
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DB_URL = os.getenv("DB_URL", "postgresql://usuario:password@localhost:5432/ecommerce_db")
CSV_PATH = os.getenv("CSV_PATH", "devoluciones.csv")

COLUMNAS_CSV_ESPERADAS = {"pedido_id", "fecha_devolucion", "motivo", "estado"}
FORMATOS_FECHA = ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y")

engine = create_engine(DB_URL)


def obtener_tipos_cambio(reintentos: int = 3, espera: int = 2) -> dict:
    """Extrae tipos de cambio respecto a EUR con reintento"""
    url = "https://api.frankfurter.app/latest?from=EUR"
    for intento in range(1, reintentos + 1):
        try:
            resp = requests.get(url, timeout=(5, 10))
            resp.raise_for_status()
            tasas = resp.json()["rates"]
            tasas["EUR"] = 1.0
            logging.info("Tipos de cambio obtenidos correctamente desde Frankfurter API.")
            return tasas
        except Exception as e:
            logging.warning(f"Intento {intento} fallido al consultar API de divisas: {e}")
            if intento < reintentos:
                time.sleep(espera * intento)
    raise RuntimeError("No fue posible obtener los tipos de cambio tras varios reintentos.")


def parsear_fechas_mixtas(serie: pd.Series) -> pd.Series:
    """Parsea formatos mixtos de forma determinista"""
    resultado = pd.Series(pd.NaT, index=serie.index, dtype="datetime64[ns]")
    for fmt in FORMATOS_FECHA:
        pendientes = (resultado.isna() & serie.notna() & (serie != "")).fillna(False).astype(bool)
        if not pendientes.any():
            break
        resultado.loc[pendientes] = pd.to_datetime(
            serie.loc[pendientes], format=fmt, errors="coerce"
        )
    return resultado


def limpiar_devoluciones(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Limpia espacios, elimina duplicaciones y separa registros válidos de los destinados a cuarentena."""
    faltantes = COLUMNAS_CSV_ESPERADAS - set(df_raw.columns)
    if faltantes:
        raise ValueError(f"El CSV no contiene las columnas esperadas: {sorted(faltantes)}")

    df = df_raw.copy()

    # Eliminar espacios sobrantes en todas las columnas y tratar cadenas vacías como NA
    for col in df.columns:
        df[col] = df[col].astype("string").str.strip().replace("", pd.NA)

    # Eliminar duplicados exactos antes de validar (ej. pedidos 102 y 112)
    filas_antes = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    duplicados_eliminados = filas_antes - len(df)
    if duplicados_eliminados:
        logging.info(f"Duplicados exactos eliminados del CSV: {duplicados_eliminados}")

    # Validaciones de calidad
    pedido_num = pd.to_numeric(df["pedido_id"], errors="coerce")
    pedido_valido = (
        pedido_num.notna() & (pedido_num > 0) & (pedido_num % 1 == 0)
    ).fillna(False).astype(bool)

    fecha_ok = parsear_fechas_mixtas(df["fecha_devolucion"])
    fecha_valida = fecha_ok.notna().astype(bool)

    estado_norm = df["estado"].str.capitalize()
    estado_valido = estado_norm.notna().astype(bool)

    # Registrar motivo exacto de rechazo por fila
    motivos = []
    for idx in df.index:
        errores = []
        if not pedido_valido.at[idx]:
            errores.append("pedido_id nulo o no entero")
        if not fecha_valida.at[idx]:
            errores.append("fecha_devolucion inválida")
        if not estado_valido.at[idx]:
            errores.append("estado nulo o vacío")
        motivos.append("; ".join(errores))

    df["_motivo_error"] = motivos
    es_valido = pedido_valido & fecha_valida & estado_valido

    invalidos = df[~es_valido].copy()
    validas = df[es_valido].copy()

    validas["pedido_id"] = pedido_num[es_valido].astype("int64")
    validas["fecha_devolucion_ok"] = fecha_ok[es_valido]
    validas["estado"] = estado_norm[es_valido]
    validas["motivo"] = validas["motivo"].fillna("Sin especificar")

    # Si hubiese varias devoluciones válidas para el mismo pedido_id, conservar la más reciente
    validas = (
        validas.sort_values("fecha_devolucion_ok")
        .drop_duplicates(subset=["pedido_id"], keep="last")
    )

    return validas, invalidos


def ejecutar_etl(carga_completa: bool = False):
    """
    Ejecuta el ciclo ETL completo.
    - carga_completa=False (por defecto): extracción incremental por pedido_id.
    - carga_completa=True (--full): reprocesa todos los pedidos aplicando UPSERT.
    """
    try:
        # 1. EXTRACCIÓN (PostgreSQL + CSV + API)
        logging.info("Iniciando extracción de datos...")

        ultimo_pedido_id = 0
        if not carga_completa:
            with engine.connect() as conn:
                ultimo_pedido_id = conn.execute(text("""
                    SELECT COALESCE(MAX(pedido_id), 0)
                    FROM analitica.ventas_consolidadas_eur
                """)).scalar()
            logging.info(f"Modo incremental activo: extrayendo pedidos con pedido_id > {ultimo_pedido_id}")
        else:
            logging.info("Modo carga completa (--full): reprocesando todos los pedidos.")

        query_sql = text("""
            SELECT p.pedido_id, p.cliente_id, c.nombre AS nombre_cliente,
                   c.pais, p.fecha_pedido, p.importe_original, p.divisa
            FROM tienda.pedidos p
            JOIN tienda.clientes c ON p.cliente_id = c.cliente_id
            WHERE p.pedido_id > :ultimo_id
            ORDER BY p.pedido_id ASC
        """)
        df_pedidos = pd.read_sql(query_sql, engine, params={"ultimo_id": ultimo_pedido_id})
        df_devoluciones_raw = pd.read_csv(CSV_PATH, dtype=str)
        tasas = obtener_tipos_cambio()

        filas_extraidas = len(df_pedidos)
        logging.info(f"Pedidos extraídos desde tienda.pedidos: {filas_extraidas}")

        # 2. TRANSFORMACIÓN Y VALIDACIÓN DE CALIDAD
        logging.info("Aplicando limpieza, validación y conversión de divisas...")
        dev_validas, invalidos = limpiar_devoluciones(df_devoluciones_raw)

        registros_ventas = []
        if not df_pedidos.empty:
            df_pedidos["divisa"] = df_pedidos["divisa"].astype(str).str.strip().str.upper()
            df_pedidos["importe_original"] = pd.to_numeric(
                df_pedidos["importe_original"], errors="raise"
            ).astype(float)

            df_final = df_pedidos.merge(
                dev_validas[["pedido_id", "estado"]],
                on="pedido_id",
                how="left"
            )
            df_final["tiene_devolucion"] = df_final["estado"].eq("Aprobada").fillna(False)
            df_final["tipo_cambio_eur"] = df_final["divisa"].map(tasas)

            if df_final["tipo_cambio_eur"].isna().any():
                divisas_err = df_final.loc[
                    df_final["tipo_cambio_eur"].isna(), "divisa"
                ].unique().tolist()
                raise ValueError(f"Divisas sin tipo de cambio en la API: {divisas_err}")

            df_final["tipo_cambio_eur"] = df_final["tipo_cambio_eur"].round(4)
            df_final["importe_eur"] = (
                df_final["importe_original"] / df_final["tipo_cambio_eur"]
            ).round(2)
            df_final["importe_neto_eur"] = df_final["importe_eur"].where(
                ~df_final["tiene_devolucion"], 0.0
            )

            registros_ventas = [
                {
                    "pid": int(row["pedido_id"]),
                    "cid": int(row["cliente_id"]),
                    "nombre": row["nombre_cliente"],
                    "pais": None if pd.isna(row["pais"]) else row["pais"],
                    "fecha": row["fecha_pedido"],
                    "divisa": row["divisa"],
                    "imp_orig": float(row["importe_original"]),
                    "tasa": float(row["tipo_cambio_eur"]),
                    "imp_eur": float(row["importe_eur"]),
                    "dev": bool(row["tiene_devolucion"]),
                    "imp_neto": float(row["importe_neto_eur"]),
                }
                for row in df_final.to_dict(orient="records")
            ]

        # Preparar registros de cuarentena
        fuente_csv = os.path.basename(CSV_PATH)[:50]
        cols_originales = [c for c in invalidos.columns if not c.startswith("_")]
        registros_cuarentena = []
        vistos = set()

        for row in invalidos.to_dict(orient="records"):
            datos_crudos = {
                c: (None if pd.isna(row[c]) else str(row[c])) for c in cols_originales
            }
            detalle = json.dumps(datos_crudos, sort_keys=True, ensure_ascii=False)
            motivo = row["_motivo_error"][:200]
            clave = (fuente_csv, detalle, motivo)
            if clave not in vistos:
                vistos.add(clave)
                registros_cuarentena.append({
                    "fuente": fuente_csv,
                    "detalle": detalle,
                    "motivo": motivo,
                })

        # 3. CARGA ATÓMICA, IDEMPOTENTE (UPSERT), CUARENTENA Y AUDITORÍA
        logging.info("Cargando datos en el esquema analitica de PostgreSQL...")
        with engine.begin() as conn:
            if registros_cuarentena:
                conn.execute(text("""
                    INSERT INTO analitica.registros_fallidos (fuente, detalle_registro, motivo_error)
                    SELECT CAST(:fuente AS VARCHAR(50)),
                           CAST(:detalle AS TEXT),
                           CAST(:motivo AS VARCHAR(200))
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM analitica.registros_fallidos
                        WHERE fuente = CAST(:fuente AS VARCHAR(50))
                          AND detalle_registro = CAST(:detalle AS TEXT)
                          AND motivo_error = CAST(:motivo AS VARCHAR(200))
                    )
                """), registros_cuarentena)

            if registros_ventas:
                conn.execute(text("""
                    INSERT INTO analitica.ventas_consolidadas_eur (
                        pedido_id, cliente_id, nombre_cliente, pais, fecha_pedido,
                        divisa_original, importe_original, tipo_cambio_eur,
                        importe_eur, tiene_devolucion, importe_neto_eur
                    ) VALUES (
                        :pid, :cid, :nombre, :pais, :fecha,
                        :divisa, :imp_orig, :tasa, :imp_eur, :dev, :imp_neto
                    )
                    ON CONFLICT (pedido_id) DO UPDATE SET
                        tipo_cambio_eur = EXCLUDED.tipo_cambio_eur,
                        importe_eur = EXCLUDED.importe_eur,
                        tiene_devolucion = EXCLUDED.tiene_devolucion,
                        importe_neto_eur = EXCLUDED.importe_neto_eur,
                        fecha_carga = CURRENT_TIMESTAMP
                """), registros_ventas)

            modo_txt = "carga completa" if carga_completa else f"incremental (pedido_id > {ultimo_pedido_id})"
            conn.execute(text("""
                INSERT INTO analitica.etl_log (
                    filas_extraidas, filas_cargadas, filas_rechazadas, estado, mensaje
                ) VALUES (:ext, :car, :rec, 'SUCCESS', :msg)
            """), {
                "ext": filas_extraidas,
                "car": len(registros_ventas),
                "rec": len(registros_cuarentena),
                "msg": f"Ejecución completada en modo {modo_txt}",
            })

        logging.info(
            f"Pipeline finalizado. Extraídas: {filas_extraidas} | "
            f"Cargadas: {len(registros_ventas)} | Cuarentena: {len(registros_cuarentena)}"
        )

    except Exception as e:
        logging.error(f"Error crítico en el pipeline ETL: {e}")
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO analitica.etl_log (
                        filas_extraidas, filas_cargadas, filas_rechazadas, estado, mensaje
                    ) VALUES (0, 0, 0, 'FAILED', :msg)
                """), {"msg": str(e)[:250]})
        except Exception as db_err:
            logging.error(f"No se pudo registrar el fallo en analitica.etl_log: {db_err}")
        raise


if __name__ == "__main__":
    modo_full = "--full" in sys.argv
    ejecutar_etl(carga_completa=modo_full)