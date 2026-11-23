-- Captura 4
SELECT
    pedido_id,
    divisa_original AS divisa,
    importe_original,
    tipo_cambio_eur AS tasa_eur,
    importe_eur,
    (ABS(ROUND(importe_original / tipo_cambio_eur, 2) - importe_eur) <= 0.01) AS conversion_ok,
    tiene_devolucion AS devolucion,
    importe_neto_eur,
    (
        (tiene_devolucion = TRUE  AND importe_neto_eur = 0.00) OR
        (tiene_devolucion = FALSE AND importe_neto_eur = importe_eur)
    ) AS neto_ok
FROM analitica.ventas_consolidadas_eur
ORDER BY pedido_id;

-- Captura 5
SELECT
    r.error_id,
    r.fuente,
    r.detalle_registro::jsonb ->> 'pedido_id' AS pedido_id_csv,
    r.detalle_registro::jsonb ->> 'fecha_devolucion' AS fecha_csv,
    r.detalle_registro::jsonb ->> 'estado' AS estado_csv,
    r.motivo_error,
    NOT EXISTS (
        SELECT 1
        FROM analitica.ventas_consolidadas_eur v
        WHERE v.pedido_id = CASE
                WHEN (r.detalle_registro::jsonb ->> 'pedido_id') ~ '^[0-9]+$'
                THEN (r.detalle_registro::jsonb ->> 'pedido_id')::INT
              END
          AND v.tiene_devolucion = TRUE
    ) AS aislado_ok
FROM analitica.registros_fallidos r
ORDER BY r.error_id;

-- Captura 6
-- 1. Historial de ejecuciones consecutivas en analitica.etl_log
SELECT
    ejecucion_id,
    TO_CHAR(fecha_ejecucion, 'YYYY-MM-DD HH24:MI:SS') AS fecha,
    filas_extraidas,
    filas_cargadas,
    filas_rechazadas,
    estado
FROM analitica.etl_log
ORDER BY ejecucion_id ASC;