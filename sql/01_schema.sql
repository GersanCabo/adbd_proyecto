-- Esquema operacional (Fuente 1)
CREATE SCHEMA IF NOT EXISTS tienda;

CREATE TABLE tienda.clientes (
    cliente_id INT PRIMARY KEY,
    nombre VARCHAR(120) NOT NULL,
    pais VARCHAR(60)
);

CREATE TABLE tienda.pedidos (
    pedido_id INT PRIMARY KEY,
    cliente_id INT REFERENCES tienda.clientes(cliente_id),
    fecha_pedido DATE NOT NULL,
    importe_original NUMERIC(10,2) NOT NULL,
    divisa VARCHAR(3) NOT NULL -- 'EUR', 'USD', 'GBP'
);

-- Esquema analítico destino
CREATE SCHEMA IF NOT EXISTS analitica;

CREATE TABLE analitica.ventas_consolidadas_eur (
    pedido_id INT PRIMARY KEY,
    cliente_id INT NOT NULL,
    nombre_cliente VARCHAR(120) NOT NULL,
    pais VARCHAR(60),
    fecha_pedido DATE NOT NULL,
    divisa_original VARCHAR(3) NOT NULL,
    importe_original NUMERIC(10,2) NOT NULL,
    tipo_cambio_eur NUMERIC(10,4) NOT NULL,
    importe_eur NUMERIC(12,2) NOT NULL,
    tiene_devolucion BOOLEAN NOT NULL DEFAULT FALSE,
    importe_neto_eur NUMERIC(12,2) NOT NULL,
    fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analitica.registros_fallidos (
    error_id SERIAL PRIMARY KEY,
    fecha_error TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fuente VARCHAR(50),
    detalle_registro TEXT,
    motivo_error VARCHAR(200)
);

CREATE TABLE analitica.etl_log (
    ejecucion_id SERIAL PRIMARY KEY,
    fecha_ejecucion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    filas_extraidas INT,
    filas_cargadas INT,
    filas_rechazadas INT,
    estado VARCHAR(20),
    mensaje TEXT
);