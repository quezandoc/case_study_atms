from logging_decorator import log_function

@log_function
def create_temperature_states_table(con):
    query = f"""--sql
CREATE TABLE IF NOT EXISTS temperature_states (
    id VARCHAR,
    vehicle_id VARCHAR,
    device_id VARCHAR,
    vehicle_out_of_service BOOLEAN,
    vehicle_maintenance BOOLEAN,
    state VARCHAR,  -- low_1, low_2, low_3, optimal, high_1, high_2, high_3
    duration_seconds INTEGER,
    avg_temp DOUBLE,
    min_temp DOUBLE,
    max_temp DOUBLE,
    std_dev DOUBLE,
    report_start_at TIMESTAMP,
    updated_at TIMESTAMP,
    temp_range DOUBLE GENERATED ALWAYS AS (max_temp - min_temp) STORED);
    """

    return con.query(query)

@log_function
def anomaly_detection(con, start_date, end_date):
    
    query = f"""--sql
WITH base_stats AS (
    SELECT 
        vehicle_id,
        device_id,
        state,
        AVG(duration_seconds) as avg_duration,
        STDDEV(duration_seconds) as std_duration,
        AVG(avg_temp) as avg_temp_historical,
        STDDEV(avg_temp) as std_temp,
        AVG(std_dev) as avg_std_dev,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_seconds) as p95_duration,
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY duration_seconds) as p05_duration,
        COUNT(*) as n_observations
    FROM temperature_states
    WHERE report_start_at < '{start_date}'
    GROUP BY vehicle_id, device_id, state
    HAVING COUNT(*) >= 30  -- Minimum 30 observations for reliable stats
),

current_data AS (
    -- Datos del período actual
    SELECT *
    FROM temperature_states
    WHERE report_start_at BETWEEN '{start_date}' AND '{end_date}'
),

anomalies AS (
    SELECT 
        c.*,
        b.avg_duration,
        b.std_duration,
        
        -- ANOMALÍA 1: Duración inusual (Z-score > 3)
        CASE 
            WHEN ABS(c.duration_seconds - b.avg_duration) / NULLIF(b.std_duration, 0) > 3 
            THEN 'DURACION_EXTREMA'
            ELSE NULL
        END as anomaly_duration,
        
        -- ANOMALÍA 2: Temperatura fuera de rango histórico
        CASE 
            WHEN c.avg_temp > b.avg_temp_historical + 3 * b.std_temp 
                 OR c.avg_temp < b.avg_temp_historical - 3 * b.std_temp
            THEN 'TEMPERATURA_FUERA_RANGO'
            ELSE NULL
        END as anomaly_temp,
        
        -- ANOMALÍA 3: Alta variabilidad (std_dev anómalo)
        CASE 
            WHEN c.std_dev > b.avg_std_dev * 2 
            THEN 'ALTA_VARIABILIDAD'
            ELSE NULL
        END as anomaly_variability,
        
        -- ANOMALÍA 4: Transiciones rápidas (duración muy corta en estado extremo)
        CASE 
            WHEN c.state IN ('low_1', 'high_3') 
                 AND c.duration_seconds < b.p05_duration
            THEN 'TRANSICION_RAPIDA_EXTREMO'
            ELSE NULL
        END as anomaly_transition,
        
        -- ANOMALÍA 5: Rango térmico anómalo (max-min inusual)
        CASE 
            WHEN (c.max_temp - c.min_temp) > 5 * b.avg_std_dev
            THEN 'RANGO_TERMICO_ANOMALO'
            ELSE NULL
        END as anomaly_range,
        
        -- Score compuesto de anomalía
        (CASE WHEN ABS(c.duration_seconds - b.avg_duration) / NULLIF(b.std_duration, 0) > 3 THEN 1 ELSE 0 END +
         CASE WHEN c.avg_temp > b.avg_temp_historical + 3 * b.std_temp OR c.avg_temp < b.avg_temp_historical - 3 * b.std_temp THEN 1 ELSE 0 END +
         CASE WHEN c.std_dev > b.avg_std_dev * 2 THEN 1 ELSE 0 END +
         CASE WHEN c.state IN ('low_1', 'high_3') AND c.duration_seconds < b.p05_duration THEN 1 ELSE 0 END +
         CASE WHEN (c.max_temp - c.min_temp) > 5 * b.avg_std_dev THEN 1 ELSE 0 END
        ) as anomaly_score
    FROM current_data c
    LEFT JOIN base_stats b 
        ON c.vehicle_id = b.vehicle_id 
        AND c.device_id = b.device_id 
        AND c.state = b.state
)

SELECT *
FROM anomalies
WHERE anomaly_score > 0
ORDER BY anomaly_score DESC, vehicle_id, report_start_at
    """

    return con.query(query)