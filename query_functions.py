import logging
from duckdb import df
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)

def sensor_anomaly_detection(analyzer, start_date, end_date, output_path='output/anomalies_from_sensors.csv'):
    """
    Performs a Hybrid Anomaly Detection (Statistical + Machine Learning).
    
    Strategies:
    1. Statistical (DuckDB): Calculates Z-Scores for Temperature, Cold Pressure, and Hot Pressure
       based on a 7-day moving window per sensor.
    2. ML (Isolation Forest): Detects multivariate anomalies (e.g., mismatch between 
       temperature and pressure, or abnormal duration patterns).
    """
    
    logger.info(f"Starting Comprehensive Anomaly Detection ({start_date} to {end_date})...")

    # -------------------------------------------------------------------------
    # STEP 1: Statistical Analysis
    # -------------------------------------------------------------------------
    # We calculate Z-Scores for 3 variables: Temp, Cold Press, Hot Press
    query_stats = f"""--sql
WITH daily_metrics AS (
    SELECT 
        id,
        report_start_at::DATE as report_date,
        sensor_id,
        vehicle_id,
        wheel_position,
        
        -- Key Metrics
        temperature_avg,
        cold_pressure_avg,
        hot_pressure_avg,
        
        -- Critical Durations (Sum of Level 3 Low/High for context) utils for ML
        (COALESCE(level_3_high_temperature_dur, 0) + COALESCE(level_3_low_temperature_dur, 0)) as crit_temp_dur,
        (COALESCE(level_3_high_cold_pressure_dur, 0) + COALESCE(level_3_low_cold_pressure_dur, 0)) as crit_cold_dur,
        (COALESCE(level_3_high_hot_pressure_dur, 0) + COALESCE(level_3_low_hot_pressure_dur, 0)) as crit_hot_dur
    
    FROM time_in_level_sensor
    WHERE report_start_at BETWEEN '{start_date}' AND '{end_date}'
),

rolling_window AS (
    SELECT 
        *,
        -- Rolling Stats for Temperature (7 days)
        AVG(temperature_avg) OVER w AS ma_temp,
        STDDEV(temperature_avg) OVER w AS sd_temp,
        
        -- Rolling Stats for Cold Pressure (7 days)
        AVG(cold_pressure_avg) OVER w AS ma_cold,
        STDDEV(cold_pressure_avg) OVER w AS sd_cold,

        -- Rolling Stats for Hot Pressure (7 days)
        AVG(hot_pressure_avg) OVER w AS ma_hot,
        STDDEV(hot_pressure_avg) OVER w AS sd_hot,

        AVG(crit_temp_dur) OVER w AS ma_dur_temp,
        STDDEV(crit_temp_dur) OVER w AS sd_dur_temp

    FROM daily_metrics
    WINDOW w AS (PARTITION BY sensor_id ORDER BY report_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)

SELECT 
    *,
    -- Calculate Z-Scores (Handle division by zero)
    CASE WHEN sd_temp = 0 OR sd_temp IS NULL THEN 0 ELSE (temperature_avg - ma_temp) / sd_temp END as z_temp,
    CASE WHEN sd_cold = 0 OR sd_cold IS NULL THEN 0 ELSE (cold_pressure_avg - ma_cold) / sd_cold END as z_cold,
    CASE WHEN sd_hot = 0 OR sd_hot IS NULL THEN 0 ELSE (hot_pressure_avg - ma_hot) / sd_hot END as z_hot,

    CASE WHEN sd_dur_temp = 0 OR sd_dur_temp IS NULL THEN 0 ELSE (crit_temp_dur - ma_dur_temp) / sd_dur_temp END as z_dur_temp
FROM rolling_window
ORDER BY report_date, sensor_id;
    """
    
    # Load data into Pandas for the ML step
    df = analyzer.query(query_stats)
    df.fillna(0, inplace=True) # Handle basic NaNs
    
    logger.info(f"Statistical features extracted. Rows: {len(df)}")

    # -------------------------------------------------------------------------
    # STEP 2: Machine Learning (Isolation Forest)
    # -------------------------------------------------------------------------
    # We use this to find weird COMBINATIONS of data that Z-Score misses.
    # e.g., High Temp + Low Hot Pressure (Physically unlikely)
    
    logger.info("Training Isolation Forest Model...")
    
    # Features for the model: A mix of averages and critical durations
    features = [
        'temperature_avg', 'cold_pressure_avg', 'hot_pressure_avg',
        'crit_temp_dur', 'crit_cold_dur', 'crit_hot_dur',
        'z_temp', 'z_cold', 'z_hot' # Feeding z-scores helps the model understand trend deviation
    ]
    
    X = df[features]
    
    # Scale data (Important for ML models)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Isolation Forest: contamination='auto' lets it decide the % of outliers
    iso_forest = IsolationForest(n_estimators=100, contamination=0.04, random_state=42)
    
    # Predict: -1 is Anomaly, 1 is Normal
    df['ml_anomaly_pred'] = iso_forest.fit_predict(X_scaled)
    
    # Anomaly Score (Lower is more anomalous)
    df['ml_anomaly_score'] = iso_forest.decision_function(X_scaled)

    # -------------------------------------------------------------------------
    # STEP 3: Combine & Interpret Results
    # -------------------------------------------------------------------------
    
    # Define flags logic
    # Z-Score Anomaly: If any Z-score > 3 (3 standard deviations)
    df['is_temp_anomaly'] = (df['z_temp'].abs() > 3).astype(int)
    df['is_cold_press_anomaly'] = (df['z_cold'].abs() > 3).astype(int)
    df['is_hot_press_anomaly'] = (df['z_hot'].abs() > 3).astype(int)
    
    # ML Anomaly: If prediction is -1
    df['is_ml_anomaly'] = (df['ml_anomaly_pred'] == -1).astype(int)
    
    # Final "Global Anomaly" flag (Union of both strategies)
    df['is_anomaly_global'] = df[['is_temp_anomaly', 'is_cold_press_anomaly', 'is_hot_press_anomaly', 'is_ml_anomaly']].max(axis=1)

    # SMART REASONING: Explain "Why" the ML triggered
    def get_detailed_reason(row):
        reasons = []
        
        # Obvious Statistical Failures (Priority 1)
        if row['is_temp_anomaly']: reasons.append("Extreme Temp")
        if row['is_cold_press_anomaly']: reasons.append("Extreme Cold Press")
        if row['is_hot_press_anomaly']: reasons.append("Extreme Hot Press")
        
        # If ML triggered but NO statistical failure occurred (The "Hidden" Anomaly)
        if row['is_ml_anomaly'] == 1 and not reasons:
            # We investigate "Near Misses" (Z-scores between 2 and 3, or unbalanced relations)
            ml_reasons = []
            
            # Check for moderate deviations that look suspicious together
            if abs(row['z_temp']) > 2: ml_reasons.append("Unusual Temp Trend")
            if abs(row['z_cold']) > 2: ml_reasons.append("Unusual Pressure Trend")
            if abs(row['z_dur_temp']) > 2: ml_reasons.append("Abnormal Time in Risk State")
            
            # Check for Physics Mismatch (Example logic)
            # High Temp but Low Pressure is weird
            if row['z_temp'] > 1.5 and row['z_cold'] < -1.5:
                ml_reasons.append("Physics Mismatch (High T / Low P)")
            
            if ml_reasons:
                reasons.append(f"Pattern: {' & '.join(ml_reasons)}")
            else:
                # If we still can't explain it easily, it's a complex multivariate outlier
                reasons.append("Complex Multivariate Pattern")
        
        # Fallback for ML combined with Stats
        elif row['is_ml_anomaly'] == 1:
             reasons.append("(Confirmed by AI)")

        return ", ".join(reasons) if reasons else "Normal"

    df['anomaly_detail_text'] = df.apply(get_detailed_reason, axis=1)

    # Clean Category for Charts
    def get_category(row):
        if row['is_temp_anomaly']: return 'Thermal Failure'
        if row['is_cold_press_anomaly'] or row['is_hot_press_anomaly']: return 'Pressure Failure'
        if row['is_ml_anomaly']: return 'Behavioral Anomaly (AI)' # Alerts without simple threshold breach
        return 'Normal'

    df['anomaly_category'] = df.apply(get_category, axis=1)

    # Export
    # Select user-friendly columns for PowerBI
    final_cols = [
        'id', 'report_date', 'sensor_id', 'vehicle_id', 'wheel_position',
        'temperature_avg', 'cold_pressure_avg', 'hot_pressure_avg',
        'is_anomaly_global', 'anomaly_category', 'anomaly_detail_text',
        'z_temp', 'z_cold', 'z_hot' # Keep Zs for tooltips
    ]
    
    df_final = df[final_cols]
    df_final.to_csv(output_path, index=False)
    
    logger.info(f"Analysis Complete. Global Anomalies Detected: {df['is_anomaly_global'].sum()}")
    return df_final

def generate_fleet_health_report(analyzer, start_date, end_date, output_folder='output/'):
    """
    Generates a comprehensive fleet health report based on SENSOR (Wheel) data.
    Focuses on Sensor Availability (Are the wheels sending data?)
    """
    logger.info(f"Generating Sensor/Wheel Health Report ({start_date} to {end_date})...")

    # =========================================================================
    # SENSOR AVAILABILITY (% Transmitting)
    # =========================================================================
    # Logic: 
    # - Daily: Count unique sensors sending data vs Total unique sensors known.
    # - "Transmitting" implies transmitting_dur > 0
    
    query_availability = f"""--sql
WITH total_fleet AS (
    -- Get the total distinct sensors that SHOULD exist (baseline)
    SELECT COUNT(DISTINCT sensor_id) as total_fleet_count 
    FROM time_in_level_sensor
),
daily_stats AS (
    SELECT 
        report_start_at::DATE as report_date,
        COUNT(DISTINCT sensor_id) as active_sensors,
        (SELECT total_fleet_count FROM total_fleet) as total_sensors
    FROM time_in_level_sensor
    WHERE transmitting_dur > 0 -- Only count if they actually transmitted
      AND report_start_at BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1
)
SELECT 
    *,
    ROUND((active_sensors::FLOAT / total_sensors) * 100, 2) as availability_pct,
    STRFTIME(report_date, '%Y-%m') as month_year
FROM daily_stats
ORDER BY report_date;
    """
    
    df_avail = analyzer.query(query_availability)
    
    df_avail['availability_pct'] = df_avail['availability_pct'] / 100
    # Calculate Aggregates for the executive summary
    overall_availability = df_avail['availability_pct'].mean() if not df_avail.empty else 0
    monthly_availability = df_avail.groupby('month_year')['availability_pct'].mean().reset_index()
    
    logger.info(f"Overall Sensor Availability: {overall_availability * 100:.2f}%")

    df_avail.to_csv(f"{output_folder}report_sensor_availability_daily.csv", index=False)
    monthly_availability.to_csv(f"{output_folder}report_sensor_availability_monthly.csv", index=False)
    

def generate_device_health_report(analyzer, start_date, end_date, output_folder='output/'):
    """
    Generates statistics specifically for the CONCENTRATOR DEVICES (Hubs).
    Focuses on connectivity, transmission levels, and vehicle status.
    Uses table: time_in_level_device
    """
    logger.info(f"Generating Device (Hub) Connectivity Report ({start_date} to {end_date})...")

    # =========================================================================
    # 1. DEVICE AVAILABILITY & CONNECTIVITY (Daily Efficiency)
    # =========================================================================
    # Logic:
    # - Analyzes how "talkative" the device is.
    # - Uptime % = Transmitting Time / Total Time (Transmit + Silence)
    
    query_device_connectivity = f"""--sql
SELECT
    device_id,
    vehicle_id,
    report_start_at::DATE as report_date,
    -- Connectivity Statistics
    AVG(transmitting_dur) as avg_transmit_sec,
    AVG(not_transmitting_dur) as avg_silence_sec,
    -- Calculated Uptime % (Connectivity Efficiency)
    ROUND(
        SUM(transmitting_dur)::FLOAT /
        NULLIF(SUM(transmitting_dur + not_transmitting_dur), 0) * 100
    , 2) as connectivity_efficiency_pct

FROM time_in_level_device
WHERE report_start_at BETWEEN '{start_date}' AND '{end_date}'
GROUP BY 1, 2, 3
ORDER BY report_date ASC; -- Worst connectivity first
    """
    
    df_connectivity = analyzer.query(query_device_connectivity)
    df_connectivity['connectivity_efficiency_pct'] = df_connectivity['connectivity_efficiency_pct'] / 100
    df_connectivity.to_csv(f"{output_folder}report_device_connectivity.csv", index=False)
    
    # =========================================================================
    # 2. FLEET STATUS OVERVIEW (Maintenance vs Service)
    # =========================================================================
    # Logic:
    # - operational_fleet: Vehicles active and running.
    # - vehicles_in_maintenance: Downtime due to mechanics.
    # - vehicles_out_of_service: Downtime due to decommission/other.
    
    query_fleet_status = f"""--sql
SELECT
    report_start_at::DATE as report_date,

    -- Total Hub Count
    COUNT(DISTINCT device_id) as total_devices,

    -- Status Breakdown
    SUM(CASE WHEN vehicle_out_of_service > 0 THEN 1 ELSE 0 END) as vehicles_out_of_service,
    SUM(CASE WHEN vehicle_maintenance > 0 THEN 1 ELSE 0 END) as vehicles_in_maintenance,

    -- Active Fleet (Ready to work)
    SUM(CASE WHEN vehicle_out_of_service = 0 AND vehicle_maintenance = 0 THEN 1 ELSE 0 END) as operational_fleet

FROM time_in_level_device
WHERE report_start_at BETWEEN '{start_date}' AND '{end_date}'
GROUP BY 1
ORDER BY report_date;
    """
    
    df_status = analyzer.query(query_fleet_status)
    df_status.to_csv(f"{output_folder}report_device_fleet_status.csv", index=False)
    
    logger.info("Device (Hub) Stats generated.")
    
    return {
        "worst_device_id": df_connectivity.iloc[0]['device_id'] if not df_connectivity.empty else None,
        "worst_device_efficiency": df_connectivity.iloc[0]['connectivity_efficiency_pct'] if not df_connectivity.empty else 0
    }