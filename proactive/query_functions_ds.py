import logging
import pandas as pd
import numpy as np
from duckdb import df

logger = logging.getLogger(__name__)

# -------------------- Configuration Parameters --------------------
SLOPE_WINDOW_DAYS = 7
OVERINFLATION_THRESHOLD = 0.3          # fraction of transmitting time
SLOW_LEAK_SLOPE_THRESHOLD = -0.5       # psi per day
TEMP_SLOPE_THRESHOLD = 0.5             # °C per day
PUNCTURE_DROP_THRESHOLD = 10           # psi drop in 1 day
PUNCTURE_LEVEL3_DUR_THRESHOLD = 0.1    # fraction of transmitting time
THERMAL_RATIO_THRESHOLD = 2.0
FROZEN_STD_THRESHOLD = 0.001           # effectively zero variation
HUB_EFFICIENCY_THRESHOLD = 0.3         # 50% connectivity
# ------------------------------------------------------------------

def generate_mechanical_failure_report(analyzer, start_date, end_date,
                                       output_sensor_path='output/sensor_anomalies.csv',
                                       output_device_path='output/hub_anomalies.csv',
                                       output_sensor_variables_path='output/sensor_variables.csv',
                                       output_sensor_statistics_path='output/sensor_statistics.csv',
                                       output_device_statistics_path='output/device_statistics.csv',
                                       ):
    """
    Generates a daily report classifying sensor and hub issues into five buckets:
    - Slow Leak (procedural deflation)
    - Puncture (rapid deflation)
    - Over-Inflation
    - Hardware Fail (sensor/hub malfunction)
    - Thermal Stress
    Also includes 'Normal' for assets without issues.

    Returns a DataFrame with columns:
        report_date, asset_id, asset_type ('sensor' or 'device'),
        issue_category, risk_score (0-100)
    """
    logger.info(f"Starting mechanical failure analysis ({start_date} to {end_date})...")

    # Step 1: Extract sensor-level features
    sensor_anomalies_df = _extract_sensor_features(analyzer, start_date, end_date)

    # Step 2: Extract device-level features
    device_anomalies_df = _extract_device_features(analyzer, start_date, end_date)

    # Step 3: Classify sensor issues
    sensor_anomalies_df = _classify_sensor_issues(sensor_anomalies_df)

    # Step 4: Classify device issues
    device_anomalies_df = _classify_device_issues(device_anomalies_df)

    # Step 5: Combine and format output
    output_sensor_anomalies_df = sensor_anomalies_df[['id', 'report_date', 'vehicle_id', 'sensor_id', 'wheel_position', 'wheel_id',
                           'issue_category', 'risk_score']]
    output_sensor_anomalies_df.to_csv(output_sensor_path, index=False)

    logger.info(f"Report saved to {output_sensor_path}. Total rows: {len(output_sensor_anomalies_df)}")

    output_device_anomalies_df = device_anomalies_df[['id', 'report_date', 'vehicle_id', 'device_id',
                                 'issue_category', 'risk_score']]
    output_device_anomalies_df.to_csv(output_device_path, index=False)
    logger.info(f"Report saved to {output_device_path}. Total rows: {len(output_device_anomalies_df)}")

    output_sensor_variables_df = sensor_anomalies_df[['id', 'report_date', 'temperature_avg', 'cold_pressure_avg', 'hot_pressure_avg']]
    output_sensor_variables_df.to_csv(output_sensor_variables_path, index=False)
    logger.info(f"Report saved to {output_sensor_variables_path}. Total rows: {len(output_sensor_variables_df)}")

    output_sensor_statistics_df = sensor_anomalies_df[['id', 'report_date', 'transmitting_dur']]
    output_sensor_statistics_df.to_csv(output_sensor_statistics_path, index=False)
    logger.info(f"Report saved to {output_sensor_statistics_path}. Total rows: {len(output_sensor_statistics_df)}")

    output_device_statistics_df = device_anomalies_df[['id', 'report_date', 'transmitting_dur', 'not_transmitting_dur', 'connectivity_efficiency', 'active_sensors']]
    output_device_statistics_df.to_csv(output_device_statistics_path, index=False)
    logger.info(f"Report saved to {output_device_statistics_path}. Total rows: {len(output_device_statistics_df)}")a

    return output_sensor_anomalies_df, output_device_anomalies_df


def _extract_sensor_features(analyzer, start_date, end_date):
    """Query sensor table and compute rolling slopes, deltas, and indices."""
    query = f"""--sql
    WITH daily AS (
        SELECT
            id,
            wheel_position,
            wheel_id,
            sensor_id,
            vehicle_id,
            report_start_at::DATE AS report_date,
            temperature_avg,
            cold_pressure_avg,
            hot_pressure_avg,
            transmitting_dur,
            level_1_high_cold_pressure_dur,
            level_2_high_hot_pressure_dur,
            level_2_high_temperature_dur,
            level_3_high_temperature_dur,
            level_3_low_cold_pressure_dur,
            level_3_low_cold_pressure_cnt,
            -- global day number for slope calculations
            report_start_at::DATE - (SELECT MIN(report_start_at::DATE) FROM time_in_level_sensor) AS day_num
        FROM time_in_level_sensor
        WHERE report_start_at BETWEEN '{start_date}' AND '{end_date}'
    ),
    rolling AS (
        SELECT
            *,
            -- rolling sums over {SLOPE_WINDOW_DAYS}-day window
            SUM(cold_pressure_avg) OVER w AS sum_y_cold,
            SUM(temperature_avg)   OVER w AS sum_y_temp,
            SUM(day_num)            OVER w AS sum_x,
            SUM(day_num * cold_pressure_avg) OVER w AS sum_xy_cold,
            SUM(day_num * temperature_avg)   OVER w AS sum_xy_temp,
            SUM(day_num * day_num)            OVER w AS sum_xx,
            COUNT(*)                OVER w AS n,
            STDDEV_SAMP(temperature_avg) OVER w AS std_temp,
            STDDEV_SAMP(cold_pressure_avg) OVER w AS std_cold,
            -- lagged values for day‑over‑day changes
            LAG(cold_pressure_avg) OVER (PARTITION BY sensor_id ORDER BY report_date) AS prev_cold,
            LAG(temperature_avg)   OVER (PARTITION BY sensor_id ORDER BY report_date) AS prev_temp,
            LAG(hot_pressure_avg)  OVER (PARTITION BY sensor_id ORDER BY report_date) AS prev_hot,
            LAG(report_date)        OVER (PARTITION BY sensor_id ORDER BY report_date) AS prev_date
        FROM daily
        WINDOW w AS (PARTITION BY sensor_id ORDER BY report_date
                     ROWS BETWEEN {SLOPE_WINDOW_DAYS-1} PRECEDING AND CURRENT ROW)
    )
    SELECT
        *,
        -- cold pressure slope (psi/day)
        CASE
            WHEN n * sum_xx - sum_x * sum_x = 0 THEN 0.0
            ELSE (n * sum_xy_cold - sum_x * sum_y_cold) / (n * sum_xx - sum_x * sum_x)
        END AS cold_pressure_slope,
        -- temperature slope (°C/day)
        CASE
            WHEN n * sum_xx - sum_x * sum_x = 0 THEN 0.0
            ELSE (n * sum_xy_temp - sum_x * sum_y_temp) / (n * sum_xx - sum_x * sum_x)
        END AS temperature_slope,
        -- day‑over‑day changes (only if days are consecutive)
        CASE WHEN prev_date = report_date - INTERVAL 1 DAY
             THEN cold_pressure_avg - prev_cold ELSE NULL END AS delta_cold,
        CASE WHEN prev_date = report_date - INTERVAL 1 DAY
             THEN temperature_avg - prev_temp   ELSE NULL END AS delta_temp,
        CASE WHEN prev_date = report_date - INTERVAL 1 DAY
             THEN hot_pressure_avg - prev_hot   ELSE NULL END AS delta_hot,
        -- thermal/pressure ratio
        CASE WHEN delta_hot != 0 THEN delta_temp / delta_hot ELSE NULL END AS thermal_pressure_ratio,
        -- over‑inflation index
        (level_1_high_cold_pressure_dur + level_2_high_hot_pressure_dur) /
            NULLIF(transmitting_dur, 0) AS over_inflation_index,
        -- high temperature duration ratio
        (level_2_high_temperature_dur + level_3_high_temperature_dur) /
            NULLIF(transmitting_dur, 0) AS high_temp_dur_ratio
    FROM rolling
    ORDER BY sensor_id, report_date;
    """

    df = analyzer.query(query)
    # Fill NaNs introduced by lag/division
    df.fillna({'delta_cold': 0, 'delta_temp': 0, 'delta_hot': 0,
               'thermal_pressure_ratio': 0, 'over_inflation_index': 0,
               'high_temp_dur_ratio': 0, 'std_temp': 0, 'std_cold': 0}, inplace=True)
    logger.info(f"Sensor features extracted: {len(df)} rows")
    return df


def _extract_device_features(analyzer, start_date, end_date):
    """Query device table and join with sensor counts per vehicle."""
    query = f"""--sql
    WITH device_daily AS (
        SELECT
            id,
            vehicle_id,
            device_id,
            report_start_at::DATE AS report_date,
            transmitting_dur,
            not_transmitting_dur,
            transmitting_dur / NULLIF(transmitting_dur + not_transmitting_dur, 0) AS connectivity_efficiency,
            vehicle_maintenance,
            vehicle_out_of_service
        FROM time_in_level_device
        WHERE report_start_at BETWEEN '{start_date}' AND '{end_date}'
    ),
    sensor_counts AS (
        SELECT
            vehicle_id,
            report_start_at::DATE AS report_date,
            COUNT(DISTINCT sensor_id) AS active_sensors
        FROM time_in_level_sensor
        WHERE transmitting_dur > 0
          AND report_start_at BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY vehicle_id, report_date
    )
    SELECT
        d.*,
        COALESCE(s.active_sensors, 0) AS active_sensors
    FROM device_daily d
    LEFT JOIN sensor_counts s
        ON d.vehicle_id = s.vehicle_id AND d.report_date = s.report_date
    ORDER BY d.vehicle_id, d.report_date;
    """
    df = analyzer.query(query)
    df.fillna({'connectivity_efficiency': 0}, inplace=True)
    logger.info(f"Device features extracted: {len(df)} rows")
    return df


def _classify_sensor_issues(df):
    """Apply sensor-level classification logic."""
    # Boolean flags for each condition
    df['is_slow_leak'] = (
        (df['cold_pressure_slope'] < SLOW_LEAK_SLOPE_THRESHOLD) &
        (df['temperature_slope'].abs() < TEMP_SLOPE_THRESHOLD)
    )

    df['is_puncture'] = (
        (df['delta_cold'] < -PUNCTURE_DROP_THRESHOLD) &
        (df['level_3_low_cold_pressure_dur'] / df['transmitting_dur'].replace(0, np.nan)
         > PUNCTURE_LEVEL3_DUR_THRESHOLD) &
        (df['transmitting_dur'] > 0)
    ).fillna(False)

    df['is_over_inflation'] = (
        (df['over_inflation_index'] > OVERINFLATION_THRESHOLD) &
        (df['high_temp_dur_ratio'] < 0.1)  # low concurrent high temperature
    )

    df['is_thermal_stress'] = (
        (df['thermal_pressure_ratio'] > THERMAL_RATIO_THRESHOLD) &
        (df['delta_temp'] > 0) &
        (df['delta_hot'] < 1)  # pressure barely rises
    )

    df['is_sensor_malfunction'] = (
        (df['transmitting_dur'] > 0) & (
            (df['std_temp'] < FROZEN_STD_THRESHOLD) |
            (df['std_cold'] < FROZEN_STD_THRESHOLD) |
            (df['temperature_avg'] < -50) | (df['temperature_avg'] > 150) |
            (df['cold_pressure_avg'] < 0) | (df['cold_pressure_avg'] > 200)
        )
    )

    # Assign category based on priority (puncture highest)
    conditions = [
        df['is_puncture'],
        df['is_over_inflation'],
        df['is_slow_leak'],
        df['is_thermal_stress'],
        df['is_sensor_malfunction']
    ]
    choices = ['Puncture', 'Over-Inflation', 'Slow Leak', 'Thermal Stress', 'Hardware Fail']
    df['issue_category'] = np.select(conditions, choices, default='Normal')

    # Compute risk score (0-100)
    risk_score = pd.Series(0.0, index=df.index, dtype='float64')

    # Slow Leak: slope magnitude relative to threshold, capped at 100
    mask = df['is_slow_leak']
    risk_score[mask] = np.minimum(100, (df.loc[mask, 'cold_pressure_slope'].abs()
                                        / abs(SLOW_LEAK_SLOPE_THRESHOLD) * 100))

    # Puncture: combination of duration fraction and count
    mask = df['is_puncture']
    risk_score[mask] = np.minimum(100,
        (df.loc[mask, 'level_3_low_cold_pressure_dur'] / df.loc[mask, 'transmitting_dur'] * 100) +
        df.loc[mask, 'level_3_low_cold_pressure_cnt'] * 5)

    # Over-Inflation: directly from index
    mask = df['is_over_inflation']
    risk_score[mask] = np.minimum(100, df.loc[mask, 'over_inflation_index'] * 100)

    # Thermal Stress: ratio relative to threshold
    mask = df['is_thermal_stress']
    risk_score[mask] = np.minimum(100, df.loc[mask, 'thermal_pressure_ratio']
                                   / THERMAL_RATIO_THRESHOLD * 100)

    # Sensor Malfunction: 80 for frozen, 100 for impossible values
    mask = df['is_sensor_malfunction']
    frozen_mask = mask & ((df['std_temp'] < FROZEN_STD_THRESHOLD) |
                          (df['std_cold'] < FROZEN_STD_THRESHOLD))
    impossible_mask = mask & ((df['temperature_avg'] < -50) | (df['temperature_avg'] > 150) |
                              (df['cold_pressure_avg'] < 0) | (df['cold_pressure_avg'] > 200))
    risk_score[frozen_mask] = 80
    risk_score[impossible_mask] = 100

    # Normal assets get risk score 0
    df['risk_score'] = risk_score.fillna(0).astype(int)

    return df


def _classify_device_issues(df):
    """Apply device-level classification (hub malfunction)."""
    df['has_hub'] = df['device_id'].notna()

    df['is_hub_malfunction'] = (
        df['has_hub'] &
        (
            (df['connectivity_efficiency'] < HUB_EFFICIENCY_THRESHOLD) |
            (df['active_sensors'] == 0)
        ) &
        (df['vehicle_maintenance'] == 0) &
        (df['vehicle_out_of_service'] == 0)
    )

    conditions = [
        ~df['has_hub'],  # No hub installed
        df['is_hub_malfunction'],  # Hub exists but malfunctioning
    ]
    choices = ['No Hardware', 'Hardware Fail']
    
    df['issue_category'] = np.select(
        conditions, 
        choices, 
        default='Normal'  # Hub exists and working properly
    )

    # Risk score: 100 - efficiency (if efficiency low) or 100 if zero sensors
    risk_score = pd.Series(0.0, index=df.index, dtype='float64')
    mask = df['is_hub_malfunction']
    zero_sensors_mask = mask & (df['active_sensors'] == 0)
    low_eff_mask = mask & ~zero_sensors_mask

    risk_score[zero_sensors_mask] = 100
    risk_score[low_eff_mask] = 100 - df.loc[low_eff_mask, 'connectivity_efficiency'] * 100
    df['risk_score'] = risk_score.fillna(0).astype(int)

    return df