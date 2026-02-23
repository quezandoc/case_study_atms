dtype_mapping_device = {
    'id': 'Int64',
    'vehicle_id': 'Int32',
    'device_id': 'Int32',
    'vehicle_out_of_service': 'Int16',
    'vehicle_maintenance': 'Int16',
    'transmitting_dur': 'Int32',
    'not_transmitting_dur': 'Int32',
    'transmission_level': 'Int16'
}

dtype_mapping_sensor = {
    # --- Identifiers and Keys ---
    'id': 'Int64',                          # Identity column, 8-byte integer to avoid overflow
    'sensor_id': 'Int32',                   # Standard 4-byte integer
    'wheel_position': 'Int32',              # Standard 4-byte integer
    'wheel_id': 'Int32',                    # Standard 4-byte integer
    'vehicle_id': 'Int32',                  # Standard 4-byte integer
    
    # --- Flags and Categorical IDs (Smallint) ---
    'vehicle_out_of_service': 'Int16',      # 2-byte integer (0/1 or small codes)
    'vehicle_maintenance': 'Int16',         # 2-byte integer (0/1 or small codes)
    'sensor_priority': 'Int16',             # 2-byte integer
    'sensor_type_id': 'Int16',              # 2-byte integer
    'priority_order': 'Int32',              # Integer (az64 encoded)
    
    # --- Temperature Metrics (Durations and Counts) ---
    # Using Int32 (Nullable) to handle possible nulls from the CSV
    'level_1_high_temperature_dur': 'Int32',
    'level_2_high_temperature_dur': 'Int32',
    'level_3_high_temperature_dur': 'Int32',
    'level_1_low_temperature_dur': 'Int32',
    'level_2_low_temperature_dur': 'Int32',
    'level_3_low_temperature_dur': 'Int32',
    'optimal_temperature_dur': 'Int32',
    'level_1_high_temperature_cnt': 'Int32',
    'level_2_high_temperature_cnt': 'Int32',
    'level_3_high_temperature_cnt': 'Int32',
    'level_1_low_temperature_cnt': 'Int32',
    'level_2_low_temperature_cnt': 'Int32',
    'level_3_low_temperature_cnt': 'Int32',
    
    # --- Cold Pressure Metrics ---
    'level_1_high_cold_pressure_dur': 'Int32',
    'level_2_high_cold_pressure_dur': 'Int32',
    'level_3_high_cold_pressure_dur': 'Int32',
    'level_1_low_cold_pressure_dur': 'Int32',
    'level_2_low_cold_pressure_dur': 'Int32',
    'level_3_low_cold_pressure_dur': 'Int32',
    'optimal_cold_pressure_dur': 'Int32',
    'level_1_high_cold_pressure_cnt': 'Int32',
    'level_2_high_cold_pressure_cnt': 'Int32',
    'level_3_high_cold_pressure_cnt': 'Int32',
    'level_1_low_cold_pressure_cnt': 'Int32',
    'level_2_low_cold_pressure_cnt': 'Int32',
    'level_3_low_cold_pressure_cnt': 'Int32',
    
    # --- Hot Pressure Metrics ---
    'level_1_high_hot_pressure_dur': 'Int32',
    'level_2_high_hot_pressure_dur': 'Int32',
    'level_3_high_hot_pressure_dur': 'Int32',
    'level_1_low_hot_pressure_dur': 'Int32',
    'level_2_low_hot_pressure_dur': 'Int32',
    'level_3_low_hot_pressure_dur': 'Int32',
    'optimal_hot_pressure_dur': 'Int32',
    'level_1_high_hot_pressure_cnt': 'Int32',
    'level_2_high_hot_pressure_cnt': 'Int32',
    'level_3_high_hot_pressure_cnt': 'Int32',
    'level_1_low_hot_pressure_cnt': 'Int32',
    'level_2_low_hot_pressure_cnt': 'Int32',
    'level_3_low_hot_pressure_cnt': 'Int32',
    
    # --- Statistical Metrics ---
    # Float32 uses half the memory of the default float64
    'temperature_min': 'float32',
    'temperature_max': 'float32',
    'temperature_sd': 'float32',
    'temperature_avg': 'float32',
    'cold_pressure_min': 'float32',
    'cold_pressure_max': 'float32',
    'cold_pressure_sd': 'float32',
    'cold_pressure_avg': 'float32',
    'hot_pressure_min': 'float32',
    'hot_pressure_max': 'float32',
    'hot_pressure_sd': 'float32',
    'hot_pressure_avg': 'float32',
    
    # --- Transmission Monitoring ---
    'transmitting_dur': 'Int32',
    'not_transmitting_dur': 'Int32',
    'transmission_level': 'Int32'
}

date_cols = ['report_start_at', 'updated_at']
