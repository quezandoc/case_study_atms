from duck import DuckDBAnalyzer
from data_types import dtype_mapping_device, dtype_mapping_sensor, date_cols
from logging_decorator import log_function
import logging
import time

class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'

logger = logging.getLogger(__name__)

def set_colored_bold(message, color):
    return f"{color}{Colors.BOLD}{message}{Colors.END}"

def query_to_duckdb(analyzer, query):
        
    logger.info(f"Executing query: {query}")
    start_time = time.time()
    result = analyzer.query(query)
    end_time= time.time()
    logger.info(f"Query executed in {end_time - start_time:.4f} seconds")
    return result

if __name__ == "__main__":
    @log_function
    def main():
        logger.info("Starting ATMS Data Analysis Application")
        #######################################################################
        # First i'll load the data from the csv files into duckDB for analyze ##
        #######################################################################
        time_in_level_device_desc = "data/time_in_level_device_desc.csv"
        time_in_level_device_data = "data/time_in_level_device.csv"

        time_in_level_sensor_desc = "data/time_in_level_sensor_desc.csv"
        time_in_level_sensor_data = "data/time_in_level_sensor.csv"

        analyzer = DuckDBAnalyzer()

        logger.info("Registering device data...")
        analyzer.register_dataframe( 'time_in_level_device',
            time_in_level_device_desc, time_in_level_device_data, 
            dtype_mapping_device, date_cols)
        
        logger.info("Registering sensor data...")
        analyzer.register_dataframe( 'time_in_level_sensor',
            time_in_level_sensor_desc, time_in_level_sensor_data, 
            dtype_mapping_sensor, date_cols)
        
        # Comparing data from both tables with a JOIN to validate the data integrity and relationships
        query_mixed_tables = f"""
SELECT * FROM time_in_level_device AS d
JOIN time_in_level_sensor AS s ON d.vehicle_id = s.vehicle_id
        """

        # logger.info("Executing query to compare device and sensor data...")
        # result_mixed_tables = analyzer.query(query_mixed_tables)
        # print(f"Result of mixed tables query: {set_colored_bold(f'{result_mixed_tables.shape[0]}', Colors.RED)} rows")
        # print(set_colored_bold(result_mixed_tables.head(), Colors.CYAN))

        #### Experimenting with different queries to analyze the data and validate the results ####
        ## Comparing data transmission rates for a specific vehicle and date range to validate the data consistency and identify any anomalies in the transmission patterns ##
        # Query parameters
        vehicle_id = 84
        start_date = '2023-03-01'
        end_date = '2023-04-01'

        ## Here I can see that device report sending isn't related to sensor report sending
        query_transmission_rate_comparison = f"""
SELECT d.id as d_id, d.vehicle_id, d.device_id, d.transmission_level as d_tr, d.transmitting_dur as d_tdur, d.not_transmitting_dur as d_ntdur, d.report_start_at as drsat, d.updated_at as duat,
    s.id, s.sensor_id, wheel_id, wheel_position, s.transmission_level as s_tr, s.transmitting_dur as s_tdur, s.not_transmitting_dur as s_ntdur, s.report_start_at as ssat, s.updated_at as suat
FROM time_in_level_device AS d
LEFT JOIN time_in_level_sensor AS s ON d.vehicle_id = s.vehicle_id AND s.report_start_at >= '{start_date}' AND s.report_start_at < '{end_date}'
WHERE d.vehicle_id = '{vehicle_id}' 
    AND d.report_start_at >= '{start_date}' 
    AND d.report_start_at < '{end_date}'
ORDER BY d.report_start_at DESC, s.report_start_at DESC;
        """

        logger.info("Executing query for devices...")
        result_transmission_rate_comparison = analyzer.query(query_transmission_rate_comparison)
        print(f"Result of device query: {set_colored_bold(f'{result_transmission_rate_comparison.shape[0]}', Colors.RED)} rows")
        print(set_colored_bold(result_transmission_rate_comparison, Colors.CYAN))

        ## Filters for vehicle_id and date range to analyze the transmission patterns for each sensor and identify any correlations between temperature, cold pressure, hot pressure levels and the transmission behavior of the sensors ##
        query_every_sensor_transmission_temperature_for_vehicle = f"""
SELECT s.id, s.sensor_id, s.wheel_id as swid, s.wheel_position as swpos, s.transmission_level as s_tr, 
    s.level_1_high_temperature_dur as s_l1htd, s.level_2_high_temperature_dur as s_l2htd, level_3_high_temperature_dur as s_l3htd, s.level_1_low_temperature_dur as s_l1ltd, s.level_2_low_temperature_dur as s_l2ltd, s.level_3_low_temperature_dur as s_l3ltd, optimal_temperature_dur as s_otd,
    s.level_1_high_temperature_cnt as s_l1htc, s.level_2_high_temperature_cnt as s_l2htc, level_3_high_temperature_cnt as s_l3htc, s.level_1_low_temperature_cnt as s_l1ltc, s.level_2_low_temperature_cnt as s_l2ltc, s.level_3_low_temperature_cnt as s_l3ltc,
    s.transmitting_dur as s_tdur, s.not_transmitting_dur as s_ntdur, s.report_start_at as ssat
FROM time_in_level_sensor AS s
WHERE s.vehicle_id = '{vehicle_id}' 
    AND s.report_start_at >= '{start_date}' 
    AND s.report_start_at < '{end_date}'
ORDER BY s.report_start_at DESC;
        """

        logger.info("Executing query for devices...")
        result_every_sensor_transmission_temperature_for_vehicle = analyzer.query(query_every_sensor_transmission_temperature_for_vehicle)
        print(f"Result of device query: {set_colored_bold(f'{result_every_sensor_transmission_temperature_for_vehicle.shape[0]}', Colors.RED)} rows")
        print(set_colored_bold(result_every_sensor_transmission_temperature_for_vehicle, Colors.CYAN))
        
        query_every_sensor_transmission_cold_pressure_for_vehicle = f"""
SELECT s.id, s.sensor_id, s.wheel_id as swid, s.wheel_position as swpos, s.transmission_level as s_tr, 
    s.level_1_high_cold_pressure_dur as s_l1htd, s.level_2_high_cold_pressure_dur as s_l2htd, level_3_high_cold_pressure_dur as s_l3htd, s.level_1_low_cold_pressure_dur as s_l1ltd, s.level_2_low_cold_pressure_dur as s_l2ltd, s.level_3_low_cold_pressure_dur as s_l3ltd, optimal_cold_pressure_dur as s_otd,
    s.level_1_high_cold_pressure_cnt as s_l1htc, s.level_2_high_cold_pressure_cnt as s_l2htc, level_3_high_cold_pressure_cnt as s_l3htc, s.level_1_low_cold_pressure_cnt as s_l1ltc, s.level_2_low_cold_pressure_cnt as s_l2ltc, s.level_3_low_cold_pressure_cnt as s_l3ltc,
    s.transmitting_dur as s_tdur, s.not_transmitting_dur as s_ntdur, s.report_start_at as ssat
FROM time_in_level_sensor AS s
WHERE s.vehicle_id = '{vehicle_id}' 
    AND s.report_start_at >= '{start_date}' 
    AND s.report_start_at < '{end_date}'
ORDER BY s.report_start_at DESC;
        """

        logger.info("Executing query for devices...")
        result_every_sensor_transmission_cold_pressure_for_vehicle = analyzer.query(query_every_sensor_transmission_cold_pressure_for_vehicle)
        print(f"Result of device query: {set_colored_bold(f'{result_every_sensor_transmission_cold_pressure_for_vehicle.shape[0]}', Colors.RED)} rows")
        print(set_colored_bold(result_every_sensor_transmission_cold_pressure_for_vehicle, Colors.CYAN))

        query_every_sensor_transmission_hot_pressure_for_vehicle = f"""
SELECT s.id, s.sensor_id, s.wheel_id as swid, s.wheel_position as swpos, s.transmission_level as s_tr, 
    s.level_1_high_hot_pressure_dur as s_l1htd, s.level_2_high_hot_pressure_dur as s_l2htd, level_3_high_hot_pressure_dur as s_l3htd, s.level_1_low_hot_pressure_dur as s_l1ltd, s.level_2_low_hot_pressure_dur as s_l2ltd, s.level_3_low_hot_pressure_dur as s_l3ltd, optimal_hot_pressure_dur as s_otd,
    s.level_1_high_hot_pressure_cnt as s_l1htc, s.level_2_high_hot_pressure_cnt as s_l2htc, level_3_high_hot_pressure_cnt as s_l3htc, s.level_1_low_hot_pressure_cnt as s_l1ltc, s.level_2_low_hot_pressure_cnt as s_l2ltc, s.level_3_low_hot_pressure_cnt as s_l3ltc,
    s.transmitting_dur as s_tdur, s.not_transmitting_dur as s_ntdur, s.report_start_at as ssat
FROM time_in_level_sensor AS s
WHERE s.vehicle_id = '{vehicle_id}' 
    AND s.report_start_at >= '{start_date}' 
    AND s.report_start_at < '{end_date}'
ORDER BY s.report_start_at DESC;
        """

        logger.info("Executing query for devices...")
        result_every_sensor_transmission_hot_pressure_for_vehicle = analyzer.query(query_every_sensor_transmission_hot_pressure_for_vehicle)
        print(f"Result of device query: {set_colored_bold(f'{result_every_sensor_transmission_hot_pressure_for_vehicle.shape[0]}', Colors.RED)} rows")
        print(set_colored_bold(result_every_sensor_transmission_hot_pressure_for_vehicle, Colors.CYAN))



        # Understanding how the levels works
        query_temperature = f"""
SELECT 
    vehicle_id,
    vehicle_out_of_service AS v_out_of_service,
    vehicle_maintenance AS v_maintenance,
    sensor_id,
    wheel_position,
    wheel_id,
    optimal_temperature_dur AS temp_optimal_dur,
    level_1_high_temperature_dur AS temp_l1_high_dur,
    level_2_high_temperature_dur AS temp_l2_high_dur,
    level_3_high_temperature_dur AS temp_l3_high_dur,
    level_1_low_temperature_dur AS temp_l1_low_dur,
    level_2_low_temperature_dur AS temp_l2_low_dur,
    level_3_low_temperature_dur AS temp_l3_low_dur,
    temperature_min,
    temperature_max,
    temperature_sd,
    temperature_avg,
    report_start_at,
    updated_at
FROM time_in_level_sensor
WHERE 
    (level_1_high_temperature_dur > 0 
    OR level_2_high_temperature_dur > 0 
    OR level_3_high_temperature_dur > 0
    OR level_1_low_temperature_dur > 0 
    OR level_2_low_temperature_dur > 0 
    OR level_3_low_temperature_dur > 0)
    AND vehicle_id = {vehicle_id}
ORDER BY 
    report_start_at DESC, 
    vehicle_id, 
    sensor_id, 
    wheel_position;
        """

        logger.info("Executing query for temperature anomalies...")
        result_temperature = analyzer.query(query_temperature)
        print(f"Result of temperature query: {set_colored_bold(f'{result_temperature.shape[0]}', Colors.RED)} rows")
        print(set_colored_bold(result_temperature, Colors.CYAN))

        logger.info("Exporting temperature anomalies to CSV...")
        result_temperature.to_csv("output/temperature_anomalies.csv", index=False)

        query_cold_pressure = f"""
SELECT 
    vehicle_id,
    vehicle_out_of_service AS v_out_of_service,
    vehicle_maintenance AS v_maintenance,
    sensor_id,
    wheel_position,
    wheel_id,
    optimal_cold_pressure_dur AS cold_p_optimal_dur,
    level_1_high_cold_pressure_dur AS cold_p_l1_high_dur,
    level_2_high_cold_pressure_dur AS cold_p_l2_high_dur,
    level_3_high_cold_pressure_dur AS cold_p_l3_high_dur,
    level_1_low_cold_pressure_dur AS cold_p_l1_low_dur,
    level_2_low_cold_pressure_dur AS cold_p_l2_low_dur,
    level_3_low_cold_pressure_dur AS cold_p_l3_low_dur,
    cold_pressure_min,
    cold_pressure_max,
    cold_pressure_sd,
    cold_pressure_avg,
    report_start_at,
    updated_at
FROM time_in_level_sensor
WHERE 
    (level_1_high_cold_pressure_dur > 0 
    OR level_2_high_cold_pressure_dur > 0 
    OR level_3_high_cold_pressure_dur > 0
    OR level_1_low_cold_pressure_dur > 0 
    OR level_2_low_cold_pressure_dur > 0 
    OR level_3_low_cold_pressure_dur > 0)
    AND vehicle_id = {vehicle_id}
ORDER BY 
    report_start_at DESC, 
    vehicle_id, 
    sensor_id, 
    wheel_position;
       """

        logger.info("Executing query for cold pressure anomalies...")
        result_cold_pressure = analyzer.query(query_cold_pressure)
        print(f"Result of cold pressure query: {set_colored_bold(f'{result_cold_pressure.shape[0]}', Colors.RED)} rows")
        print(set_colored_bold(result_cold_pressure, Colors.CYAN))

        logger.info("Exporting cold pressure anomalies to CSV...")
        result_cold_pressure.to_csv("output/cold_pressure_anomalies.csv", index=False)


        query_hot_pressure = f"""
SELECT 
    vehicle_id,
    vehicle_out_of_service AS v_out_of_service,
    vehicle_maintenance AS v_maintenance,
    sensor_id,
    wheel_position,
    wheel_id,
    optimal_hot_pressure_dur AS hot_p_optimal_dur,
    level_1_high_hot_pressure_dur AS hot_p_l1_high_dur,
    level_2_high_hot_pressure_dur AS hot_p_l2_high_dur,
    level_3_high_hot_pressure_dur AS hot_p_l3_high_dur,
    level_1_low_hot_pressure_dur AS hot_p_l1_low_dur,
    level_2_low_hot_pressure_dur AS hot_p_l2_low_dur,
    level_3_low_hot_pressure_dur AS hot_p_l3_low_dur,
    hot_pressure_min,
    hot_pressure_max,
    hot_pressure_sd,
    hot_pressure_avg,
    report_start_at,
    updated_at
FROM time_in_level_sensor
WHERE 
    (level_1_high_hot_pressure_dur > 0 
    OR level_2_high_hot_pressure_dur > 0 
    OR level_3_high_hot_pressure_dur > 0
    OR level_1_low_hot_pressure_dur > 0 
    OR level_2_low_hot_pressure_dur > 0 
    OR level_3_low_hot_pressure_dur > 0)
    AND vehicle_id = {vehicle_id}
ORDER BY 
    report_start_at DESC, 
    vehicle_id, 
    sensor_id, 
    wheel_position;
        """

        logger.info("Executing query for hot pressure anomalies...")
        result_hot_pressure = analyzer.query(query_hot_pressure)
        print(f"Result of hot pressure query: {set_colored_bold(f'{result_hot_pressure.shape[0]}', Colors.RED)} rows")
        print(set_colored_bold(result_hot_pressure, Colors.CYAN))

        logger.info("Exporting hot pressure anomalies to CSV...")
        result_hot_pressure.to_csv("output/hot_pressure_anomalies.csv", index=False)


        query_not_transmission = f"""
SELECT 
    vehicle_id,
    vehicle_out_of_service AS v_out_of_service,
    vehicle_maintenance AS v_maintenance,
    device_id,
    transmitting_dur,
    not_transmitting_dur,
    transmission_level,
    report_start_at,
    updated_at
FROM time_in_level_device
WHERE 
    not_transmitting_dur > 86000
ORDER BY 
    report_start_at DESC, 
    vehicle_id, 
    device_id;
        """

        logger.info("Executing query for not transmitting devices...")
        result_not_transmission = analyzer.query(query_not_transmission)
        print(f"Result of not transmitting devices query: {set_colored_bold(f'{result_not_transmission.shape[0]}', Colors.RED)} rows")
        print(set_colored_bold(result_not_transmission, Colors.CYAN))

        logger.info("Exporting not transmitting devices to CSV...")
        result_not_transmission.to_csv("output/not_transmitting_devices.csv", index=False)


        ## Finishing up and closing the connection ##
        analyzer.close()
        logger.info("Application completed successfully")
    
    main()
