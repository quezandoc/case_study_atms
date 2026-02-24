import logging

from duck import DuckDBAnalyzer
from logging_decorator import log_function

from query_functions import generate_device_health_report, generate_fleet_health_report, sensor_anomaly_detection
from data_types import dtype_mapping_device, dtype_mapping_sensor, date_cols

class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    @log_function
    def main():
        logger.info("Starting ATMS Data Analysis Application")
        #######################################################################
        # First i'll load the data from the csv files into duckDB for analyze ##
        #######################################################################

        START_DATE = '2023-01-01'
        END_DATE = '2023-12-31'

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
        #######################################################################
        # Now that the data is loaded, we can create the necessary tables and perform the analysis to detect anomalies in temperature states. ##
        #######################################################################

        logger.info("Performing comprehensive fleet health analysis...")
        generate_fleet_health_report(analyzer, START_DATE, END_DATE)
        logger.info("Performing comprehensive device health analysis...")
        generate_device_health_report(analyzer, START_DATE, END_DATE)
        logger.info("Performing comprehensive anomaly detection...")
        sensor_anomaly_detection(analyzer, START_DATE, END_DATE)

    
    main()