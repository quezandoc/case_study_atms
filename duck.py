import duckdb
import pandas as pd
from logging_decorator import log_function

# Load the data into a pandas DataFrame and return it
@log_function
def load_data(file_header, file_data, dtype_mapping=None, date_columns=None):
    # 1. Obtain column names from the header file
    metadata = pd.read_csv(file_header, sep='|')
    columns = metadata['column_name'].tolist()
    
    # 2. Load the data file using the obtained column names and specified data types
    # Using low_memory=False to avoid dtype inference issues with large files
    dataframe = pd.read_csv(
        file_data, 
        header=0, 
        names=columns, 
        sep=',',
        dtype=dtype_mapping,
        parse_dates=date_columns,
        low_memory=False
    )
    return dataframe

class DuckDBAnalyzer:
    def __init__(self):
        self.conn = duckdb.connect(':memory:')
    
    @log_function
    def register_dataframe(self, name, path_header, path_data, dtype_mapping=None, date_columns=None):
        """Register DataFrame as a view/table without copying"""
        df = load_data(path_header, path_data, dtype_mapping, date_columns)
        self.conn.register(name, df)
        print(f"Registered DataFrame '{name}'")
    
    @log_function
    def query(self, sql):
        return self.conn.execute(sql).fetchdf()
    
    def close(self):
        self.conn.close()

# Usage - DuckDB is faster for large datasets and complex analytics
# analyzer = DuckDBAnalyzer()
# analyzer.register_dataframe(large_df, 'sales')
# result = analyzer.query("SELECT * FROM sales WHERE amount > 1000")