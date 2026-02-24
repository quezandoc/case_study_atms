import json
import logging
import sqlparse
import functools
import time
from typing import Any, Callable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app_logs.log')
    ]
)

def pretty_format(val):
    # 1. Si es SQL (detectamos por palabras clave o el comentario --sql que usas)
    if isinstance(val, str) and any(kw in val.upper() for kw in ['SELECT', 'CREATE', 'WITH', 'INSERT', '--SQL']):
        formatted_sql = sqlparse.format(
            val.replace('--sql', ''), # Limpiamos el tag si existe
            keyword_case='upper',
            strip_comments=True
        )
        return f"\n--- SQL QUERY ---\n{formatted_sql.strip()}\n-----------------"

    if isinstance(val, dict):
        return f"\n{json.dumps(val, indent=4)}"
    
    return repr(val)

def format_if_sql(val):
    if isinstance(val, str) and val.strip().upper().startswith(('SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'CREATE')):
        return f"\n{sqlparse.format(val, reindent=True, keyword_case='upper')}\n"
    return repr(val)

def log_function(func: Callable) -> Callable:
    """
    Decorator that logs function execution time, arguments, and return values.
    Handles both exceptions and successful execution.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        logger = logging.getLogger(func.__module__)
        func_name = func.__name__

        p_args = [pretty_format(a) for a in args]
        p_kwargs = [f"{k}={pretty_format(v)}" for k, v in kwargs.items()]
        
        func_signature = f"{func_name}(\n    {', '.join(p_args + p_kwargs)}\n)"

        logger.info(f"🚀 Starting: {func_signature}")
        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            logger.info(f"Completed: {func_name} - Duration: {elapsed_time:.4f}s")
            return result
        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"Error in {func_name} after {elapsed_time:.4f}s: {type(e).__name__}: {str(e)}", exc_info=True)
            raise

    return wrapper
