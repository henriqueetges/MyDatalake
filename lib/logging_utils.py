from datetime import datetime, timezone
import logging
import os
import json
import traceback

class JSONFormatter(logging.Formatter):
  def format(self, record):
    log_record = {
      'timestamp': datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
      'level': record.levelname, 
      'message': record.getMessage(),
      'logger': record.name,
      'module': record.module,
    }
    if record.exc_info:
      log_record['exception'] = ''.join(traceback.format_exception(*record.exc_info))
    return json.dumps(log_record)

def setup_logging(log_file: str, logger_name: str, level=logging.INFO):
    """
    Configures logging to output to both a file and the notebook/console.
    Saves the log file in a 'logs' folder one level up from the current script.

    Parameters:
        log_file (str): Name of the log file.
        level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
    """
  
    logs_dir = '/Workspace/Users/ike.etges@gmail.com/Datalake/logs'
    os.makedirs(logs_dir, exist_ok=True)
    log_path = os.path.join(logs_dir, log_file)  
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    while logger.handlers:    
        handler = logger.handlers[0]
        logger.removeHandler(handler)
        handler.close()
    
    while logger.root.handlers:
        handler = logger.root.handlers[0]
        logger.root.removeHandler(handler)
        handler.close()
    
    file_handler = logging.FileHandler(log_path)
    stream_handler = logging.StreamHandler()
    formatter = JSONFormatter()
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger, file_handler

        
        
    
            

