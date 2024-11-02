import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from src.config import settings

def setup_logging(name: str) -> logging.Logger:
    """Configura e retorna um logger com handlers para arquivo e console."""
    
    # Criar o logger
    logger = logging.getLogger(name)
    logger.setLevel(settings.LOG_LEVEL)
    
    # Formato do log
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler para arquivo
    log_file = settings.BASE_DIR / 'logs' / f'{name}.log'
    log_file.parent.mkdir(exist_ok=True)
    
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10485760,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger