import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Diretório base do projeto
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Configurações básicas
PROJECT_NAME = os.getenv('PROJECT_NAME', 'lupa_esg')
ENVIRONMENT = os.getenv('ENV', 'development')

# Configuração de logs
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Diretório temporário base
TEMP_DIR = BASE_DIR / 'data' / 'temp'
TEMP_DIR.mkdir(parents=True, exist_ok=True)

def get_company_temp_dir(cod_cvm: int) -> Path:
    """
    Retorna o diretório temporário específico de uma empresa.
    Cria o diretório apenas quando chamado.
    """
    company_dir = TEMP_DIR / f"{cod_cvm:06d}_files"
    company_dir.mkdir(exist_ok=True)  # Cria apenas quando necessário
    return company_dir