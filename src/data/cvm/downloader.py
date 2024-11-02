import logging
from pathlib import Path
import requests
from tqdm import tqdm
from src.config import settings
from src.utils.logging import setup_logging  # Corrigido de setup_logger para setup_logging
from src.config import settings

logger = setup_logging(__name__)  # Corrigido aqui também

class CVMDownloader:
    def __init__(self):
        self.base_url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/"
        self.temp_dir = settings.TEMP_DIR
    
    def download_file(self, url: str, filename: str, cod_cvm: int = None) -> Path:
        """
        Faz o download de um arquivo com barra de progresso.
        
        Args:
            url: URL do arquivo para download
            filename: Nome do arquivo local
            cod_cvm: Código CVM da empresa (opcional)
            
        Returns:
            Path do arquivo baixado
        """
        # Se tiver cod_cvm, usa diretório específico da empresa
        if cod_cvm is not None:
            output_dir = settings.get_company_temp_dir(cod_cvm)
        else:
            output_dir = self.temp_dir
            
        output_path = output_dir / filename
        
        try:
            logger.info(f"Iniciando download de: {url}")
            
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(output_path, 'wb') as file, \
                 tqdm(
                    desc=filename,
                    total=total_size,
                    unit='iB',
                    unit_scale=True
                 ) as progress_bar:
                
                for data in response.iter_content(chunk_size=8192):
                    size = file.write(data)
                    progress_bar.update(size)
            
            logger.info(f"Download concluído: {output_path}")
            return output_path
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro no download do arquivo {url}: {str(e)}")
            raise
    
    def download_latest_fre(self) -> Path:
        """
        Faz o download do arquivo ZIP mais recente do FRE.
        
        Returns:
            Path do arquivo ZIP baixado
        """
        import datetime
        current_year = datetime.datetime.now().year
        filename = f"fre_cia_aberta_{current_year}.zip"
        url = f"{self.base_url}/{filename}"
        
        return self.download_file(url, filename)