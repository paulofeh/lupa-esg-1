import pandas as pd
from pathlib import Path
import zipfile
from src.utils.logging import setup_logging
from typing import Dict, Any

logger = setup_logging(__name__)

class CVMProcessor:
    """Processa os arquivos de dados da CVM."""
    
    def __init__(self, zip_path: Path):
        self.zip_path = zip_path
    
    def extract_csv(self) -> pd.DataFrame:
        """
        Extrai o CSV do arquivo ZIP e carrega em um DataFrame.
        
        Returns:
            DataFrame com os dados do CSV
        """
        try:
            logger.info(f"Extraindo dados do arquivo: {self.zip_path}")
            
            with zipfile.ZipFile(self.zip_path, 'r') as zip_file:
                # Encontra o arquivo CSV dentro do ZIP
                csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                if not csv_files:
                    raise ValueError("Nenhum arquivo CSV encontrado no ZIP")
                
                csv_file = csv_files[0]
                logger.info(f"Arquivo CSV encontrado: {csv_file}")
                
                # Lê o CSV com encoding 'latin1' em vez de 'utf-8'
                with zip_file.open(csv_file) as f:
                    df = pd.read_csv(
                        f,
                        sep=';',
                        encoding='latin1',  # Alterado aqui
                        parse_dates=['DT_REFER', 'DT_RECEB']
                    )
                
                logger.info(f"CSV carregado com sucesso: {len(df)} registros encontrados")
                return df
                
        except Exception as e:
            logger.error(f"Erro ao processar arquivo ZIP: {str(e)}")
            raise
    
    def get_latest_documents(self) -> pd.DataFrame:
        """
        Obtém os documentos mais recentes de cada empresa.
        
        Returns:
            DataFrame com o último documento de cada empresa
        """
        try:
            df = self.extract_csv()
            
            # Ordena por CNPJ, data de recebimento e versão
            df_sorted = df.sort_values(
                by=['CNPJ_CIA', 'DT_RECEB', 'VERSAO'],
                ascending=[True, False, False]
            )
            
            # Pega o primeiro registro (mais recente) de cada empresa
            latest_docs = df_sorted.groupby('CNPJ_CIA').first().reset_index()
            
            logger.info(f"Processamento concluído: {len(latest_docs)} documentos únicos encontrados")
            return latest_docs
        
        except Exception as e:
            logger.error(f"Erro ao processar documentos: {str(e)}")
            raise