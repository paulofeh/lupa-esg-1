# src/data/cvm/xml_processor.py

import requests
from pathlib import Path
import zipfile
import re
from datetime import datetime
from src.utils.logging import setup_logging
from src.data.db.mongodb import MongoDB, ProcessingStatus
from src.config import settings
from .esg_extractor import ESGDataExtractor

class XMLProcessor:
    """Processa XMLs dos documentos FRE."""
    
    def __init__(self):
        """Inicializa o processador."""
        self.logger = setup_logging(__name__)
        self.db = MongoDB()
    
    def _download_zip(self, url: str, doc_id: str, cod_cvm: int) -> Path:
        """
        Faz o download do arquivo ZIP do documento.
        
        Args:
            url: URL do documento
            doc_id: ID do documento no MongoDB
            cod_cvm: Código CVM da empresa
            
        Returns:
            Path do arquivo ZIP baixado
        """
        try:
            self.logger.info(f"Iniciando download: {url}")
            
            response = requests.get(url)
            response.raise_for_status()
            
            # Usa o diretório específico da empresa
            company_dir = settings.get_company_temp_dir(cod_cvm)
            zip_path = company_dir / f"{doc_id}.zip"
            
            with open(zip_path, 'wb') as f:
                f.write(response.content)
                
            self.logger.info(f"Download concluído: {zip_path}")
            return zip_path
            
        except Exception as e:
            self.logger.error(f"Erro no download do ZIP: {str(e)}")
            raise

    def _extract_fre_xml(self, zip_path: Path, cod_cvm: int, dt_refer: datetime, versao: int) -> Path:
        """
        Extrai o XML do FRE do arquivo ZIP.
        
        Args:
            zip_path: Path do arquivo ZIP
            cod_cvm: Código CVM da empresa
            dt_refer: Data de referência
            versao: Versão do documento
            
        Returns:
            Path do arquivo XML extraído
        """
        try:
            self.logger.info(f"Processando ZIP: {zip_path}")
            
            # Formata o padrão esperado do nome do arquivo
            # Exemplo: 014206FRE31-12-2024v6
            dt_refer_str = dt_refer.strftime("%d-%m-%Y")
            expected_pattern = f"{int(cod_cvm):06d}FRE{dt_refer_str}v{versao}"
            
            with zipfile.ZipFile(zip_path, 'r') as zip_file:
                # Lista todos os arquivos XML
                xml_files = [f for f in zip_file.namelist() if f.endswith('.xml')]
                
                # Encontra o arquivo FRE
                fre_xml = None
                for xml in xml_files:
                    if 'FormularioCadastral' not in xml and expected_pattern in xml:
                        fre_xml = xml
                        break
                
                if not fre_xml:
                    raise ValueError(f"XML do FRE não encontrado para o padrão: {expected_pattern}")
                
                # Extrai para o diretório da empresa
                company_dir = settings.get_company_temp_dir(cod_cvm)
                xml_path = company_dir / fre_xml
                zip_file.extract(fre_xml, company_dir)
                
                self.logger.info(f"XML extraído: {xml_path}")
                return xml_path
                
        except Exception as e:
            self.logger.error(f"Erro ao extrair XML: {str(e)}")
            raise

    def process_pending_documents(self, limit: int = 5):
        """
        Processa documentos pendentes.
        """
        try:
            pending_docs = self.db.get_pending_documents(limit=limit)
            self.logger.info(f"Processando {len(pending_docs)} documentos pendentes")
            
            for doc in pending_docs:
                try:
                    # Extrai o código CVM e URL do documento
                    cod_cvm = doc.get('cod_cvm')
                    doc_url = doc.get('url')
                    
                    if not cod_cvm or not doc_url:
                        error_msg = f"Dados incompletos no documento: cod_cvm={cod_cvm}, url={doc_url}"
                        self.logger.error(error_msg)
                        self.db.update_document_status(
                            doc['_id'], 
                            ProcessingStatus.ERROR,
                            error=error_msg
                        )
                        continue
                    
                    # Atualiza status para downloading
                    self.db.update_document_status(doc['_id'], ProcessingStatus.DOWNLOADING)
                    
                    # Download do ZIP
                    zip_path = self._download_zip(doc_url, str(doc['_id']), cod_cvm)
                    
                    # Marca como downloaded
                    self.db.update_document_status(
                        doc['_id'], 
                        ProcessingStatus.DOWNLOADED,
                        metadata={'arquivos': {'zip_path': str(zip_path)}}
                    )
                    
                    # Extração do XML
                    self.db.update_document_status(doc['_id'], ProcessingStatus.PROCESSING)
                    
                    xml_path = self._extract_fre_xml(
                        zip_path,
                        cod_cvm,
                        doc['dt_referencia'],
                        doc['versao']
                    )
                    
                    # Atualiza status como XML extraído
                    self.db.update_document_status(
                        doc['_id'], 
                        ProcessingStatus.XML_EXTRACTED,
                        metadata={
                            'arquivos': {
                                'zip_path': str(zip_path),
                                'xml_path': str(xml_path)
                            }
                        }
                    )
                    
                    # Extração de dados ESG
                    try:
                        esg_extractor = ESGDataExtractor(
                            str(doc['_id']), 
                            xml_path,
                            cod_cvm
                        )
                        dados_esg = esg_extractor.extract_data()
                        
                        # Atualiza documento com dados ESG
                        self.db.update_document_status(
                            doc['_id'],
                            ProcessingStatus.PROCESSED,
                            metadata={
                                'arquivos': {
                                    'zip_path': str(zip_path),
                                    'xml_path': str(xml_path)
                                },
                                'dados_esg': dados_esg,
                                'stats': {
                                    'processado_em': datetime.utcnow().isoformat()
                                }
                            }
                        )
                        
                    except Exception as e:
                        error_msg = f"Erro na extração ESG: {str(e)}"
                        self.logger.error(error_msg)
                        self.db.update_document_status(
                            doc['_id'], 
                            ProcessingStatus.ERROR,
                            error=error_msg
                        )
                    
                except Exception as e:
                    error_msg = f"Erro ao processar documento: {str(e)}"
                    self.logger.error(error_msg)
                    self.db.update_document_status(
                        doc['_id'], 
                        ProcessingStatus.ERROR,
                        error=error_msg
                    )
                    
            return True
            
        except Exception as e:
            self.logger.error(f"Erro no processamento de documentos pendentes: {str(e)}")
            raise