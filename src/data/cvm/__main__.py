# src/data/cvm/__main__.py
from pathlib import Path
import sys
import os
from datetime import datetime

# Adiciona o diretório raiz ao PATH para imports relativos funcionarem
root_dir = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(root_dir))

from src.data.cvm.downloader import CVMDownloader
from src.data.cvm.processor import CVMProcessor
from src.data.cvm.xml_processor import XMLProcessor
from src.data.cvm.esg_extractor import ESGDataExtractor
from src.data.db.mongodb import MongoDB, ProcessingStatus
from src.utils.logging import setup_logging
from src.config import settings

logger = setup_logging(__name__)

def process_initial_data():
    """
    Função para download e processamento inicial dos dados da CVM.
    """
    db = MongoDB()
    downloader = CVMDownloader()
    
    try:
        # Download do arquivo principal
        logger.info("Iniciando download do arquivo principal da CVM...")
        zip_path = downloader.download_latest_fre()
        logger.info(f"Download concluído com sucesso: {zip_path}")
        
        # Processamento do arquivo
        processor = CVMProcessor(zip_path)
        latest_docs = processor.get_latest_documents()
        
        processed = 0
        total_companies = len(latest_docs)
        logger.info(f"Processando dados de {total_companies} empresas...")
        
        for _, doc in latest_docs.iterrows():
            try:
                doc_dict = doc.to_dict()
                
                # Atualiza empresa
                db.upsert_company(doc_dict)
                
                # Insere documento
                db.insert_document(doc_dict)
                
                processed += 1
                if processed % 10 == 0:
                    logger.info(f"Progresso: {processed}/{total_companies} empresas processadas")
                
            except Exception as e:
                logger.error(f"Erro ao processar empresa {doc_dict.get('DENOM_CIA')}: {str(e)}")
                continue
        
        logger.info(f"Processamento inicial concluído: {processed}/{total_companies} empresas processadas")
        return True
        
    except Exception as e:
        logger.error(f"Erro durante o processamento inicial: {str(e)}")
        return False

def process_pending_documents(limit: int = 5):
    """
    Processa documentos pendentes no banco de dados.
    """
    try:
        xml_processor = XMLProcessor()
        
        # Processa documentos pendentes
        success = xml_processor.process_pending_documents(limit=limit)
        
        if success:
            logger.info(f"Processamento de documentos pendentes concluído com sucesso")
            
            # Log de status dos PDFs (opcional)
            try:
                db = MongoDB()
                processed_docs = db.documents.find({
                    "status": ProcessingStatus.PROCESSED.value
                }).limit(limit)
                
                for doc in processed_docs:
                    if 'metadata' in doc and 'arquivos' in doc['metadata']:
                        temp_dir = settings.get_company_temp_dir(doc['cod_cvm'])
                        pdf_dir = temp_dir / 'pdfs'
                        if pdf_dir.exists():
                            num_pdfs = len(list(pdf_dir.glob('*.pdf')))
                            logger.info(f"Empresa {doc['cod_cvm']}: {num_pdfs} PDFs extraídos")
            except Exception as e:
                logger.warning(f"Erro ao gerar log de PDFs: {str(e)}")
                
        return success
        
    except Exception as e:
        logger.error(f"Erro no processamento de documentos pendentes: {str(e)}")
        return False

def cleanup_temp_files(days_old: int = 7):
    """
    Limpa arquivos temporários antigos.
    
    Args:
        days_old: Remove arquivos mais antigos que este número de dias
    """
    try:
        from datetime import timedelta
        
        temp_dir = settings.TEMP_DIR
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        for item in temp_dir.glob('**/'):
            if item.is_file():
                if datetime.fromtimestamp(item.stat().st_mtime) < cutoff_date:
                    item.unlink()
            elif item.is_dir() and not any(item.iterdir()):
                item.rmdir()
                
        logger.info(f"Limpeza de arquivos temporários concluída")
                
    except Exception as e:
        logger.error(f"Erro durante limpeza de arquivos temporários: {str(e)}")

def main():
    """Função principal do pipeline de dados."""
    logger.info("Iniciando pipeline de dados...")
    
    # Processamento inicial
    if process_initial_data():
        logger.info("Processamento inicial concluído com sucesso")
    else:
        logger.error("Erro no processamento inicial")
        return
    
    # Processamento dos documentos pendentes
    if process_pending_documents(limit=5):
        logger.info("Processamento de documentos pendentes concluído")
    else:
        logger.error("Erro no processamento de documentos pendentes")
        return
    
    # Limpeza de arquivos temporários
    cleanup_temp_files()
    
    logger.info("Pipeline de dados finalizado")

if __name__ == "__main__":
    main()