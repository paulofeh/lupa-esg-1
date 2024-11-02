from typing import Dict, Any, List
from pymongo import MongoClient
from datetime import datetime
import os
from enum import Enum
from bson.objectid import ObjectId
from src.utils.logging import setup_logging

class ProcessingStatus(Enum):
    """Status de processamento do documento."""
    PENDING = "pending"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    PROCESSING = "processing"
    XML_EXTRACTED = "xml_extracted"  # Novo status intermediário
    PROCESSED = "processed"
    ERROR = "error"

class MongoDB:
    """Gerenciador de conexões e operações com MongoDB."""
    
    def __init__(self):
        self.logger = setup_logging(__name__)
        
        # Inicializa conexão
        self.client = MongoClient(os.getenv('MONGODB_URI'))
        self.db = self.client[os.getenv('MONGODB_DB', 'lupa_esg')]
        
        # Inicializa as collections
        self.companies = self.db.companies
        self.documents = self.db.documents
        
        # Cria índices necessários
        self._setup_indexes()
        
    def _setup_indexes(self):
        """Configura os índices necessários nas collections."""
        # Índices para companies
        self.companies.create_index([
            ('cod_cvm', 1),
            ('cnpj', 1)
        ], unique=True)
        
        # Índices para documents
        self.documents.create_index([
            ('cod_cvm', 1),
            ('ano_referencia', 1)
        ], unique=True)
        
        self.documents.create_index('status')
        self.documents.create_index('data_inclusao')
        self.documents.create_index([('status', 1), ('data_inclusao', 1)])
    
    def upsert_company(self, company_data: Dict[str, Any]) -> str:
        """
        Insere ou atualiza dados de uma empresa.
        
        Args:
            company_data: Dicionário com dados da empresa
            
        Returns:
            ID do documento inserido/atualizado ou None
        """
        try:
            company = {
                'cod_cvm': company_data['CD_CVM'],
                'cnpj': company_data['CNPJ_CIA'],
                'razao_social': company_data['DENOM_CIA'],
                'setor': company_data.get('setor', ''),
                'subsetor': company_data.get('subsetor', ''),
                'segmento': company_data.get('segmento', ''),
                'situacao': company_data.get('SITUACAO', ''),
                'ultima_atualizacao': datetime.utcnow(),
                'metadata': {
                    'primeira_inclusao': datetime.utcnow(),
                    'fonte': 'CVM',
                    'ativo': True
                }
            }
            
            result = self.companies.update_one(
                {'cod_cvm': company['cod_cvm']},
                {
                    '$set': {k: v for k, v in company.items() if k != 'metadata'},
                    '$setOnInsert': {'metadata': company['metadata']}
                },
                upsert=True
            )
            
            self.logger.info(
                f"{'Empresa inserida' if result.upserted_id else 'Empresa atualizada'}: {company['razao_social']}"
            )
            return str(result.upserted_id) if result.upserted_id else None
            
        except Exception as e:
            self.logger.error(f"Erro ao inserir/atualizar empresa: {str(e)}")
            raise

    def insert_document(self, doc_data: Dict[str, Any]) -> str:
        """
        Insere ou atualiza um documento FRE, substituindo versões anteriores do mesmo ano.
        
        Args:
            doc_data: Dicionário com dados do documento
            
        Returns:
            ID do documento inserido/atualizado ou None
        """
        try:
            # Extrai o ano de referência da data
            ano_referencia = doc_data['DT_REFER'].year
            
            document = {
                'cod_cvm': doc_data['CD_CVM'],
                'ano_referencia': ano_referencia,
                'dt_referencia': doc_data['DT_REFER'],
                'dt_recebimento': doc_data['DT_RECEB'],
                'versao': doc_data['VERSAO'],
                'id_documento': doc_data['ID_DOC'],
                'tipo': doc_data.get('CATEG_DOC', 'FRE'),
                'url': doc_data['LINK_DOC'],
                'status': ProcessingStatus.PENDING.value,
                'data_inclusao': datetime.utcnow(),
                'data_modificacao': datetime.utcnow(),
                'metadata': {
                    'tentativas_processamento': 0,
                    'ultimo_erro': None,
                    'arquivos': {},
                    'stats': {}
                }
            }
            
            # Usa update_one com upsert=True para substituir ou criar novo documento
            result = self.documents.update_one(
                {
                    'cod_cvm': document['cod_cvm'],
                    'ano_referencia': ano_referencia
                },
                {'$set': document},
                upsert=True
            )
            
            action = "atualizado" if result.matched_count else "inserido"
            self.logger.info(f"Documento {action}: {document['id_documento']}")
            
            return str(result.upserted_id) if result.upserted_id else None
                
        except Exception as e:
            self.logger.error(f"Erro ao inserir/atualizar documento: {str(e)}")
            raise

    def get_pending_documents(self, 
                            limit: int = 10, 
                            status: List[str] = None,
                            max_retries: int = 3) -> list:
        """
        Recupera documentos pendentes de processamento.
        
        Args:
            limit: Número máximo de documentos
            status: Lista de status a filtrar (default: apenas PENDING)
            max_retries: Número máximo de tentativas de processamento
            
        Returns:
            Lista de documentos pendentes
        """
        if status is None:
            status = [ProcessingStatus.PENDING.value]
            
        query = {
            'status': {'$in': status},
            'metadata.tentativas_processamento': {'$lt': max_retries}
        }
        
        return list(self.documents.find(query)
                   .sort('data_inclusao', 1)
                   .limit(limit))

    def update_document_status(
        self,
        document_id: str,
        status: ProcessingStatus,
        error: str = None,
        metadata: Dict = None
    ):
        """
        Atualiza o status de processamento de um documento.
        
        Args:
            document_id: ID do documento
            status: Novo status
            error: Mensagem de erro (opcional)
            metadata: Dados adicionais de processamento (opcional)
        """
        # Preparamos os dados para atualização
        update_dict = {
            '$set': {
                'status': status.value,
                'data_modificacao': datetime.utcnow()
            },
            '$inc': {
                'metadata.tentativas_processamento': 1
            }
        }
        
        if error:
            update_dict['$set']['metadata.ultimo_erro'] = error
            
        if metadata:
            for key, value in metadata.items():
                update_dict['$set'][f'metadata.{key}'] = value
        
        # Executa a atualização
        self.documents.update_one(
            {'_id': ObjectId(document_id)},  # Convertemos o ID para ObjectId
            update_dict
        )
        
        self.logger.info(
            f"Status do documento {document_id} atualizado para {status.value}"
            + (f" (erro: {error})" if error else "")
        )

    def __del__(self):
        """Fecha a conexão com o MongoDB quando o objeto é destruído."""
        if hasattr(self, 'client'):
            self.client.close()