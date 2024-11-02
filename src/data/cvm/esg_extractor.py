# src/data/cvm/esg_extractor.py

from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import hashlib
from lxml import etree
from src.utils.logging import setup_logging
from src.config import settings

class ESGDataExtractor:
    """Extrai dados ESG de XMLs do FRE."""
    
    def __init__(self, document_id: str, xml_path: Path, cod_cvm: int):
        """
        Inicializa o extrator.
        
        Args:
            document_id: ID do documento no MongoDB
            xml_path: Caminho para o arquivo XML do FRE
        """
        self.logger = setup_logging(__name__)
        self.document_id = document_id
        self.xml_path = xml_path
        self.cod_cvm = cod_cvm
        self.temp_dir = settings.get_company_temp_dir(cod_cvm) / 'pdfs'
        self.temp_dir.mkdir(exist_ok=True)

        # Parse do XML com encoding específico para documentos da CVM
        try:
            parser = etree.XMLParser(encoding='cp1252')
            self.tree = etree.parse(str(xml_path), parser)
            self.root = self.tree.getroot()
        except Exception as e:
            self.logger.error(f"Erro ao carregar XML {xml_path}: {str(e)}")
            raise

    def extract_data(self) -> Dict[str, Any]:
        """
        Extrai todos os dados ESG do XML.
        
        Returns:
            Dicionário com dados estruturados e referências a PDFs
        """
        self.logger.info(f"Iniciando extração de dados ESG do documento {self.document_id}")
        try:
            dados_esg = {
                "data_extracao": datetime.utcnow(),
                "documentos": self._extract_documents(),
                "dados_quantitativos": {
                    "orgaos_admin": self._extract_admin_diversity(),
                    "recursos_humanos": self._extract_hr_data(),
                }
            }
            self.logger.info(f"Extração de dados ESG concluída para documento {self.document_id}")
            return dados_esg
            
        except Exception as e:
            self.logger.error(f"Erro durante extração de dados ESG: {str(e)}")
            raise
        
    def _save_pdf(self, pdf_content: bytes, section_name: str) -> Dict[str, str]:
        """
        Salva um PDF e retorna seus metadados.
        
        Args:
            pdf_content: Conteúdo do PDF em bytes
            section_name: Nome da seção do documento
            
        Returns:
            Dicionário com metadados do PDF
        """
        if not pdf_content:
            return None
            
        # Gera nome único baseado no hash do conteúdo
        pdf_hash = hashlib.md5(pdf_content).hexdigest()
        filename = f"{section_name}_{pdf_hash}.pdf"
        file_path = self.temp_dir / filename
        
        # Salva o arquivo
        with open(file_path, 'wb') as f:
            f.write(pdf_content)
            
        return {
            "nome_arquivo": filename,
            "hash": pdf_hash,
            "path": str(file_path)
        }

    def _extract_documents(self) -> Dict[str, Dict[str, str]]:
        """
        Extrai e salva todos os PDFs relevantes.
        
        Returns:
            Dicionário com metadados dos PDFs por seção
        """
        documentos = {}
        
        # Mapeamento das seções e seus XPaths
        sections = {
            # Seções existentes
            "info_asg": "//InfoASG/ImagemObjetoArquivoPdf",
            "programa_integridade": "//ProgramaIntegridade/ImagemObjetoArquivoPdf",
            "gestao_riscos": "//DescricaoGerenciamentoRiscos/ImagemObjetoArquivoPdf",
            "controles_internos": "//DescricaoControlesInternos/ImagemObjetoArquivoPdf",
            "recursos_humanos": "//DescricaoRH/ImagemObjetoArquivoPdf",
            "fatores_risco": "//DescricaoFatoresRisco/ImagemObjetoArquivoPdf",
            "fatores_risco_principais": "//Descricao5PrincipaisFatoresRisco/ImagemObjetoArquivoPdf",
            
            # Novas seções
            "historico": "//HistoricoEmissor/ImagemObjetoArquivoPdf",
            "atividades_controladas": "//AtividadesEmissorControladas/ImagemObjetoArquivoPdf",
            "segmentos_operacionais": "//InfoSegmentosOperacionais/ImagemObjetoArquivoPdf",
            "producao_mercados": "//ProducaoComercializacaoMercados/ImagemObjetoArquivoPdf",
            "regulacao_estatal": "//EfeitosRegulacaoEstatal/ImagemObjetoArquivoPdf",
            "economia_mista": "//InfoSociedadeEconomiaMista/ImagemObjetoArquivoPdf",
            "alteracoes_negocios": "//AlteracoesNegocios/ImagemObjetoArquivoPdf",
            "plano_negocios": "//PlanoNegocios/ImagemObjetoArquivoPdf",
            "caracteristicas_orgaos": "//CaracteristicasOrgaosAdmECF/ImagemObjetoArquivoPdf",
            "conselho_adm": "//InformacoesConselhoAdm/ImagemObjetoArquivoPdf",
            "politica_remuneracao": "//PoliticaPraticaRemuneracao/ImagemObjetoArquivoPdf",
            "remuneracao_empregados": "//RemuneracaoEmpregados/ImagemObjetoArquivoPdf"
        }
        
        for section, xpath in sections.items():
            elem = self.root.xpath(xpath)
            if elem and elem[0].text:
                try:
                    import base64
                    pdf_content = base64.b64decode(elem[0].text)
                    doc_info = self._save_pdf(pdf_content, section)
                    if doc_info:
                        documentos[section] = doc_info
                except Exception as e:
                    self.logger.error(f"Erro ao processar PDF da seção {section}: {str(e)}")
                    
        return documentos

    def _extract_admin_diversity(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extrai dados de diversidade dos órgãos administrativos.
        
        Returns:
            Dicionário com dados de diversidade por órgão
        """
        diversidade = []
        
        # Extrai dados de cor/raça
        raca_xpath = "//XmlFormularioReferenciaDadosFREFormularioAssembleiaGeralEAdmDescricaoCaracteristicasOrgaosAdmECFCorRaca"
        for elem in self.root.xpath(raca_xpath):
            orgao = elem.find("OrgaoAdministracao")
            if orgao is not None:
                dados = {
                    "orgao": orgao.text,
                    "cor_raca": {
                        "amarelo": self._get_int_value(elem, "Amarelo"),
                        "branco": self._get_int_value(elem, "Branco"),
                        "preto": self._get_int_value(elem, "Preto"),
                        "pardo": self._get_int_value(elem, "Pardo"),
                        "indigena": self._get_int_value(elem, "Indigena"),
                        "outros": self._get_int_value(elem, "Outros"),
                        "nao_informado": self._get_int_value(elem, "PrefereNaoResponder")
                    }
                }
                diversidade.append(dados)
        
        # Extrai dados de gênero
        genero_xpath = "//XmlFormularioReferenciaDadosFREFormularioAssembleiaGeralEAdmDescricaoCaracteristicasOrgaosAdmECFGenero"
        for elem in self.root.xpath(genero_xpath):
            orgao = elem.find("OrgaoAdministracao")
            if orgao is not None:
                # Procura se já existe entrada para este órgão
                org_data = next((d for d in diversidade if d["orgao"] == orgao.text), None)
                if org_data is None:
                    org_data = {"orgao": orgao.text}
                    diversidade.append(org_data)
                
                org_data["genero"] = {
                    "masculino": self._get_int_value(elem, "Masculino"),
                    "feminino": self._get_int_value(elem, "Feminino"),
                    "nao_binario": self._get_int_value(elem, "NaoBinario"),
                    "outros": self._get_int_value(elem, "Outros"),
                    "nao_informado": self._get_int_value(elem, "PrefereNaoResponder")
                }
        
        return {"diversidade": diversidade}

    def _extract_hr_data(self) -> Dict[str, Any]:
        """
        Extrai dados de recursos humanos.
        
        Returns:
            Dicionário com dados de RH
        """
        hr_data = {"diversidade": {}}
        
        # Extrai dados de cor/raça
        raca_xpath = "//XmlFormularioReferenciaDadosFREFormularioRecursosHumanosDescricaoRHEmissorCorRaca"
        elem = self.root.xpath(raca_xpath)
        if elem:
            hr_data["diversidade"]["cor_raca"] = {
                "amarelo": self._get_int_value(elem[0], "Amarelo"),
                "branco": self._get_int_value(elem[0], "Branco"),
                "preto": self._get_int_value(elem[0], "Preto"),
                "pardo": self._get_int_value(elem[0], "Parda"),
                "indigena": self._get_int_value(elem[0], "Indigena"),
                "outros": self._get_int_value(elem[0], "Outros"),
                "nao_informado": self._get_int_value(elem[0], "PrefiroNaoResponder")
            }
        
        # Extrai dados de gênero
        genero_xpath = "//XmlFormularioReferenciaDadosFREFormularioRecursosHumanosDescricaoRHEmissorGenero"
        elem = self.root.xpath(genero_xpath)
        if elem:
            hr_data["diversidade"]["genero"] = {
                "masculino": self._get_int_value(elem[0], "Masculino"),
                "feminino": self._get_int_value(elem[0], "Feminino"),
                "nao_binario": self._get_int_value(elem[0], "NaoBinario"),
                "outros": self._get_int_value(elem[0], "Outros"),
                "nao_informado": self._get_int_value(elem[0], "PrefiroNaoResponder")
            }
        
        # Extrai dados de faixa etária
        etaria_xpath = "//XmlFormularioReferenciaDadosFREFormularioRecursosHumanosDescricaoRHEmissorFaixaEtaria"
        elem = self.root.xpath(etaria_xpath)
        if elem:
            hr_data["diversidade"]["faixa_etaria"] = {
                "abaixo_30": self._get_int_value(elem[0], "FaixaAbaixo30"),
                "entre_30_50": self._get_int_value(elem[0], "FaixaDe30a50"),
                "acima_50": self._get_int_value(elem[0], "FaixaAcima50")
            }
        
        # Extrai dados de localização
        loc_xpath = "//XmlFormularioReferenciaDadosFREFormularioRecursosHumanosDescricaoRHEmissorLocalizacaoGeografica"
        elem = self.root.xpath(loc_xpath)
        if elem:
            hr_data["diversidade"]["localizacao"] = {
                "norte": self._get_int_value(elem[0], "Norte"),
                "nordeste": self._get_int_value(elem[0], "Nordeste"),
                "centro_oeste": self._get_int_value(elem[0], "CentroOeste"),
                "sudeste": self._get_int_value(elem[0], "Sudeste"),
                "sul": self._get_int_value(elem[0], "Sul"),
                "exterior": self._get_int_value(elem[0], "Exterior")
            }
            
        # Extrai dados de remuneração
        rem_xpath = "//RemuneracaoEmpregadosEst"
        elem = self.root.xpath(rem_xpath)
        if elem:
            hr_data["remuneracao"] = {
                "maior": self._get_float_value(elem[0], "RemuneracaoMaior"),
                "mediana": self._get_float_value(elem[0], "RemuneracaoMediana"),
                "razao": self._get_float_value(elem[0], "RazaoRemuneracoes")
            }
            
        return hr_data

    def _get_int_value(self, elem: etree._Element, tag: str) -> int:
        """Extrai valor inteiro de um elemento XML."""
        try:
            value = elem.find(tag)
            return int(value.text) if value is not None and value.text else 0
        except (ValueError, AttributeError):
            return 0

    def _get_float_value(self, elem: etree._Element, tag: str) -> float:
        """Extrai valor float de um elemento XML."""
        try:
            value = elem.find(tag)
            return float(value.text) if value is not None and value.text else 0.0
        except (ValueError, AttributeError):
            return 0.0        