import xml.etree.ElementTree as ET
from typing import TextIO, Optional
import codecs

def analisar_estrutura_xml(arquivo_xml: str, arquivo_saida: str, encoding: str = 'cp1252') -> None:
    """
    Analisa a estrutura de um arquivo XML e salva em um arquivo TXT.
    
    Args:
        arquivo_xml: Caminho para o arquivo XML de entrada
        arquivo_saida: Caminho para o arquivo TXT de saída
        encoding: Codificação do arquivo XML (padrão: 'cp1252' para Windows-1252)
    """
    def escrever_estrutura(elemento: ET.Element, arquivo: TextIO, nivel: int = 0, 
                          caminho: Optional[str] = None) -> None:
        # Constrói o caminho atual do elemento
        nome_atual = elemento.tag
        caminho_atual = f"{caminho}/{nome_atual}" if caminho else nome_atual
        
        # Escreve o elemento atual com indentação
        arquivo.write("  " * nivel + f"Elemento: {nome_atual}\n")
        
        # Lista os atributos se existirem
        if elemento.attrib:
            arquivo.write("  " * (nivel + 1) + "Atributos:\n")
            for atrib in elemento.attrib:
                arquivo.write("  " * (nivel + 2) + f"- {atrib}\n")
        
        # Processa os elementos filhos
        for filho in elemento:
            escrever_estrutura(filho, arquivo, nivel + 1, caminho_atual)
    
    try:
        # Lê o arquivo XML com a codificação especificada
        with codecs.open(arquivo_xml, 'r', encoding=encoding) as xml_file:
            xml_content = xml_file.read()
        
        # Faz o parse do conteúdo XML
        root = ET.fromstring(xml_content)
        
        # Abre o arquivo de saída e escreve a estrutura
        with open(arquivo_saida, 'w', encoding='utf-8') as f:
            f.write(f"Estrutura do arquivo XML: {arquivo_xml}\n")
            f.write("=" * 50 + "\n\n")
            escrever_estrutura(root, f)
            
        print(f"Análise concluída! Estrutura salva em: {arquivo_saida}")
        
    except ET.ParseError as e:
        print(f"Erro ao fazer parse do XML: {e}")
        print("Linha do erro:", e.position)
    except UnicodeDecodeError as e:
        print(f"Erro de codificação: {e}")
        print("Tente especificar a codificação correta ao chamar a função")
    except IOError as e:
        print(f"Erro ao manipular arquivos: {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")

# Exemplo de uso
if __name__ == "__main__":
    analisar_estrutura_xml("data/temp/xml/001023FRE31-12-2024v8.xml", "estrutura_xml.txt", encoding='cp1252')