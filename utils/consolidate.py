import os
from pathlib import Path
from datetime import datetime

def consolidate_code(project_root: str, output_file: str, ignore_paths: list = None):
    """
    Consolida todo o código do projeto em um único arquivo texto.
    
    Args:
        project_root: Caminho raiz do projeto
        output_file: Nome do arquivo de saída
        ignore_paths: Lista de caminhos a ignorar
    """
    if ignore_paths is None:
        ignore_paths = ['venv', '__pycache__', '.git', '.pytest_cache']
    
    # Extensões de arquivo a incluir
    extensions = ['.py', '.md', '.txt', '.env.example', '.gitignore']
    
    with open(output_file, 'w', encoding='utf-8') as f:
        # Escreve cabeçalho
        f.write(f"# Consolidação do Código do Projeto\n")
        f.write(f"# Gerado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Percorre todos os arquivos do projeto
        for root, dirs, files in os.walk(project_root):
            # Remove diretórios a serem ignorados
            dirs[:] = [d for d in dirs if d not in ignore_paths]
            
            path = Path(root)
            
            # Processa cada arquivo
            for file in files:
                file_path = path / file
                
                # Verifica se é um arquivo que devemos processar
                if any(file.endswith(ext) for ext in extensions):
                    # Calcula o caminho relativo
                    relative_path = file_path.relative_to(project_root)
                    
                    try:
                        # Lê o conteúdo do arquivo
                        with open(file_path, 'r', encoding='utf-8') as source:
                            content = source.read()
                            
                        # Escreve separador e caminho do arquivo
                        f.write(f"\n{'='*80}\n")
                        f.write(f"# Arquivo: {relative_path}\n")
                        f.write(f"{'='*80}\n\n")
                        
                        # Escreve conteúdo
                        f.write(content)
                        f.write("\n")
                        
                    except Exception as e:
                        f.write(f"# Erro ao ler arquivo {relative_path}: {str(e)}\n")

if __name__ == "__main__":
    # Define o diretório raiz do projeto (um nível acima do script)
    project_root = Path(__file__).parent
    
    # Nome do arquivo de saída
    output_file = project_root / "consolidated_code.txt"
    
    # Executa a consolidação
    consolidate_code(project_root, output_file)
    print(f"Código consolidado em: {output_file}")