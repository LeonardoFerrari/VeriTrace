# scripts/run_pipeline_simulation.py
"""
Script pra rodar uma simulação

inicializa a plataforma com 'dev.yaml'
roda o pipeline completo (Ingestao, Validacao, Versao, Auditoria) num CSV de exemplo
nostra onde ficam os arquivos gerados (catalogo de metadados e log de auditoria)
"""
import os
import sys
import json
from pathlib import Path

# Adiciona o root do projeto no path pra importar modulos de 'src'
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from src.data_platform.core.platform import DataReliabilityPlatform
from src.data_platform.core.exceptions import PlatformError

def main():
    print("==============================================")
    print(" Iniciando Simulação do Pipeline ")
    print("==============================================")

    try:
        # Carrega 'config/dev.yaml' por padrão
        platform = DataReliabilityPlatform()
        print("\nPlataforma inicializada!")

        source_file = PROJECT_ROOT / "data_sources" / "samples" / "vendas_diarias.csv"
        output_file = PROJECT_ROOT / "staging" / "processed_vendas_diarias.parquet"
        
        if not source_file.exists():
            print(f"\n ❌ ERRO: Arquivo de origem não encontrado em '{source_file}'")
            print("Crie os arquivos de exemplo conforme instruções")
            return

        print(f"\nProcessando arquivo: '{source_file.name}'")

        # Pipeline Completo
        pipeline_results = platform.full_pipeline(
            source_path=str(source_file),
            output_path=str(output_file)
        )
        print("\nPipeline rodou com sucesso")
        print("\n==============================================")
        print("📄 Resumo dos Arquivos Gerados 📄")
        print("----------------------------------------------")
        
        # catalogo de metadados
        metadata_path = Path(platform.config.get('paths.metadata_catalog'))
        if metadata_path.exists():
            print(f"Catalogo de Metadados..: '{metadata_path}'")
            with open(metadata_path, 'r') as f:
                print(json.dumps(json.load(f), indent=2))
        else:
            print(f"❌ Catalogo de metadados não encontrado em '{metadata_path}'")
            
        print("----------------------------------------------")
        
        # Mostra o log de auditoria blockchain
        audit_log_path = Path(platform.config.get('paths.blockchain_log'))
        if audit_log_path.exists():
            print(f"🔗 Log de Auditoria.......: '{audit_log_path}'")
            with open(audit_log_path, 'r') as f:
                print(json.dumps(json.load(f), indent=2))
        else:
            print(f"Log de auditoria não encontrado em '{audit_log_path}'")
        
        print("----------------------------------------------")
        print(f"Arquivo processado salvo em: '{output_file}'")
        print("==============================================")

    except PlatformError as e:
        print(f"\n❌ Erro da plataforma: {e}")
    except Exception as e:
        print(f"\n❌ Erro inesperado: {e}")

if __name__ == "__main__":
    main()
