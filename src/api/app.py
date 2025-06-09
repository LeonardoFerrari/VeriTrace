
"""
FastAPI para expor a plataforma via REST
"""
import os
import sys
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# --- Ajuste de Path para MVP ---
# permiti importação dos módulos 'src'.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, PROJECT_ROOT)

from src.data_platform.core.platform import DataReliabilityPlatform
from src.data_platform.core.exceptions import PlatformError, IngestionError, ValidationError

app = FastAPI(
    title="VeriTrace - API da Plataforma de Confiabilidade de Dados",
    description="API para executar e gerenciar o MVP da Plataforma Inteligente de Confiabilidade de Dados.",
    version="1.0.0"
)

# iniciar a aplicação
try:
    CONFIG_FILE = os.path.join(PROJECT_ROOT, "config", "dev.yaml")
    platform = DataReliabilityPlatform(config_file=CONFIG_FILE)
except Exception as e:
    # Se a plataforma falhar ao iniciar, a API não sobe.
    # objeto dummy para permitir que a API inicie e reporte o erro
    platform = None
    initialization_error = e


class PipelineRequest(BaseModel):
    """Define o body da requisição para acionar o pipeline."""
    source_path: str = Field(..., example="data_sources/samples/vendas_diarias.csv", description="Caminho para o arquivo de dados de origem, relativo à raiz")
    output_path: str = Field(..., example="staging/processed_sales.parquet", description="Caminho para salvar o arquivo Parquet de saída processado")
    branch: str = Field("main", example="main", description="Branch de versionamento de dados para o commit")

@app.on_event("startup")
async def startup_event():
    if platform is None:
        print(f"FATAL: Falha na inicialização da plataforma: {initialization_error}")
    else:
        print("API iniciada com sucesso.")


@app.get("/status", tags=["Saúde"], summary="Verificar Status da Plataforma")
def get_status():
    """Retorna o status operacional da plataforma e seus componentes."""
    if platform is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Plataforma indisponível devido a erro de inicialização: {initialization_error}"
        )
    return platform.get_status()

@app.post("/pipeline/run", tags=["Pipeline"], summary="Executar Pipeline Completo")
async def run_pipeline(request: PipelineRequest):
    """
    Aciona o pipeline completo de confiabilidade de dados: Ingestão -> Validação -> Versionamento -> Auditoria
    """
    if platform is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Plataforma indisponível devido a erro de inicialização."
        )
        
    try:
        # caminhos absolutos para robustez 
        source_abs_path = os.path.join(PROJECT_ROOT, request.source_path)
        output_abs_path = os.path.join(PROJECT_ROOT, request.output_path)

        result = platform.full_pipeline(
            source_path=source_abs_path,
            output_path=output_abs_path,
            branch=request.branch
        )
        return {"message": "Pipeline executado com sucesso", "results": result}
    except FileNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Arquivo não encontrado: {e}")
    except (IngestionError, ValidationError, PlatformError) as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Ocorreu um erro inesperado: {str(e)}")
