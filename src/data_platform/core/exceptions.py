"""
Exceções personalizadas para o projeto
"""

class PlatformError(Exception):
    """Exception base para as outras exceções do projeto"""
    pass

class ValidationError(PlatformError):
    """Exceção para erros na validação dos dados"""
    pass

class IngestionError(PlatformError):
    """Exceção para erros na ingestão de dados"""
    pass

class VersioningError(PlatformError):
    """Exceção para erros relacionados ao versionamento de dados"""
    pass

class BlockchainError(PlatformError):
    """Exceção para erros relacionados à blockchain"""	
    pass