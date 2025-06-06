# VeriTrace
Sistema de Confiabilidade - VeriTrace

**Autor:** Leonardo Ferrari Soares 
**Instituição:** XP Educação, Pós-Graduação 
**Data:** 28/05/2025

## Visão Geral do Projeto

O projeto "Sistema de Confiabilidade (VeriTrace)" é um Projeto Aplicado desenvolvido para o curso de Pós-Graduação da XP Educação. O objetivo principal é desenvolver uma plataforma inteligente e descentralizada para garantir a confiabilidade de dados corporativos.

Este projeto aborda o desafio crítico da crescente geração e disseminação de dados e o uso de múltiplos sistemas nas organizações, o que torna essencial assegurar a confiabilidade, integridade e consistência das informações transmitidas. A falta de confiança nos dados, devido à ausência de padronização e rastreabilidade, é um problema central que este projeto visa solucionar. 

## Problema Identificado

* Bases de dados frequentemente descentralizadas e sem comunicação entre si, levando a erros de batimento e inventário.
* Integridade e origem dos dados frequentemente contestadas.
* Ausência de controle de versão confiável ou registro de alterações em dados históricos, ou, quando existente, é desorganizado.
* Governança de dados centralizada, gerando gargalos.
* Falta de confiança nos dados por parte dos tomadores de decisão.
* Decisões erradas e construção de soluções sobre informações incompletas (GIGO - Garbage In, Garbage Out).

## Persona Alvo

* **Nome:** Afonso Luis da Silva
* **Idade:** 45 anos 
* **Cargo:** Gerente de TI na área de Riscos e Tesouraria 
* **Setor:** Mercado financeiro 
* **Comportamento:** Proativo, orientado a processos, preocupado com compliance e SLAs. 
* **Necessidades:** Uma plataforma ou sistema automatizado e confiável para governança de dados.
* **Dores:** Baixa rastreabilidade, erros constantes, retrabalho.

## Solução proposta: VeriTrace

Desenvolver uma plataforma inteligente e descentralizada de confiabilidade de dados até o final do curso. A plataforma visa:
* Integrar dados de múltiplos sistemas utilizando arquitetura Datamesh.
* Registrar alterações via blockchain para garantir imutabilidade e rastreabilidade.
* Aplicar Inteligência Artificial (IA) para validar e corrigir dados automaticamente.
* Promover integridade, rastreabilidade e eficiência na governança de dados corporativos.

### Proposta de Valor

* **Produto/Serviço:** Plataforma inteligente de confiabilidade de dados com IA, blockchain, ontologias, controle de versão e modelagem dos dados.
* **Principais Funcionalidades (Remédios):** 
    * Blockchain registra todas as alterações de forma imutável. 
    * IA detecta e corrige anomalias automaticamente. 
    * Ontologias padronizam e alinham semanticamente os dados. 
    * Versionamento permite reversão e comparação de dados históricos.
    * Modelagem dos dados garante a visualização e manipulação dos metadados.
* **Benefícios (Criador de Ganho):** 
    * Facilita auditorias e compliance com total transparência. 
    * Permite maior autonomia das equipes com Datamesh.
    * Melhora a tomada de decisão com dados íntegros. 
    * Reduz o tempo gasto com retrabalho de dados.

## Objetivo SMART do Projeto

Desenvolver uma plataforma inteligente e descentralizada de confiabilidade de dados até o final do curso, que integre dados de múltiplos sistemas usando arquitetura Datamesh, registrando alterações via blockchain e aplicando inteligência artificial para validar e corrigir dados automaticamente, promovendo integridade, rastreabilidade e eficiência na governança de dados corporativos. 

* **S (Específico):** Plataforma de confiabilidade de dados com IA, blockchain, controle de versão e integração descentralizada. 
* **M (Mensurável):** A plataforma deverá integrar pelo menos 3 domínios de dados (ex: boletador, finanças, pagamentos), gerar logs na blockchain para cada alteração e realizar validações automáticas com IA, reduzindo erros em pelo menos metade. 
* **A (Alcançável):** Serão utilizados frameworks e ferramentas open-source (LakeFS, Airflow, Hyperledger, Python), gerando código reproduzível nos respectivos repositórios. 
* **R (Relevante):** Endereçar problemas críticos de confiabilidade e rastreabilidade de dados em ambientes corporativos, melhorando compliance, observabilidade e eficiência operacional. 
* **T (Temporal):** O protótipo funcional será entregue em até 30 dias, dividido em 3 sprints, finalizando na última sprint/pré apresentação. 

## Tecnologias visadas para utilização

* **Ingestão de Dados:** Airflow, scripts Python, Kafka (possíveis ferramentas).
* **Validação com IA:** Scikit-learn, Pandas, API de modelos de IA.
* **Controle de Versão:** LakeFS, GitHub.
* **Auditoria e Rastreabilidade (Blockchain):** Hyperledger Fabric / BigchainDB.
* **Governança Descentralizada (Datamesh):** Datamesh, Apache Atlas (possível ferramenta).
* **Catalogação e Visualização:** Metabase, Superset.
* **Infraestrutura de Testes:** AWS Free Tier. 
* **Linguagem de Programação:** Python.

