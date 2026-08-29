# Registro de Contribuições

## Grupo: Ingestão Moderna de Dados

| Membro | Matrícula | Contribuições principais |
|--------|-----------|--------------------------|
| Francisco de Assis de Moura Correia | 2650102 | Arquitetura da solução, ajustes na pipeline de ingestão, implementação da lógica de execução por cenário (`full` e `incremental`), registros de log e evidências automáticas, documentação e organização do projeto |
| Sara Vieira da Silveira | 2651176 | Desenvolvimento do código inicial da pipeline de ingestão (extract/load), conexão com o MongoDB Atlas e escrita das coleções do `sample_mflix` na camada Bronze |
| Ana Raquel Soares Magalhaes | 2650612 | Implementação da idempotência do pipeline (estratégia de dedup por chave/hash, garantindo que reexecuções não duplicassem registros), alimentação da tabela `control_ingestion_log` a cada execução, tratamento de schema drift (preservação de registros divergentes via coluna de rescue/quarentena) e implementação das validações de reconciliação e qualidade (contagem origem × destino, checagem de nulos e duplicidade de `_source_id`) |