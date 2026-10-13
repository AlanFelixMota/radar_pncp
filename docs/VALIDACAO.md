# Validação da entrega — Radar PNCP 1.0.0

Base do trabalho: código vigente da versão 0.7.30 fornecida em 13/10/2026. O pacote de origem continha bancos auxiliares, versões antigas e runtime portátil; `compras.db` não estava presente.

Foram executados **11 testes automatizados**, todos aprovados, além das verificações de conteúdo e da migração das cópias dos bancos auxiliares.

## Verificações executadas

| Verificação | Resultado |
| --- | --- |
| Busca dos três nomes anteriores em arquivos e conteúdo, incluindo os XMLs dos modelos Office | Nenhuma menção no pacote final |
| Sintaxe Python e hashes do manifesto da aplicação | Verificados |
| Inicialização de base nova | Cria base vazia; nova tentativa não substitui banco existente |
| Disparadores de coleta | Coleta PNCP de um dia e atualização de janela móvel geram tipos de jobs e comandos distintos |
| Checkpoint de coleta | Grava e recupera o ponto de continuação |
| Busca e acompanhamento | Registra referência inicial, detecta novo registro e não duplica a novidade |
| Índice FTS5 | Reconstrói o índice e recupera contratação pelo texto |
| Exportação Excel e Word | Arquivos gerados com preço de quatro casas decimais preservado e identidade atualizada |
| Modelo Word | Renderizado e inspecionado visualmente após a alteração |
| Migração com prefixos de uma geração anterior | Tabelas, valores técnicos, relações e contadores preservados na cópia |
| Migração dos bancos auxiliares fornecidos | Conferência de contagens das 7 tabelas de jobs e 15 tabelas de pesquisas; sem violações de FK; arquivos fornecidos preservados byte a byte |
| Recusa de migração com jobs ativos ou destino ocupado | Verificada em bases isoladas |
| Pacote local de atualização | Manifesto, hashes e extração verificados |
| Interface Streamlit | As quatro páginas principais executadas sem exceções, em processo isolado e com consultas externas bloqueadas |

Os testes locais foram executados em Linux, Python 3.12.14, com as três dependências diretas nas versões declaradas em `requirements.txt`. O teste dos bancos auxiliares usou cópias de trabalho, com estados de execução encerrados apenas nessas cópias. Os arquivos fornecidos permaneceram preservados e não estão incluídos na entrega.

## Limites da validação

- A migração da base principal de produção não foi executada: `compras.db` não foi enviado.
- As APIs do PNCP e Compras.gov não foram consultadas nos testes. Disponibilidade, autenticação de rede e resposta das fontes devem ser conferidas na sua instalação.
- Os arquivos `.bat`, o Agendador de Tarefas do Windows, o reinício da interface pelo atualizador e a recuperação de atualização interrompida precisam de validação no seu Windows.
- O teste da interface verifica a execução do Streamlit e a navegação principal; não substitui a conferência visual dos componentes HTML no navegador.
- O script de migração depende de origem encerrada durante toda a cópia para uma fotografia coerente entre os diferentes bancos. Ele recusa estados ativos que consegue identificar no banco; não inspeciona todos os processos e tarefas da máquina.

O fluxo do GitHub Actions está preparado para repetir os testes em Linux e Windows. Esse workflow será executado somente depois que você publicar o repositório; nenhum resultado remoto é presumido nesta entrega.
