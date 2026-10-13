# Radar PNCP

Aplicação local para pesquisar contratações públicas, acompanhar buscas e organizar referências de preços. Usa Python, Streamlit e SQLite; consulta o PNCP e fontes complementares do Compras.gov.

## Recursos

- Pesquisa de contratações e itens na base local, com índice FTS5.
- Consulta de fornecedores e resultados por item.
- Coleta diária, revisão semanal e recuperação de períodos.
- Coleta de itens em segundo plano, com checkpoints e controle de ritmo.
- Buscas salvas e acompanhamento de novas referências.
- Cestas de pesquisa, conferência das referências e exportação para Excel e Word.
- Classificação heurística de TIC e painéis de cobertura, operações e logs.

## Instalação no Windows

Requer Python 3.12, internet para instalar as dependências e espaço em disco para a base de dados. O código foi preparado com as dependências da versão de origem: Requests 2.32.5, Streamlit 1.49.1 e Pandas 2.3.3.

1. Extraia este projeto em uma pasta nova, por exemplo `C:\Projetos\radar_pncp`.
2. Execute `instalar.bat`.
3. Escolha um dos caminhos:
   - **Base vazia:** execute `CRIAR_BASE_INICIAL.bat`.
   - **Dados de uma instalação existente:** siga [Migração por cópia](docs/MIGRACAO.md). Não crie a base vazia antes da migração.
4. Execute `iniciar.bat`.

A interface abre em `http://localhost:8501`. O servidor escuta somente no computador local. Coletas e tarefas do Windows não são instaladas automaticamente. Configure-as em **Sistema → Configurações** quando desejar.

## Instalação pelo terminal

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe criar_base_inicial.py
.venv\Scripts\python.exe radar_pncp_iniciar.py
```

Em Linux/macOS, os comandos equivalentes usam `.venv/bin/python`. Os recursos de agendamento e troca automática de versões dependem do Windows; o teste dessa plataforma continua necessário antes de uso em produção.

## Estrutura

| Caminho | Finalidade |
| --- | --- |
| `radar_pncp_iniciar.py` | Inicializador estável da instalação |
| `radar_versoes/atual/` | Código da aplicação vigente |
| `radar_versoes/atual/coleta_*.py` | Coleta diária, complementação de fontes e agendas |
| `radar_versoes/atual/monitoramento*.py` | Acompanhamento de buscas e novidades |
| `radar_versoes/atual/modelos/` | Modelos de exportação |
| `radar_versoes/pacotes/` | Pacotes locais de atualização |
| `radar_dados/` | Bancos, caches, logs e dados do usuário |
| `tests/` | Testes offline dos fluxos de regressão |
| `docs/` | Instruções de migração e validação |

## Dados e configuração

Os bancos são criados ou copiados no computador de quem instala o sistema. Eles não fazem parte deste repositório. A pasta de dados fica separada do código e não é substituída pelas atualizações locais.

As variáveis opcionais existentes foram mantidas: `RADAR_DATA_DIR`, `RADAR_DB`, `RADAR_JOBS_DB`, `RADAR_FTS_DB`, `RADAR_TIC_DB`, `RADAR_STATS_DB` e `RADAR_LOG_DIR`. Use caminhos absolutos. A migração inicial e o atualizador de versões usam o layout padrão em `radar_dados`.

Para alterar a porta, crie `radar_versoes/atual/instalacao.json` com `{"version":"1.0.0","port":8502}`. Esse arquivo é configuração local e está no `.gitignore`.

## Validação

```powershell
.venv\Scripts\python.exe -m unittest discover -s tests -v
.venv\Scripts\python.exe verificar_instalacao.py --somente-codigo
```

Depois de criar ou migrar a base, execute `verificar_instalacao.py` sem a opção `--somente-codigo` para conferir também as tabelas básicas. O comando não varre todos os registros nem consulta APIs externas.

Veja [Validação da entrega](docs/VALIDACAO.md) para conhecer os testes executados e os pontos que precisam ser conferidos no Windows.

## Atualizações locais

`python scripts/gerar_pacote.py` recompõe o manifesto SHA-256 da versão atual e gera o ZIP de atualização em `dist/`. Para uma próxima versão, atualize o número em `release.json`, `interface_radar_pncp.py` e nos documentos pertinentes antes de gerar o pacote. Coloque o ZIP em `radar_versoes/pacotes` para usá-lo pela interface.

Esta primeira versão com a nova identidade deve ser instalada em pasta nova. O procedimento de [migração](docs/MIGRACAO.md) faz a transição dos bancos; não use o atualizador da instalação anterior para essa mudança de identidade.

## Subir no GitHub

O pacote contém apenas o código vigente, os modelos e a documentação. O `.gitignore` exclui bancos, logs, configurações privadas, caches, ambientes virtuais, o runtime portátil e versões antigas.

Crie um repositório vazio e envie os arquivos desta pasta. Pelo terminal:

```powershell
git init
git add .
git commit -m "Publica Radar PNCP 1.0.0"
git branch -M main
git remote add origin URL_DO_SEU_REPOSITORIO
git push -u origin main
```

A identidade e a configuração de acesso ao GitHub são feitas no seu computador. Esta entrega não publica o projeto automaticamente nem atribui uma licença de distribuição.

## Interpretação dos resultados

O sistema organiza uma base local que pode conter lacunas ou registros ainda em atualização. Ausência de resultados não comprova ausência de contratação na fonte. A classificação TIC auxilia a triagem; a comparabilidade dos preços e o conteúdo final dos relatórios devem ser conferidos por quem realiza a pesquisa.
