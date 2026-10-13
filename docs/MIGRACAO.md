# Migração por cópia para Radar PNCP 1.0.0

Esta transição altera nomes internos, incluindo tabelas de controle, tipos de execução e tarefas agendadas. Copiar os bancos diretamente sem adaptar a estrutura pode esconder acompanhamentos ou históricos de coleta. Use o procedimento abaixo em uma pasta nova.

## Preparar a origem

1. Na instalação existente, desative a coleta automática e as agendas diária, semanal e do mutirão.
2. Pare as coletas ativas pela interface e aguarde a confirmação de encerramento. Se houver atualização do código em andamento, conclua-a ou recupere-a antes da migração.
3. Feche a interface e mantenha a instalação anterior encerrada durante toda a cópia. As tarefas de coleta podem executar mesmo sem o navegador aberto; confirme a desativação no Agendador de Tarefas do Windows.
4. Mantenha a pasta de dados original como backup. Reserve espaço livre para uma segunda cópia completa dos bancos, mais uma margem de trabalho.

## Criar a instalação nova

1. Extraia o projeto em uma pasta nova, separada da instalação anterior.
2. Execute `instalar.bat`. **Não execute `CRIAR_BASE_INICIAL.bat`**, pois a migração exige um destino vazio.
3. Abra um terminal na pasta nova e execute:

```powershell
.venv\Scripts\python.exe migrar_dados.py --origem "C:\CAMINHO_DA_INSTALACAO_ANTERIOR\radar_dados"
```

A origem precisa conter `compras.db`. Informe a pasta de dados completa do seu computador, não os bancos auxiliares isolados. O script aceita também a pasta raiz da instalação anterior, se ela contiver `radar_dados`.

O processo abre os bancos originais em modo de leitura e usa a API de backup do SQLite para copiar cada banco. Na cópia, adapta tabelas, índices e valores técnicos interpretados pelo programa. Conserva pesquisas, referências, buscas salvas, acompanhamentos e contadores de identidade. Textos e dados das pesquisas do usuário são preservados.

São copiadas todas as bases `*.db` da origem, incluindo índices e bases auxiliares existentes. Caches CSV, diagnósticos, ZIPs antigos e logs não são copiados; os logs históricos continuam na origem. Para operações que dependam de CSVs em cache, os arquivos podem precisar ser baixados novamente.

As verificações de integridade e chaves estrangeiras podem demorar bastante em bancos grandes. O script compara as violações de FK anteriores e posteriores à adaptação e relata violações preexistentes. Ele não corrige automaticamente inconsistências de dados da origem.

Se o comando falhar, a origem permanece preservada e a pasta final só é publicada depois que todas as cópias foram verificadas. Uma interrupção abrupta do computador pode deixar uma pasta temporária `.migracao_*` ao lado do destino; depois de confirmar que não há migração rodando, remova somente essa pasta temporária e execute novamente.

## Conferir e retomar

```powershell
.venv\Scripts\python.exe verificar_instalacao.py
```

Abra `iniciar.bat` e confira:

- Quantidade de contratações e itens e período da base.
- Pesquisas/cestas, referências e buscas salvas.
- Acompanhamentos existentes e novidades pendentes.
- Exportação de uma pesquisa para Excel e Word.
- Situação da base e histórico das execuções.

A coleta em segundo plano fica desativada na cópia. Os acompanhamentos ativos das buscas mantêm a configuração e podem ser verificados pela interface. As agendas do Windows não são transportadas: configure novamente a diária, a semanal e o mutirão na instalação nova, conforme sua rotina. Confirme que as agendas da instalação anterior continuam desativadas para evitar duas instalações coletando simultaneamente.

Os novos nomes de tarefas são `RADAR_PNCP_COLETA_DIARIA`, `RADAR_PNCP_COLETA_REVISAO_SEMANAL` e `RADAR_PNCP_MUTIRAO_ITENS_PERSISTENTE`.

Mantenha a instalação anterior encerrada como backup até conferir o funcionamento da nova. Se precisar voltar a ela, encerre a nova e desative suas agendas antes de reabrir a anterior. Alterações feitas na base nova não retornam à base original.
