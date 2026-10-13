"""Nomes de navegação e apresentação do progresso, sem alterar a coleta."""
import re

VERSION = "1.0.0"
PAGINAS = ("Consultar", "Minhas pesquisas", "Acompanhamentos", "Sistema")
ROTULOS_NAV = PAGINAS
AJUDAS_NAV = (
    "Encontrar contratações, itens e preços de fornecedores.",
    "Continuar as pesquisas, revisar referências e exportar documentos.",
    "Acompanhar novas referências das buscas salvas.",
    "Consultar a base, acompanhar operações e configurar atualizações.",
)
AREAS_SISTEMA = ("Visão geral", "Situação da base", "Atividade", "Configurações")
AREAS_CONFIG = ("Visão geral", "Atualização automática", "Coleta em segundo plano", "Atualizar sistema", "Recuperar histórico", "Ferramentas técnicas")
# Atalhos antigos continuam funcionando, inclusive os guardados na sessão aberta.
ALIASES = {
    "Pesquisar": ("Consultar", None, None),
    "Cestas de pesquisa": ("Minhas pesquisas", None, None),
    "Situação da base": ("Sistema", "Situação da base", None),
    "Status da base": ("Sistema", "Situação da base", None),
    "Atividade": ("Sistema", "Atividade", None),
    "Execuções e Logs": ("Sistema", "Atividade", None),
    "Configurações": ("Sistema", "Configurações", None),
    "Administração": ("Sistema", "Configurações", "Visão geral"),
    "COLETA e Agendamentos": ("Sistema", "Configurações", "Atualização automática"),
    "Fila e Mutirão": ("Sistema", "Configurações", "Coleta em segundo plano"),
    "Saúde das Fontes": ("Sistema", "Configurações", "Ferramentas técnicas"),
    "Manutenção": ("Sistema", "Configurações", "Recuperar histórico"),
    "Atualizar sistema": ("Sistema", "Configurações", "Atualizar sistema"),
}


def resolver_rota(pagina):
    return ALIASES.get(pagina, (pagina if pagina in PAGINAS else "Consultar", None, None))

ESTADOS_COLETA = {
    "AGUARDANDO_JOB": "Aguardando a execução atual terminar",
    "AGUARDANDO_COLETA": "Aguardando a atualização diária",
    "COLETA_RETORNANDO": "Retomando uma atualização incompleta",
    "AGUARDANDO_PESQUISA": "Pausada no horário reservado para pesquisa",
    "PAUSADO_PESQUISA": "Pausada no horário reservado para pesquisa",
    "INATIVO": "Desativada", "ATIVANDO": "Ativando", "ATIVO": "Ativa",
    "AGUARDANDO": "Aguardando trabalho", "ERRO": "Precisa de atenção",
    "HIBRIDO_ATIVO": "Coletando itens pelo Compras.gov",
    "PNCP_FALLBACK_ATIVO": "Consultando itens complementares no PNCP",
    "RESULTADOS_PNCP": "Coletando resultados por item no PNCP",
    "HORARIO_PESQUISA": "Pausada no horário reservado para pesquisa",
    "SINCRONIZANDO_PESQUISA": "Atualizando o índice de pesquisa",
    "AGUARDANDO_INDICE": "Aguardando o índice de pesquisa",
    "PAUSADO_USUARIO": "Pausada pelo usuário",
    "PAUSADO_SEM_COLETA": "Aguardando configuração da atualização diária",
    "AGUARDANDO_COMPRASGOV": "Aguardando nova tentativa no Compras.gov",
    "AGUARDANDO_ATUALIZACOES": "Aguardando novos dados",
    "COOLDOWN": "Aguardando para retomar as consultas",
}


def estado_coleta(valor):
    return ESTADOS_COLETA.get(str(valor or ""), str(valor or "Aguardando informações").replace("_", " ").capitalize())


def progresso_resultados(mensagem):
    """Extrai somente contadores explícitos; nunca infere duração restante."""
    m = re.search(r"Resultados\s*·\s*(\d{4}-\d{2}-\d{2})\s*·\s*(\d+)/(\d+)\s+dias", str(mensagem or ""))
    if not m:
        return None
    dia, atual, total = m.group(1), int(m.group(2)), int(m.group(3))
    if total <= 0 or not 0 <= atual <= total:
        return None
    return {"dia": dia, "atual": atual, "total": total, "fracao": atual / total}
