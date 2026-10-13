"""Resolve a raiz sem deslocar radar_dados ao mover o programa de pasta."""
from pathlib import Path


def raiz_da_instalacao(base, *, obrigatoria=False):
    base=Path(base).resolve()
    for root in (base,*base.parents):
        if (root/'radar_dados'/'radar_jobs.db').is_file():return root
    if obrigatoria:
        raise FileNotFoundError('Não encontrei radar_dados/radar_jobs.db. Extraia o pacote ao lado da pasta radar_dados existente.')
    # Compatibilidade com abertura inicial e testes sem uma base criada.
    if base.name in ('atual','anterior') and base.parent.name=='radar_versoes':return base.parent.parent
    return base.parent
