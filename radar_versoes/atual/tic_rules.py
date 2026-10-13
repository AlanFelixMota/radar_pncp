#!/usr/bin/env python3
"""Regras heurísticas explicáveis de classificação TIC do Radar PNCP V0.6."""

from __future__ import annotations

RULES_VERSION = "2026.09.01-r1"
TIC_THRESHOLD = 35

# (termo/frase, peso-base). O peso recebe multiplicador conforme o local:
# objeto 1,5; informação complementar 1,25; item 1,0.
CATEGORIES: dict[str, list[tuple[str, int]]] = {
    "Segurança da informação": [
        ("segurança da informação", 28), ("cibersegurança", 30),
        ("cybersecurity", 28), ("firewall", 28), ("antivírus", 24),
        ("antivirus", 24), ("endpoint detection", 26), ("edr", 22),
        ("xdr", 22), ("siem", 24), ("soc", 18),
        ("gestão de vulnerabilidades", 26), ("vulnerability", 18),
        ("pentest", 24), ("teste de intrusão", 24),
        ("gestão de identidade", 22), ("iam", 18), ("mfa", 18),
        ("zero trust", 22), ("criptografia", 18),
    ],
    "Dados e privacidade": [
        ("lgpd", 30), ("privacidade", 24), ("privacy", 22),
        ("privacy tools", 36), ("dados pessoais", 22), ("dpo", 18),
        ("encarregado de dados", 20), ("anonimização", 20),
        ("consentimento", 14), ("governança de dados", 20),
        ("data governance", 20), ("data loss prevention", 24), ("dlp", 18),
    ],
    "Software e licenças": [
        ("software", 20), ("licenciamento", 20), ("licença de software", 26),
        ("licenças de software", 26), ("subscrição", 18), ("subscricao", 18),
        ("assinatura de software", 24), ("saas", 22),
        ("microsoft 365", 26), ("office 365", 26),
        ("power bi", 24), ("power apps", 24), ("adobe", 15),
        ("banco de dados", 16), ("database", 16),
        ("sistema informatizado", 24), ("sistema de informação", 22),
    ],
    "Redes e conectividade": [
        ("wi-fi", 28), ("wifi", 26), ("wireless", 22),
        ("access point", 28), ("ponto de acesso", 22), ("wlan", 22),
        ("switch", 20), ("switch ethernet", 24), ("roteador", 22),
        ("router", 20), ("sd-wan", 24), ("rede sem fio", 24),
        ("rede de dados", 18), ("fibra óptica", 16), ("sfp", 12),
        ("controladora wireless", 24), ("controlador wireless", 24),
    ],
    "Nuvem": [
        ("computação em nuvem", 30), ("computacao em nuvem", 30),
        ("cloud computing", 28), ("serviço de nuvem", 26),
        ("serviços de nuvem", 26), ("cloud", 14), ("iaas", 24),
        ("paas", 24), ("azure", 18), ("aws", 18),
        ("google cloud", 22), ("oracle cloud", 22),
    ],
    "Serviços de TI": [
        ("serviços de tecnologia da informação", 30),
        ("serviço de tecnologia da informação", 30),
        ("serviços de ti", 26), ("serviço de ti", 26),
        ("fábrica de software", 28), ("desenvolvimento de software", 28),
        ("sustentação de sistemas", 26), ("sustentacao de sistemas", 26),
        ("service desk", 24), ("help desk", 22),
        ("suporte técnico de ti", 24), ("suporte de ti", 22),
        ("consultoria de ti", 24), ("outsourcing de ti", 24),
        ("ust", 12), ("ponto de função", 18), ("pontos de função", 18),
    ],
    "Equipamentos de TIC": [
        ("notebook", 24), ("microcomputador", 24), ("computador", 18),
        ("desktop", 22), ("workstation", 22), ("estação de trabalho", 20),
        ("servidor", 20), ("server", 16), ("storage", 22),
        ("armazenamento de dados", 20), ("monitor", 12),
        ("scanner", 12), ("impressora", 12), ("tablet", 20),
        ("thin client", 22), ("nobreak", 10),
    ],
    "Infraestrutura e datacenter": [
        ("data center", 26), ("datacenter", 26), ("centro de dados", 24),
        ("rack", 14), ("servidor blade", 22), ("hyperconverg", 24),
        ("hiperconverg", 24), ("virtualização", 22), ("virtualizacao", 22),
        ("vmware", 22), ("hyper-v", 20), ("backup", 16),
        ("disaster recovery", 22), ("recuperação de desastre", 22),
    ],
    "Comunicações": [
        ("telefonia ip", 24), ("voip", 24), ("videoconferência", 20),
        ("videoconferencia", 20), ("comunicação unificada", 22),
        ("comunicacao unificada", 22), ("teams", 12),
        ("sip", 14), ("pbx", 18), ("central telefônica", 18),
    ],
}

# Expressões que, sozinhas, tendem a gerar falso positivo. Elas não anulam um
# conjunto forte de evidências, apenas reduzem a pontuação final.
NEGATIVE_TERMS: list[tuple[str, int]] = [
    ("software educacional impresso", 8),
    ("monitor de sinais vitais", 10),
    ("monitor cardíaco", 10),
    ("rede de proteção", 8),
]
