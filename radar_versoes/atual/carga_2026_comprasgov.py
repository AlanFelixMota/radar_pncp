#!/usr/bin/env python3
"""Bootstrap 2026 do Compras.gov — RADAR_PNCP/Radar PNCP V0.7.6.

Baixa/importa o conjunto anual corrente publicado no repositório oficial:
COMPRA, COMPRA_ITEM e ITEM_RESULTADO. O CSV enriquece a base, mas não afirma
completude de itens; essa certificação continua reservada ao PNCP.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import job_manager as jm
import radar_stats as rs
from comprasgov_resultados import importar_csv_resultados, garantir_tabelas as garantir_resultados
from importar_comprasgov_csv import (
    CSV_DIR, agora, atualizar_auxiliares, baixar, conectar_db, contar_orfaos,
    importar_arquivo, registrar_orfaos_resolvidos, resolver_itens_orfaos, sessao_http,
)
from proveniencia import registrar_cobertura
from radar_paths import DATA_DIR, DB_COMPRAS, DB_JOBS, DB_STATS, garantir_diretorios

BASE_ANUAL="https://repositorio.dados.gov.br/seges/comprasgov/anual"
ANO=2026


def atualizar_job(job_id, jobs_db, etapa, atual=0, total=0, mensagem=None):
    if job_id is not None and jobs_db is not None:
        jm.atualizar_job(job_id,jobs_db,etapa=etapa,progresso_atual=int(atual),progresso_total=int(total),mensagem=mensagem or etapa)


def info(recurso: str):
    bases={
        "COMPRA":"VW_FT_PNCP_COMPRA",
        "COMPRA_ITEM":"VW_FT_PNCP_COMPRA_ITEM",
        "ITEM_RESULTADO":"VW_DM_PNCP_ITEM_RESULTADO",
    }
    base=bases[recurso]
    nome=f"comprasGOV-anual-{base}-{ANO}.csv"
    url=f"{BASE_ANUAL}/{ANO}/{nome}"
    path=CSV_DIR/"anual"/str(ANO)/nome
    return url,path,f"{ANO}-ANUAL"


def espaco():
    livre=shutil.disk_usage(DATA_DIR).free
    print(f"Espaço livre no disco de dados: {livre/1024**3:.1f} GB",flush=True)
    if livre < 15*1024**3:
        raise RuntimeError("A carga 2026 requer pelo menos 15 GB livres para CSVs, SQLite, WAL e temporários.")
    if livre < 25*1024**3:
        print("AVISO: menos de 25 GB livres; a carga pode concluir, mas a margem para crescimento/índices será pequena.",flush=True)


def resolver(con,titulo,job_id,jobs_db):
    antes=contar_orfaos(con)
    print(f"\n[{titulo}] Staging antes: {antes:,}",flush=True)
    def cb(a,t): atualizar_job(job_id,jobs_db,titulo,a,t)
    qtd,por=resolver_itens_orfaos(con,progress_callback=cb)
    registrar_orfaos_resolvidos(con,por)
    print(f"    Resolvidos: {qtd:,} · staging restante: {contar_orfaos(con):,}",flush=True)
    return qtd


def main():
    ap=argparse.ArgumentParser(description="Carga massiva Compras.gov 2026 — V0.7.6")
    ap.add_argument("--db",type=Path,default=DB_COMPRAS)
    ap.add_argument("--jobs-db",type=Path,default=DB_JOBS)
    ap.add_argument("--job-id",type=int)
    args=ap.parse_args(); jobs_db=args.jobs_db if args.job_id else None
    garantir_diretorios(); CSV_DIR.mkdir(parents=True,exist_ok=True); espaco()
    print("="*92); print("Radar PNCP / Radar PNCP V0.7.6 — CARGA MASSIVA COMPRAS.GOV 2026"); print("="*92)
    print(f"Banco: {args.db.resolve()}")
    print("Recursos: contratações + itens + resultados homologados/fornecedores.")
    print("Regra: dados Compras.gov NÃO confirmam sozinhos a completude integral dos itens.")
    con=None
    try:
        con=conectar_db(args.db); garantir_resultados(con); s=sessao_http()
        staging0=contar_orfaos(con)
        recursos=["COMPRA","COMPRA_ITEM","ITEM_RESULTADO"]
        resumo={}
        for idx,recurso in enumerate(recursos,1):
            atualizar_job(args.job_id,jobs_db,f"2026 · {recurso}",idx-1,len(recursos))
            url,path,periodo=info(recurso)
            print(f"\n[2026 · {recurso}]",flush=True)
            baixar(s,url,path,periodo,None,None,recurso)
            if recurso=="ITEM_RESULTADO":
                resumo[recurso]=importar_csv_resultados(con,path,url,periodo,args.job_id,jobs_db)
            else:
                resumo[recurso]=importar_arquivo(con,path,url,periodo,recurso,args.job_id,jobs_db)
            if recurso=="COMPRA": resolver(con,"Após pais 2026",args.job_id,jobs_db)
            if recurso=="COMPRA_ITEM": resolver(con,"Após itens 2026",args.job_id,jobs_db)
            atualizar_job(args.job_id,jobs_db,f"2026 · {recurso} concluído",idx,len(recursos))
        registrar_cobertura(con,"comprasgov_2026_anual","IMPORTADO")
        registrar_cobertura(con,"comprasgov_2026_anual_em",agora())
        data_max_2026 = con.execute(
            """SELECT MAX(substr(data_publicacao_pncp,1,10))
                 FROM contratacoes
                WHERE csv_compra_mes='2026-ANUAL'
                  AND substr(data_publicacao_pncp,1,4)='2026'"""
        ).fetchone()[0]
        if data_max_2026:
            registrar_cobertura(con,"comprasgov_2026_anual_data_max",str(data_max_2026))
            print(f"Maior data de publicação encontrada no snapshot anual 2026: {data_max_2026}",flush=True)
            print("Use a COLETA incremental a partir do dia seguinte para fechar o intervalo recente.",flush=True)
        staging1=contar_orfaos(con)
        nres=int(con.execute("SELECT COUNT(*) FROM resultados_itens").fetchone()[0] or 0)
        con.commit(); con.close(); con=None
        atualizar_job(args.job_id,jobs_db,"Atualizando FTS/TIC",0,1)
        atualizar_auxiliares(args.db,args.job_id,jobs_db)
        atualizar_job(args.job_id,jobs_db,"Atualizando snapshot",0,1)
        rs.atualizar_automaticamente(args.db,DB_STATS)
        print("\n"+"="*92); print("RESUMO · CARGA 2026"); print("="*92)
        print(f"Staging: {staging0:,} → {staging1:,}")
        print(f"Linhas COMPRA processadas:        {resumo['COMPRA'].get('linhas',0):,}")
        print(f"Linhas COMPRA_ITEM processadas:   {resumo['COMPRA_ITEM'].get('linhas',0):,}")
        print(f"Linhas ITEM_RESULTADO processadas:{resumo['ITEM_RESULTADO'].get('linhas',0):>12,}")
        print(f"Resultados estruturados no banco: {nres:,}")
        print("Carga 2026 concluída.")
        return 0
    except KeyboardInterrupt:
        return 130
    finally:
        if con is not None:
            try: con.close()
            except Exception: pass

if __name__=="__main__":
    raise SystemExit(main())
