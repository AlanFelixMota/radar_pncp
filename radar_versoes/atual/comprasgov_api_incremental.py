#!/usr/bin/env python3
"""COLETA incremental via API oficial de Dados Abertos do Compras.gov — V0.7.6.

Objetivo: manter a base recente mesmo quando algum serviço do PNCP estiver fora.
- Contratações: endpoint 1_consultarContratacoes_PNCP_14133, por data/modalidade.
- Itens: endpoint 2.1 por idCompra, como ENRIQUECIMENTO; nunca confirma completude.
- Resultados: endpoint 3 por dataResultadoPncp, com fornecedor e valores homologados.

A API do Compras.gov é fonte complementar. O retorno de itens pode ser parcial e
não altera itens_coletados=1.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import job_manager as jm
import radar_stats as rs
from comprasgov_resultados import garantir_tabelas as garantir_resultados, upsert_json_rows
from importar_comprasgov_csv import (
    COMPRA_MAP, ITEM_MAP, SQL_COMPRA_API, SQL_ITEM_API, COLS_COMPRA, COLS_ITEM,
    agora, conectar_db, parametros_compra, parametros_item, resolver_colunas,
    atualizar_auxiliares,
)
from proveniencia import FONTE_COMPRASGOV_API, registrar_cobertura
from radar_paths import DB_COMPRAS, DB_JOBS, DB_STATS

BASE="https://dadosabertos.compras.gov.br"
URL_COMPRAS=f"{BASE}/modulo-contratacoes/1_consultarContratacoes_PNCP_14133"
URL_ITENS_ID=f"{BASE}/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id"
URL_RESULTADOS=f"{BASE}/modulo-contratacoes/3_consultarResultadoItensContratacoes_PNCP_14133"
# Tabela de modalidades publicada no Manual da API Compras.gov. Alguns códigos
# podem retornar vazio para a Lei 14.133; isso não é erro.
MODALIDADES=(1,2,3,4,5,6,7,12,20,22,33,44,57)
PAGE=500


def sessao():
    s=requests.Session()
    retry=Retry(total=3,connect=3,read=3,backoff_factor=1.0,status_forcelist=(429,500,502,503,504),
                allowed_methods=frozenset({"GET"}),respect_retry_after_header=True)
    s.mount("https://",HTTPAdapter(max_retries=retry))
    s.headers.update({"Accept":"application/json,*/*","User-Agent":"Mozilla/5.0 RADAR_PNCP-COLETA/0.6.6.4"})
    return s


def get_json(s,url,params,timeout=(20,90)):
    r=s.get(url,params=params,timeout=timeout)
    r.raise_for_status()
    obj=r.json()
    if not isinstance(obj,dict): raise RuntimeError("Resposta JSON inesperada do Compras.gov")
    return obj


def datas(inicio:str,fim:str):
    a=date.fromisoformat(inicio); b=date.fromisoformat(fim)
    if b<a: raise ValueError("Data final anterior à inicial")
    if (b-a).days>90: raise ValueError("Para segurança, processe no máximo 91 dias por job.")
    while a<=b:
        yield a.isoformat(); a+=timedelta(days=1)


def atualizar(job_id,jobs_db,etapa,a=0,t=0,msg=None):
    if job_id is not None and jobs_db is not None:
        jm.atualizar_job(job_id,jobs_db,etapa=etapa,progresso_atual=int(a),progresso_total=int(t),mensagem=msg or etapa)


def upsert_compra_api(con,row,periodo,stamp):
    res=resolver_colunas(list(row.keys()),COMPRA_MAP)
    p=parametros_compra(row,res,periodo,stamp,fonte=FONTE_COMPRASGOV_API)
    if p is None: return None
    pid=p[COLS_COMPRA.index("numero_controle_pncp")]
    con.execute(SQL_COMPRA_API,p)
    return str(pid)


def upsert_item_api(con,row,periodo,stamp):
    res=resolver_colunas(list(row.keys()),ITEM_MAP)
    p,situ=parametros_item(row,res,periodo,stamp,fonte=FONTE_COMPRASGOV_API)
    if p is None or situ!="VALIDO": return None
    pid=str(p[COLS_ITEM.index("numero_controle_pncp")])
    # Pai precisa existir para respeitar a FK; contratos deste job são gravados primeiro.
    if not con.execute("SELECT 1 FROM contratacoes WHERE numero_controle_pncp=?",(pid,)).fetchone():
        return None
    con.execute(SQL_ITEM_API,p)
    con.execute("""UPDATE contratacoes SET itens_fonte=?
                    WHERE numero_controle_pncp=? AND COALESCE(itens_coletados,0)=0""",
                (FONTE_COMPRASGOV_API,pid))
    return pid


def _assinatura_pagina(rows: list[dict[str, Any]]) -> str:
    """Assinatura curta para detectar paginação travada/repetida.

    Há relatos públicos de metadados de paginação inconsistentes nessa API.
    Por isso a COLETA não usa totalPaginas como critério único de parada e protege
    contra uma mesma página retornada indefinidamente.
    """
    if not rows:
        return "VAZIA"
    def chave(r: dict[str, Any]) -> str:
        return str(r.get("idCompraItem") or r.get("idCompra") or r.get("idContratacaoPNCP")
                   or r.get("numeroControlePNCP") or r.get("sequencialResultado") or repr(sorted(r.items())[:5]))
    return f"{len(rows)}|{chave(rows[0])}|{chave(rows[-1])}"


def paginar(s,url,params_base):
    pagina=1
    assinaturas=set()
    while True:
        params=dict(params_base); params.update({"pagina":pagina,"tamanhoPagina":PAGE})
        obj=get_json(s,url,params)
        rows=obj.get("resultado") or []
        if not isinstance(rows,list): rows=[]
        sig=_assinatura_pagina(rows)
        if rows and sig in assinaturas:
            raise RuntimeError(f"Paginação repetiu a mesma página na página {pagina}; interrompendo para evitar loop/duplicação.")
        if rows:
            assinaturas.add(sig)
        yield rows,obj,pagina
        # Critério mais confiável: página vazia ou menor que o tamanho solicitado.
        # totalPaginas/paginasRestantes ficam apenas como metadados, pois já houve
        # relatos públicos de valores inconsistentes nesses campos.
        if not rows or len(rows)<PAGE:
            break
        pagina+=1
        if pagina>10000:
            raise RuntimeError("Mais de 10.000 páginas para uma única consulta; abortado por segurança.")


def buscar_compras_dia(con,s,dia,com_itens,job_id,jobs_db):
    stamp=agora(); vistos=set(); ids=[]; aplicadas=0; erros=0
    for mi,mod in enumerate(MODALIDADES,1):
        atualizar(job_id,jobs_db,f"Compras.gov API · {dia} · modalidade {mod}",mi-1,len(MODALIDADES))
        base={"dataPublicacaoPncpInicial":dia,"dataPublicacaoPncpFinal":dia,"codigoModalidade":mod}
        try:
            for rows,obj,pag in paginar(s,URL_COMPRAS,base):
                for row in rows:
                    pid=upsert_compra_api(con,row,dia,stamp)
                    if pid:
                        aplicadas+=1; vistos.add(pid)
                        idc=row.get("idCompra") or row.get("id_compra")
                        if idc: ids.append(str(idc))
                con.commit()
        except Exception as exc:
            erros+=1; print(f"    AVISO modalidade {mod}: {type(exc).__name__}: {exc}",flush=True)
    ids=list(dict.fromkeys(ids))
    itens=0; erros_itens=0
    if com_itens:
        print(f"    Enriquecendo itens por idCompra: {len(ids):,} contratação(ões).",flush=True)
        for n,idc in enumerate(ids,1):
            if n%100==0: atualizar(job_id,jobs_db,f"Itens Compras.gov · {dia}",n,len(ids))
            obj=None
            # A API já apareceu publicamente com duas formas de parâmetros. Testamos a
            # forma tipo/codigo e, em caso de 400/404, tentamos idCompra. Qualquer retorno
            # é tratado como enriquecimento parcial, nunca completude.
            for params in ({"tipo":"idCompra","codigo":idc},{"idCompra":idc}):
                try:
                    obj=get_json(s,URL_ITENS_ID,params)
                    break
                except requests.HTTPError as exc:
                    code=getattr(exc.response,"status_code",None)
                    if code not in (400,404,422):
                        obj=None; break
                except Exception:
                    obj=None; break
            if not obj:
                erros_itens+=1; continue
            rows=obj.get("resultado") or []
            if not isinstance(rows,list): continue
            for row in rows:
                if upsert_item_api(con,row,dia,stamp): itens+=1
            if n%50==0: con.commit()
        con.commit()
    return {"contratacoes":len(vistos),"linhas_contratacoes":aplicadas,"itens":itens,"erros_modalidades":erros,"erros_itens":erros_itens}


def buscar_resultados_dia(con,s,dia):
    total=0; rejeit=0; paginas=0
    base={"dataResultadoPncpInicial":dia,"dataResultadoPncpFinal":dia}
    try:
        for rows,obj,pag in paginar(s,URL_RESULTADOS,base):
            paginas+=1
            a,b=upsert_json_rows(con,rows,fonte=FONTE_COMPRASGOV_API,periodo=dia)
            total+=a; rejeit+=b
    except Exception as exc:
        print(f"    AVISO resultados {dia}: {type(exc).__name__}: {exc}",flush=True)
        return total,rejeit,1
    return total,rejeit,0


def main():
    ap=argparse.ArgumentParser(description="COLETA incremental Compras.gov API — V0.7.6")
    ap.add_argument("--inicio",required=True); ap.add_argument("--fim",required=True)
    ap.add_argument("--db",type=Path,default=DB_COMPRAS); ap.add_argument("--jobs-db",type=Path,default=DB_JOBS); ap.add_argument("--job-id",type=int)
    ap.add_argument("--sem-itens",action="store_true",help="Não consultar o endpoint de itens por idCompra")
    args=ap.parse_args(); jobs_db=args.jobs_db if args.job_id else None
    ds=list(datas(args.inicio,args.fim))
    print("="*92); print("Radar PNCP / Radar PNCP V0.7.6 — COLETA INCREMENTAL · API COMPRAS.GOV"); print("="*92)
    print(f"Período: {args.inicio} → {args.fim} · dias {len(ds)}")
    print("Itens via Compras.gov são enriquecimento; nunca confirmam completude.")
    con=conectar_db(args.db); garantir_resultados(con); s=sessao()
    totais={"contratacoes":0,"itens":0,"resultados":0,"erros":0}
    try:
        print("[PREFLIGHT] Testando API de contratações do Compras.gov...",flush=True)
        try:
            get_json(s,URL_COMPRAS,{
                "dataPublicacaoPncpInicial":ds[0],"dataPublicacaoPncpFinal":ds[0],
                "codigoModalidade":6,"pagina":1,"tamanhoPagina":10,
            },timeout=(10,30))
            print("[PREFLIGHT] API Compras.gov respondeu; iniciando janela.",flush=True)
        except Exception as exc:
            print(f"[PREFLIGHT] API Compras.gov indisponível/instável: {type(exc).__name__}: {exc}",flush=True)
            print("Nenhum dia foi iniciado; execute novamente mais tarde.",flush=True)
            return 75

        for di,dia in enumerate(ds,1):
            atualizar(args.job_id,jobs_db,f"COLETA Compras.gov · {dia}",di-1,len(ds))
            print(f"\n[{dia}] Contratações",flush=True)
            rc=buscar_compras_dia(con,s,dia,not args.sem_itens,args.job_id,jobs_db)
            print(f"    Pais: {rc['contratacoes']:,} · itens enriquecidos: {rc['itens']:,}",flush=True)
            print(f"[{dia}] Resultados homologados",flush=True)
            rr,rq,re=buscar_resultados_dia(con,s,dia)
            print(f"    Resultados: {rr:,} · quarentena: {rq:,}",flush=True)
            totais["contratacoes"]+=rc["contratacoes"]; totais["itens"]+=rc["itens"]; totais["resultados"]+=rr
            totais["erros"]+=rc["erros_modalidades"]+rc["erros_itens"]+re
            registrar_cobertura(con,"comprasgov_api_ultimo_dia",dia)
            atualizar(args.job_id,jobs_db,f"COLETA Compras.gov · {dia} concluído",di,len(ds))
        con.commit(); con.close(); con=None
        print("\n[FINAL] Atualizando FTS/TIC existentes...",flush=True)
        atualizar_auxiliares(args.db,args.job_id,jobs_db)
        print("[FINAL] Atualizando snapshot...",flush=True)
        rs.atualizar_automaticamente(args.db,DB_STATS)
        print("\n"+"="*92); print("RESUMO · COLETA INCREMENTAL COMPRAS.GOV"); print("="*92)
        print(f"Contratações distintas encontradas: {totais['contratacoes']:,}")
        print(f"Itens enriquecidos/aplicados:        {totais['itens']:,}")
        print(f"Resultados aplicados:                {totais['resultados']:,}")
        print(f"Falhas parciais observadas:          {totais['erros']:,}")
        # Falha em um endpoint complementar não desfaz dados já coletados; marca parcial.
        return 75 if totais["erros"] else 0
    except KeyboardInterrupt:
        return 130
    finally:
        if con is not None:
            try: con.close()
            except Exception: pass

if __name__=="__main__": raise SystemExit(main())
