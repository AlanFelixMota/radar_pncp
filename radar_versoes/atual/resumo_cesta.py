"""Estatística descritiva por grupo, tipo de preço e unidade, sem conversões."""
from collections import Counter
from decimal import Decimal, InvalidOperation, localcontext
import math
import re


REGRAS = (
    'Somente referências com decisão Incluir entram nos cálculos.',
    'É necessário informar grupo e unidade e ter preço unitário positivo e finito.',
    'Referências com indicação de cancelamento ficam fora dos cálculos.',
    'Grupo, tipo de preço e unidade definem séries separadas; não há conversão de unidades.',
    'Itens estruturados usam sua identificação própria e não são misturados com grupos livres de mesmo nome.',
    'Grupo e unidade ignoram apenas diferenças de maiúsculas e espaços; abreviações e sinônimos não são unificados.',
    'Média e mediana usam uma observação por referência, sem ponderação por quantidade.',
    'Desvio padrão amostral e coeficiente de variação exigem pelo menos duas referências.',
    'As estatísticas não escolhem o preço final nem excluem automaticamente valores extremos.',
)


def _texto(valor):
    return ' '.join(str(valor or '').split())


def _preco(valor):
    if valor is None or isinstance(valor,bool):return None
    try:
        d=Decimal(str(valor))
        return d if d.is_finite() and d>0 and math.isfinite(float(d)) and float(d)>0 else None
    except (InvalidOperation,ValueError,OverflowError):
        return None


def consolidar(referencias, *, incluir_conferencia=False):
    series={};conferencia=[];motivos=Counter();incluidas=0;usadas=0
    for ref in referencias:
        d=ref['dados'];grupo=_texto(ref.get('grupo'));unidade=_texto(d.get('unidade_medida'))
        tipo=d.get('tipo_preco');preco=_preco(d.get('valor_unitario'))
        problemas=[]
        if ref.get('situacao')!='Incluir':
            problemas.append('Decisão: '+str(ref.get('situacao') or 'não informada'))
        else:
            incluidas+=1
            if not grupo:problemas.append('Grupo não informado')
            if not unidade:problemas.append('Unidade não informada')
            if tipo not in ('Estimado','Homologado'):problemas.append('Tipo de preço inválido')
            if preco is None:problemas.append('Preço unitário ausente, inválido ou não positivo')
            if d.get('cancelado'):problemas.append('Indicação de cancelamento')
            motivos.update(problemas)
        participa=not problemas
        if incluir_conferencia:
            conferencia.append({'id':ref['id'],'decisao':ref.get('situacao'),
                'grupo':grupo,'tipo_preco':tipo,'unidade':unidade,
                'participa':participa,'motivo':'; '.join(problemas) if problemas else 'Utilizada no resumo',
                'numero_controle_pncp':d.get('numero_controle_pncp'),'numero_item':d.get('numero_item'),
                'item_pesquisa_id':ref.get('item_pesquisa_id'),
                'item_pesquisa_numero':ref.get('item_pesquisa_numero'),
                'item_pesquisa_nome':ref.get('item_pesquisa_nome')})
        if not participa:continue
        identidade=('item',ref['item_pesquisa_id']) if ref.get('item_pesquisa_id') else ('livre',grupo.casefold())
        usadas+=1;chave=(*identidade,tipo,unidade.casefold())
        s=series.setdefault(chave,{'grupo':grupo,'tipo_preco':tipo,'unidade':unidade,
            'item_pesquisa_id':ref.get('item_pesquisa_id'),'item_pesquisa_numero':ref.get('item_pesquisa_numero'),
            'item_pesquisa_nome':ref.get('item_pesquisa_nome'),
            'precos':[],'contratos':set(),'fornecedores':set()})
        s['precos'].append(preco)
        if d.get('numero_controle_pncp'):s['contratos'].add(d['numero_controle_pncp'])
        fornecedor=_texto(d.get('ni_fornecedor')) or _texto(d.get('nome_fornecedor'))
        if re.fullmatch(r'[0-9./\-\s]+',fornecedor):fornecedor=re.sub(r'\D','',fornecedor)
        if fornecedor:s['fornecedores'].add(fornecedor.casefold())
    grupos=[]
    with localcontext() as ctx:
        ctx.prec=40
        ordenadas=sorted(series.values(),key=lambda s:(0 if s['item_pesquisa_id'] else 1,
            s['item_pesquisa_numero'] or 0,s['grupo'].casefold(),s['tipo_preco'],s['unidade'].casefold()))
        for s in ordenadas:
            valores=sorted(s['precos']);n=len(valores);media=sum(valores)/n
            mediana=valores[n//2] if n%2 else (valores[n//2-1]+valores[n//2])/2
            desvio=(sum((v-media)**2 for v in valores)/(n-1)).sqrt() if n>1 else None
            grupos.append({'grupo':s['grupo'],'tipo_preco':s['tipo_preco'],'unidade':s['unidade'],
                'item_pesquisa_id':s['item_pesquisa_id'],'item_pesquisa_numero':s['item_pesquisa_numero'],
                'item_pesquisa_nome':s['item_pesquisa_nome'],
                'referencias':n,'contratacoes':len(s['contratos']),'fornecedores':len(s['fornecedores']),
                'media':float(media),'mediana':float(mediana),'minimo':float(valores[0]),'maximo':float(valores[-1]),
                'desvio_padrao':float(desvio) if desvio is not None else None,
                'cv_percentual':float(desvio/media*100) if desvio is not None else None})
    return {'grupos':grupos,'incluidas':incluidas,'utilizadas':usadas,
            'fora_do_resumo':incluidas-usadas,'motivos':dict(motivos),'conferencia':conferencia}
