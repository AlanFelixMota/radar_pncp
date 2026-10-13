"""Filtros locais de unidade e preço unitário, compartilhados pelos dois modos."""
import math
import re
import unicodedata
from decimal import Decimal


def unidade(valor):
    texto=' '.join(str(valor or '').split())
    return ''.join(c for c in unicodedata.normalize('NFKD',texto) if not unicodedata.combining(c)).casefold()


def ler_preco(texto):
    texto=str(texto or '').strip()
    if not texto:return None
    # Vírgula decimal permite agrupamento brasileiro: 1.234,5678.
    if ',' in texto:
        if not re.fullmatch(r'(?:\d+|\d{1,3}(?:\.\d{3})+),\d+',texto):
            raise ValueError('Informe o preço como 0,0300 ou 1234,56, sem símbolo de moeda.')
        texto=texto.replace('.','').replace(',','.')
    elif not re.fullmatch(r'\d+(?:\.\d+)?',texto):
        raise ValueError('Informe o preço como 0,0300 ou 1234.56, sem símbolo de moeda.')
    valor=float(texto)
    if not math.isfinite(valor):raise ValueError('Informe um preço finito.')
    if valor==0 and Decimal(texto)!=0:raise ValueError('O preço informado é pequeno demais para a precisão da base.')
    return valor


def validar(f):
    if not isinstance(f.unidade,str) or len(f.unidade)>200:
        raise ValueError('A unidade de medida deve ter até 200 caracteres.')
    minimo,maximo=f.preco_min,f.preco_max
    for valor in (minimo,maximo):
        if valor is not None:
            try:valido=type(valor) in (float,int) and math.isfinite(valor) and valor>=0
            except OverflowError:valido=False
            if not valido:raise ValueError('O preço unitário deve ser um número finito maior ou igual a zero.')
    if minimo is not None and maximo is not None and minimo>maximo:
        raise ValueError('O preço unitário mínimo não pode ser maior que o máximo.')
    if hasattr(f,'preco_tipo') and f.preco_tipo not in ('Estimado','Homologado'):
        raise ValueError('Tipo de preço inválido.')


def ativo(f):
    return bool(f.unidade.strip() or f.preco_min is not None or f.preco_max is not None)


def registrar(con):
    con.create_function('unidade_filtro',1,unidade,deterministic=True)


def clausula(f,coluna_unidade,coluna_preco):
    validar(f)
    termos=[];params=[]
    if f.unidade.strip():
        termos.append(f'unidade_filtro({coluna_unidade})=?');params.append(unidade(f.unidade))
    if f.preco_min is not None or f.preco_max is not None:
        # Sem CAST: preços ausentes ou texto não numérico não viram zero.
        termos.append(f"typeof({coluna_preco}) IN ('integer','real')")
        termos.append(f'abs({coluna_preco})<=1.7976931348623157e308')
        if f.preco_min is not None:
            termos.append(f'{coluna_preco}>=?');params.append(f.preco_min)
        if f.preco_max is not None:
            termos.append(f'{coluna_preco}<=?');params.append(f.preco_max)
    return ' AND '.join(termos) or '1=1',params


def exige_resultados(f):
    return f.somente_homologadas or (ativo(f) and f.preco_tipo=='Homologado')


def item_contratacao(f):
    """Condição sobre i; unidade e faixa pertencem ao mesmo item/resultado."""
    if not ativo(f):return '1=1',[]
    if f.preco_tipo=='Estimado':return clausula(f,'i.unidade_medida','i.valor_unitario_estimado')
    from pesquisa_fornecedores import CANCELADO
    where,params=clausula(f,'i.unidade_medida','r.valor_unitario_homologado')
    return ('EXISTS (SELECT 1 FROM resultados_itens r WHERE r.numero_controle_pncp=i.numero_controle_pncp '
            'AND r.numero_item=i.numero_item AND NOT '+CANCELADO+' AND '+where+')'),params


def contratacao(f):
    if f.preco_tipo=='Estimado':
        where,params=item_contratacao(f)
        return 'EXISTS (SELECT 1 FROM itens i WHERE i.numero_controle_pncp=c.numero_controle_pncp AND '+where+')',params
    from pesquisa_fornecedores import CANCELADO
    where,params=clausula(f,'i.unidade_medida','r.valor_unitario_homologado')
    # Preço homologado pode existir antes da coleta de descrição/unidade.
    return ('EXISTS (SELECT 1 FROM resultados_itens r LEFT JOIN itens i ON '
            'i.numero_controle_pncp=r.numero_controle_pncp AND i.numero_item=r.numero_item '
            'WHERE r.numero_controle_pncp=c.numero_controle_pncp AND NOT '+CANCELADO+' AND '+where+')'),params


def rotulo(f):
    partes=[]
    if f.unidade.strip():partes.append('Unidade exata: '+f.unidade.strip())
    if f.preco_min is not None:partes.append('mínimo R$ '+texto_preco(f.preco_min))
    if f.preco_max is not None:partes.append('máximo R$ '+texto_preco(f.preco_max))
    if partes:partes.insert(0,'Preço '+getattr(f,'preco_tipo','Homologado').lower())
    return ' · '.join(partes)


def texto_preco(valor):
    return '' if valor is None else format(Decimal(str(valor)),'f').replace('.',',')


def campos(prefix,*,tipo=False):
    import streamlit as st
    medida=st.text_input('Unidade de medida (exata)',placeholder='Ex.: página, unidade, licença',max_chars=200,key=prefix+'_unidade')
    a,b=st.columns(2)
    minimo=a.text_input('Preço unitário mínimo (R$)',placeholder='Opcional. Ex.: 0,0300',key=prefix+'_preco_min')
    maximo=b.text_input('Preço unitário máximo (R$)',placeholder='Opcional. Ex.: 0,0500',key=prefix+'_preco_max')
    tipo_preco=st.selectbox('Tipo de preço para unidade e faixa',['Estimado','Homologado'],key=prefix+'_preco_tipo') if tipo else 'Homologado'
    st.caption('Limites inclusivos; campos vazios deixam o preço sem limite. Use vírgula ou ponto decimal. A unidade ignora acentos, maiúsculas e espaços repetidos; abreviações e unidades diferentes permanecem separadas.')
    return medida,minimo,maximo,tipo_preco
