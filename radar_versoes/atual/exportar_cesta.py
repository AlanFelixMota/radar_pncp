"""Preenche o modelo XLSX distribuído com o Radar PNCP, usando apenas a biblioteca padrão.

Texto de fontes e usuários é sempre texto literal, nunca fórmula. Valores nulos
permanecem vazios; números não são convertidos para strings monetárias.
"""
from datetime import datetime
from io import BytesIO
import math
from pathlib import Path
import re
import xml.etree.ElementTree as ET
import zipfile
from resumo_cesta import consolidar, REGRAS

COLUNAS=[
 ('ID da referência','id','inteiro'),('Grupo da pesquisa','grupo','texto'),
 ('Decisão','situacao','texto'),('Tipo de preço','tipo_preco','texto'),
 ('Descrição','descricao','texto'),('Unidade','unidade_medida','texto'),
 ('Quantidade','quantidade','numero'),('Preço unitário (R$)','valor_unitario','moeda4'),
 ('Valor total informado (R$)','valor_total','moeda4'),('Data do resultado','data_preco','data'),
 ('Fornecedor','nome_fornecedor','texto'),('CNPJ / identificação','ni_fornecedor','texto'),
 ('Órgão','orgao_razao_social','texto'),('CNPJ do órgão','orgao_cnpj','texto'),
 ('Unidade compradora','unidade_nome','texto'),('UF','uf_sigla','texto'),
 ('Processo','processo','texto'),('Número da compra','numero_compra','texto'),
 ('Ano da compra','ano_compra','inteiro'),('Controle PNCP','numero_controle_pncp','texto'),
 ('Item','numero_item','inteiro'),('Sequencial do resultado','sequencial_resultado','inteiro'),
 ('Link PNCP','url_pncp','link'),('Link do sistema de origem','link_sistema_origem','link'),
 ('Fonte do preço','fonte_ultima','texto'),('Importado em','importado_em','texto'),
 ('Atualizado na fonte','data_atualizacao_fonte','texto'),('Referência salva em','selecionado_em','texto'),
 ('Consulta local em','consultado_em','texto'),('Cancelado na seleção','cancelado','bool'),
 ('Data do cancelamento','data_cancelamento_pncp','data'),('Motivo do cancelamento','motivo_cancelamento','texto'),
 ('Situação na fonte','situacao_resultado_nome','texto'),('Justificativa da decisão','justificativa','texto'),
 ('Observações de comparabilidade','observacoes','texto'),('Avaliação salva em','revisado_em','texto'),
 ('Informação complementar','informacao_complementar','texto'),('Data de publicação','data_publicacao_pncp','data'),
 ('Contratação disponível na seleção','contratacao_local','bool'),('Item disponível na seleção','item_local','bool'),
 ('ID do item da pesquisa','item_pesquisa_id','inteiro'),('Número do item da pesquisa','item_pesquisa_numero','inteiro'),
 ('Nome do item da pesquisa','item_pesquisa_nome','texto'),('Descrição do item da pesquisa','item_pesquisa_descricao','texto'),
 ('Unidade pretendida da pesquisa','item_pesquisa_unidade','texto'),
 ('Resultado da conferência local','conferencia_estado','texto'),('Conferência local em','conferencia_em','texto'),
 ('Preço unitário local na conferência (R$)','conferencia_preco_local','moeda4'),
 ('Situação local na conferência','conferencia_situacao_local','texto'),
 ('Fonte do preço local na conferência','conferencia_fonte','texto'),
 ('Importado/coletado local em','conferencia_importado_em','texto'),
 ('Atualizado na fonte do registro local','conferencia_atualizado_fonte','texto'),
 ('Última consulta PNCP registrada','conferencia_pncp_em','texto'),
 ('Avisos da conferência','conferencia_avisos','texto'),
]
NS='http://schemas.openxmlformats.org/spreadsheetml/2006/main'
REL='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
PKG='http://schemas.openxmlformats.org/package/2006/relationships'
CT='http://schemas.openxmlformats.org/package/2006/content-types'

COLUNAS_RESUMO=[('Grupo','grupo','texto',32),('Tipo de preço','tipo_preco','texto',18),
 ('Unidade','unidade','texto',24),('Referências','referencias','inteiro',14),
 ('Contratações','contratacoes','inteiro',16),('Fornecedores','fornecedores','inteiro',16),
 ('Média (R$)','media','moeda4',21),('Mediana (R$)','mediana','moeda4',21),
 ('Mínimo (R$)','minimo','moeda4',21),('Máximo (R$)','maximo','moeda4',21),
 ('Desvio padrão amostral (R$)','desvio_padrao','moeda4',24),('CV (%)','cv_percentual','numero',16)]
COLUNAS_RESUMO += [('ID do item da pesquisa','item_pesquisa_id','inteiro',20),
 ('Número do item da pesquisa','item_pesquisa_numero','inteiro',23),('Nome do item da pesquisa','item_pesquisa_nome','texto',36)]
COLUNAS_CONFERENCIA=[('ID da referência','id','inteiro',14),('Decisão','decisao','texto',16),
 ('Grupo','grupo','texto',32),('Tipo de preço','tipo_preco','texto',18),('Unidade','unidade','texto',24),
 ('Participa do resumo','participa','bool',20),('Motivo','motivo','texto',58),
 ('Controle PNCP','numero_controle_pncp','texto',36),('Item','numero_item','inteiro',12)]
COLUNAS_CONFERENCIA += [('ID do item da pesquisa','item_pesquisa_id','inteiro',20),
 ('Número do item da pesquisa','item_pesquisa_numero','inteiro',23),('Nome do item da pesquisa','item_pesquisa_nome','texto',36)]
COLUNAS_ITENS=[('ID do item da pesquisa','id','inteiro',20),('Número','numero','inteiro',12),
 ('Nome','nome','texto',36),('Descrição / critérios de comparabilidade','descricao','texto',66),
 ('Unidade pretendida','unidade','texto',24),('Referências vinculadas','referencias','inteiro',23),
 ('Incluir','incluidas','inteiro',14),('A avaliar','a_avaliar','inteiro',14),('Excluir','excluidas','inteiro',14)]


def _nova_planilha(titulo,colunas,registros,notas,*,estilo_cv='16',links_referencias=None):
    root=ET.Element('{'+NS+'}worksheet')
    ET.SubElement(root,'{'+NS+'}dimension',{'ref':f'A1:{col(len(colunas))}{max(6,len(registros)+5)}'})
    views=ET.SubElement(root,'{'+NS+'}sheetViews')
    view=ET.SubElement(views,'{'+NS+'}sheetView',{'showGridLines':'0','workbookViewId':'0'})
    ET.SubElement(view,'{'+NS+'}pane',{'ySplit':'5','topLeftCell':'A6','activePane':'bottomLeft','state':'frozen'})
    ET.SubElement(root,'{'+NS+'}sheetFormatPr',{'defaultRowHeight':'18'})
    widths=ET.SubElement(root,'{'+NS+'}cols')
    for j,(_,_,_,largura) in enumerate(colunas,1):
        ET.SubElement(widths,'{'+NS+'}col',{'min':str(j),'max':str(j),'width':str(largura),'customWidth':'1'})
    sd=ET.SubElement(root,'{'+NS+'}sheetData')
    for i,text in enumerate([titulo,*notas],1):
        row=ET.SubElement(sd,'{'+NS+'}row',{'r':str(i),'ht':str(30 if i==1 else 48),'customHeight':'1'})
        row.append(_celula('A'+str(i),text,'texto','2' if i==1 else '9'))
    head=ET.SubElement(sd,'{'+NS+'}row',{'r':'5','ht':'36','customHeight':'1'})
    for j,(label,_,_,_) in enumerate(colunas,1):head.append(_celula(col(j)+'5',label,'texto','6'))
    hyperlinks=ET.Element('{'+NS+'}hyperlinks')
    for linha,registro in enumerate(registros,6):
        row=ET.SubElement(sd,'{'+NS+'}row',{'r':str(linha),'ht':'44','customHeight':'1'})
        for j,(_,campo,tipo,_) in enumerate(colunas,1):
            style='17' if tipo=='moeda4' else ('12' if tipo=='inteiro' else '9')
            if campo=='cv_percentual':style=estilo_cv
            row.append(_celula(col(j)+str(linha),registro.get(campo),tipo,style))
        if links_referencias and registro.get('id') in links_referencias:
            ET.SubElement(hyperlinks,'{'+NS+'}hyperlink',{'ref':'A'+str(linha),
                'location':"'Referências'!A"+str(links_referencias[registro['id']]),'tooltip':'Conferir referência e origem'})
    if not registros:
        row=ET.SubElement(sd,'{'+NS+'}row',{'r':'6'})
        row.append(_celula('A6','Nenhuma referência apta para o resumo. Confira decisões, grupos, unidades e preços.','texto','9'))
    ET.SubElement(root,'{'+NS+'}autoFilter',{'ref':f'A5:{col(len(colunas))}{max(5,len(registros)+5)}'})
    merges=ET.SubElement(root,'{'+NS+'}mergeCells',{'count':'4'})
    for linha in range(1,5):ET.SubElement(merges,'{'+NS+'}mergeCell',{'ref':f'A{linha}:{col(len(colunas))}{linha}'})
    if len(hyperlinks):root.append(hyperlinks)
    return ET.tostring(root,encoding='utf-8',xml_declaration=True)


def _adicionar_resumo(arquivos,referencias,pesquisa):
    resumo=consolidar(referencias,incluir_conferencia=True)
    styles=ET.fromstring(arquivos['xl/styles.xml']);xfs=styles.find('{'+NS+'}cellXfs')
    cvstyle=ET.fromstring(ET.tostring(xfs[16]));cvstyle.set('numFmtId','2')
    indice_cv=str(len(xfs));xfs.append(cvstyle);xfs.set('count',str(len(xfs)))
    arquivos['xl/styles.xml']=ET.tostring(styles,encoding='utf-8',xml_declaration=True)
    contador=f"Incluídas: {resumo['incluidas']} · Utilizadas nos cálculos: {resumo['utilizadas']} · Incluídas fora do resumo: {resumo['fora_do_resumo']}"
    arquivos['xl/worksheets/sheet3.xml']=_nova_planilha('Resumo de preços por grupo',COLUNAS_RESUMO,
        resumo['grupos'],['Somente referências Incluir, com grupo, unidade e preço positivo, sem indicação de cancelamento. Uma observação por referência; sem ponderação por quantidade.',
        'Resumo calculado na exportação. Alterações feitas apenas na aba Referências não recalculam estes números. Avalie no Radar PNCP e exporte novamente. Desvio padrão amostral e CV exigem duas referências.',contador],estilo_cv=indice_cv)
    arquivos['xl/worksheets/sheet4.xml']=_nova_planilha('Conferência do resumo',COLUNAS_CONFERENCIA,
        resumo['conferencia'],['Clique no ID para conferir a linha correspondente em Referências e seus dados de origem.',
        'Esta conferência retrata as decisões salvas no Radar PNCP no momento da exportação. Uma referência pode ter mais de um motivo para ficar fora dos cálculos.',contador],
        links_referencias={r['id']:linha for linha,r in enumerate(referencias,6)})
    workbook=ET.fromstring(arquivos['xl/workbook.xml'])
    sheets=workbook.find('{'+NS+'}sheets')
    rels=ET.fromstring(arquivos['xl/_rels/workbook.xml.rels'])
    types=ET.fromstring(arquivos['[Content_Types].xml'])
    novas=[(3,'Resumo de preços'),(4,'Conferência do resumo')]
    if pesquisa.get('itens'):
        from itens_cesta import painel
        arquivos['xl/worksheets/sheet5.xml']=_nova_planilha('Itens da pesquisa',COLUNAS_ITENS,
            painel(pesquisa['itens'],referencias),['Números e nomes desta aba pertencem à sua pesquisa, não à contratação de origem.',
            'Unidade pretendida e descrição orientam a conferência; não convertem unidades nem aprovam referências automaticamente.',
            'Inclui itens sem referências. Estatísticas de preços permanecem separadas por item, tipo de preço e unidade na aba Resumo de preços.'])
        novas.append((5,'Itens da pesquisa'))
    if any(r.get('conferencia') for r in referencias):
        from conferencia_cesta import colunas_exportacao,estado
        colunas=[('ID da referência','id','inteiro',16),('Controle PNCP','pid','texto',35),
            ('Item da contratação','item','inteiro',20),('Sequencial do resultado','seq','inteiro',22),
            ('Tipo de preço','tipo','texto',18),('Decisão salva','decisao','texto',18),
            ('Conferência local em','consultado_em','texto',32),('Resultado','estado','texto',32),
            ('Campo comparado','campo','texto',32),('Cópia salva na cesta','salvo','texto',48),
            ('Registro local na conferência','atual','texto',48),('Avisos','avisos','texto',65)]
        linhas=[]
        for ref in referencias:
            d=ref['dados'];check=ref.get('conferencia') or {}
            meta={'id':ref['id'],'pid':d['numero_controle_pncp'],'item':d['numero_item'],
                  'seq':d.get('sequencial_resultado'),'tipo':d['tipo_preco'],'decisao':ref['situacao'],
                  'consultado_em':check.get('consultado_em'),'estado':estado(ref),
                  'avisos':colunas_exportacao(ref)['conferencia_avisos']}
            def texto(v):return 'Não informado' if v is None else ('Sim' if v is True else 'Não' if v is False else str(v))
            for diferenca in check.get('divergencias') or [None]:
                linhas.append({**meta,'campo':diferenca['rotulo'] if diferenca else ('Sem divergência local' if meta['estado']=='Sem divergência local' else 'Confira o resultado e os avisos'),
                    'salvo':texto(diferenca['salvo']) if diferenca else None,
                    'atual':texto(diferenca['atual']) if diferenca else None})
        if len(linhas)>1048571:raise ValueError('As divergências excedem o limite de linhas do Excel.')
        arquivos['xl/worksheets/sheet6.xml']=_nova_planilha('Conferência da atualização',colunas,linhas,
            ['Compara cópias salvas com a base local na data de cada conferência. Não é consulta em tempo real ao PNCP.',
             'Sem divergência local não garante atualização externa. Não localizada ou ausente no PNCP não equivale a cancelamento. Campos indisponíveis não são comparados.',
             'Preços, decisões e estatísticas continuam usando a cópia original. Alertas de atualização precisam de revisão; não excluem referências automaticamente.'],
            links_referencias={r['id']:linha for linha,r in enumerate(referencias,6)})
        novas.append((6,'Atualização das referências'))
    for i,nome in novas:
        rid='rIdRADAR_PNCP'+str(i)
        ET.SubElement(sheets,'{'+NS+'}sheet',{'name':nome,'sheetId':str(i),'{'+REL+'}id':rid})
        ET.SubElement(rels,'{'+PKG+'}Relationship',{'Id':rid,'Type':REL+'/worksheet','Target':f'/xl/worksheets/sheet{i}.xml'})
        ET.SubElement(types,'{'+CT+'}Override',{'PartName':f'/xl/worksheets/sheet{i}.xml',
            'ContentType':'application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml'})
    arquivos['xl/workbook.xml']=ET.tostring(workbook,encoding='utf-8',xml_declaration=True)
    arquivos['xl/_rels/workbook.xml.rels']=ET.tostring(rels,encoding='utf-8',xml_declaration=True)
    arquivos['[Content_Types].xml']=ET.tostring(types,encoding='utf-8',xml_declaration=True)
    return resumo
ET.register_namespace('',NS)
ET.register_namespace('r',REL)


def col(n):
    out=''
    while n:n,k=divmod(n-1,26);out=chr(65+k)+out
    return out


def _texto(v):
    s=str(v)
    if len(s)>32767:raise ValueError('Um campo excede o limite de texto do Excel (32.767 caracteres).')
    return ''.join(c for c in s if c in '\t\n\r' or 0x20<=ord(c)<=0xD7FF or 0xE000<=ord(c)<=0xFFFD or 0x10000<=ord(c)<=0x10FFFF)


def _celula(endereco,valor,tipo,style='0'):
    c=ET.Element('{'+NS+'}c',{'r':endereco,'s':style})
    if valor is None:return c
    if tipo=='bool':valor='Sim' if valor else 'Não';tipo='texto'
    if tipo=='data':
        try:
            data=datetime.fromisoformat(str(valor).replace('Z','+00:00')).replace(tzinfo=None)
            valor=(data-datetime(1899,12,30)).total_seconds()/86400;tipo='numero'
        except (ValueError,TypeError):tipo='texto'
    if tipo in ('numero','moeda4','inteiro'):
        try:n=float(valor)
        except (ValueError,TypeError):raise ValueError('Valor numérico inválido na referência: '+str(valor))
        if not math.isfinite(n):raise ValueError('Valor numérico não finito na referência.')
        ET.SubElement(c,'{'+NS+'}v').text=str(int(n)) if tipo=='inteiro' else repr(n)
    else:
        c.set('t','inlineStr')
        t=ET.SubElement(ET.SubElement(c,'{'+NS+'}is'),'{'+NS+'}t')
        t.set('{http://www.w3.org/XML/1998/namespace}space','preserve')
        t.text=_texto(valor)
    return c


def _substituir(sheet,endereco,valor,tipo='texto'):
    sd=sheet.find('{'+NS+'}sheetData');num=re.search(r'\d+$',endereco).group()
    row=sd.find("{%s}row[@r='%s']"%(NS,num))
    if row is None:row=ET.SubElement(sd,'{'+NS+'}row',{'r':num})
    old=row.find("{%s}c[@r='%s']"%(NS,endereco));style=old.get('s','0') if old is not None else '0'
    cell=_celula(endereco,valor,tipo,style)
    if old is not None:
        # O Excel exige células em ordem de coluna. Substituir A2 no fim
        # da linha do modelo gerava B2...AN2,A2 e um aviso de recuperação.
        index=list(row).index(old)
        row.remove(old)
        row.insert(index,cell)
    else:
        def coluna(c):
            numero=0
            for letra in re.match(r'[A-Z]+',c.get('r')).group():
                numero=numero*26+ord(letra)-ord('A')+1
            return numero
        destino=coluna(cell)
        index=next((i for i,c in enumerate(row) if coluna(c)>destino),len(row))
        row.insert(index,cell)


def gerar_xlsx(pesquisa,referencias):
    if not referencias:raise ValueError('A cesta está vazia.')
    if len(referencias)>1048571:raise ValueError('A cesta excede o limite de linhas do Excel.')
    template=Path(__file__).parent/'modelos'/'cesta.xlsx'
    with zipfile.ZipFile(template) as z:arquivos={i.filename:z.read(i) for i in z.infolist()}
    sheet=ET.fromstring(arquivos['xl/worksheets/sheet1.xml'])
    sd=sheet.find('{'+NS+'}sheetData')
    modelo=sd.find("{%s}row[@r='6']"%NS)
    estilos={re.sub(r'\d+','',c.get('r')):c.get('s','0') for c in modelo}
    larguras={}
    for item in sheet.findall('{'+NS+'}cols/{'+NS+'}col'):
        for j in range(int(item.get('min')),int(item.get('max'))+1):larguras[j]=float(item.get('width','22'))
    for row in list(sd):
        if int(row.get('r'))>=6:sd.remove(row)
    for j,(label,_,_) in enumerate(COLUNAS,1):_substituir(sheet,col(j)+'5',label)
    for cell in sd.find("{%s}row[@r='5']"%NS):cell.set('s','6')
    # Colunas novas recebem o estilo e a largura apropriados, mantendo as originais.
    widths=sheet.find('{'+NS+'}cols')
    for j in range(41,len(COLUNAS)+1):
        ET.SubElement(widths,'{'+NS+'}col',{'min':str(j),'max':str(j),'width':str(22 if j<43 else 38),'customWidth':'1'})
    _substituir(sheet,'A2',f"Referências — {pesquisa['nome']}")
    links=ET.Element('{'+NS+'}hyperlinks');rels=ET.Element('Relationships',{'xmlns':PKG})
    for linha,ref in enumerate(referencias,6):
        row=ET.SubElement(sd,'{'+NS+'}row',{'r':str(linha),'ht':'48','customHeight':'1'})
        dados={**ref['dados'],**{k:v for k,v in ref.items() if k!='dados'}}
        from conferencia_cesta import colunas_exportacao
        dados.update(colunas_exportacao(ref))
        linhas_texto=1
        for j,(_,campo,tipo) in enumerate(COLUNAS,1):
            letra=col(j);endereco=letra+str(linha);valor=dados.get(campo)
            padrao='17' if tipo=='moeda4' else '12' if tipo=='inteiro' else '9'
            row.append(_celula(endereco,valor,tipo,estilos.get(letra,padrao)))
            if valor is not None and tipo in ('texto','link'):
                largura=max(8,int(larguras.get(j,22)*.9))
                linhas_texto=max(linhas_texto,sum(max(1,math.ceil(len(t)/largura)) for t in str(valor).split('\n')))
            if tipo=='link' and str(valor or '').startswith(('https://','http://')):
                rid='rId'+str(len(rels)+1)
                ET.SubElement(links,'{'+NS+'}hyperlink',{'ref':endereco,'{'+REL+'}id':rid})
                ET.SubElement(rels,'Relationship',{'Id':rid,'Type':REL+'/hyperlink','Target':_texto(valor),'TargetMode':'External'})
        row.set('ht',str(min(409,max(48,linhas_texto*14+6))))
    ultima=len(referencias)+5;last=col(len(COLUNAS))
    dimension=sheet.find('{'+NS+'}dimension')
    if dimension is not None:dimension.set('ref',f'A1:{last}{ultima}')
    filtro=sheet.find('{'+NS+'}autoFilter')
    if filtro is None:
        filtro=ET.Element('{'+NS+'}autoFilter');sheet.insert(list(sheet).index(sd)+1,filtro)
    filtro.set('ref',f'A5:{last}{ultima}')
    # O modelo não contém links, tabelas ou fórmulas dinâmicas a recalcular.
    if len(links):
        sheet.insert(list(sheet).index(filtro)+1,links)
        arquivos['xl/worksheets/_rels/sheet1.xml.rels']=ET.tostring(rels,encoding='utf-8',xml_declaration=True)
    arquivos['xl/worksheets/sheet1.xml']=ET.tostring(sheet,encoding='utf-8',xml_declaration=True)
    meta=ET.fromstring(arquivos['xl/worksheets/sheet2.xml'])
    valores=[pesquisa['nome'],pesquisa.get('objetivo',''),pesquisa['id'],pesquisa['criado_em'],
        pesquisa['atualizado_em'],datetime.now().astimezone().isoformat(timespec='seconds'),len(referencias),
        'Todas as referências, inclusive as excluídas e ainda não avaliadas.',
        'Os dados representam a cópia salva na seleção; não há atualização automática dos preços desta planilha.',
        'Valores homologados e estimados estão identificados. Total informado pela fonte, sem recálculo.',
        'Campo vazio indica informação ausente; zero significa valor zero informado.',
        'Preços em reais (R$), conforme os campos locais de resultados e estimativas. Sem conversão monetária.',
        'Datas com fuso horário são preservadas como texto nos registros de consulta, importação e seleção.']
    for linha,valor in enumerate(valores,4):_substituir(meta,'B'+str(linha),valor)
    objetivo_linhas=sum(max(1,math.ceil(len(t)/95)) for t in str(pesquisa.get('objetivo','')).split('\n'))
    meta.find("{%s}sheetData/{%s}row[@r='5']"%(NS,NS)).set('ht',str(min(409,max(84,objetivo_linhas*16+6))))
    meta_sd=meta.find('{'+NS+'}sheetData')
    for linha,regra in enumerate(REGRAS,17):
        row=ET.SubElement(meta_sd,'{'+NS+'}row',{'r':str(linha),'ht':'44','customHeight':'1'})
        row.append(_celula('A'+str(linha),'Critério do resumo '+str(linha-16),'texto','9'))
        row.append(_celula('B'+str(linha),regra,'texto','9'))
    dimension=meta.find('{'+NS+'}dimension')
    if dimension is not None:dimension.set('ref',f'A1:B{16+len(REGRAS)}')
    arquivos['xl/worksheets/sheet2.xml']=ET.tostring(meta,encoding='utf-8',xml_declaration=True)
    _adicionar_resumo(arquivos,referencias,pesquisa)
    saida=BytesIO()
    with zipfile.ZipFile(saida,'w',zipfile.ZIP_DEFLATED) as z:
        for nome,dados in arquivos.items():z.writestr(nome,dados)
    return saida.getvalue()
