"""Minuta DOCX a partir da cesta, sem dependências adicionais no Python portátil.

WordprocessingML: https://learn.microsoft.com/en-us/office/open-xml/word/structure-of-a-wordprocessingml-document
O modelo contém estilos/cabeçalho/rodapé. Fontes e textos entram como texto literal.
"""
from collections import Counter
from datetime import datetime,timezone
from decimal import Decimal,InvalidOperation,localcontext
from io import BytesIO
from pathlib import Path
import copy,zipfile
from urllib.parse import urlsplit
import xml.etree.ElementTree as ET
from xml.dom import minidom

from resumo_cesta import consolidar,REGRAS
from conferencia_cesta import estado,SEM
from relatorios_pesquisa import vazio
from interface_radar_pncp import VERSION

W='http://schemas.openxmlformats.org/wordprocessingml/2006/main'
R='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
PKG='http://schemas.openxmlformats.org/package/2006/relationships'
XML='http://www.w3.org/XML/1998/namespace'
CP='http://schemas.openxmlformats.org/package/2006/metadata/core-properties'
DC='http://purl.org/dc/elements/1.1/'
DT='http://purl.org/dc/terms/'
XSI='http://www.w3.org/2001/XMLSchema-instance'
for prefix,ns in [('w',W),('r',R),('cp',CP),('dc',DC),('dcterms',DT),('xsi',XSI)]:ET.register_namespace(prefix,ns)
MAX_REFERENCIAS=5000


def texto(v):
    s=str(v)
    if len(s)>100000:raise ValueError('Um campo excede 100.000 caracteres. Revise o conteúdo antes de gerar o relatório.')
    return ''.join(c for c in s if c in '\t\n\r' or 0x20<=ord(c)<=0xD7FF or 0xE000<=ord(c)<=0xFFFD or 0x10000<=ord(c)<=0x10FFFF)


def exibir(v):
    if v is None or v=='':return 'Não informado'
    if isinstance(v,bool):return 'Sim' if v else 'Não'
    return texto(v)


def numero(v,*,moeda=False,estatistica=False):
    if v is None or isinstance(v,bool):return 'Não informado'
    try:
        d=Decimal(str(v))
        if not d.is_finite():return 'Valor inválido'
        if abs(d.adjusted())>308 or -d.as_tuple().exponent>400 or len(d.as_tuple().digits)>400:return 'Valor inválido'
        with localcontext() as ctx:
            ctx.prec=max(50,len(d.as_tuple().digits)+abs(d.adjusted())+20)
            if estatistica:s=f'{d:,.4f}'
            else:
                casas=max(4,-d.as_tuple().exponent) if moeda else max(0,-d.as_tuple().exponent)
                s=f'{d:,.{casas}f}'
        s=s.replace(',','X').replace('.',',').replace('X','.')
        return ('R$ ' if moeda else '')+s
    except (InvalidOperation,ValueError,TypeError,OverflowError):return 'Valor inválido'


def serie(ref):
    d=ref['dados'];ident=('item',ref['item_pesquisa_id']) if ref.get('item_pesquisa_id') else ('livre',' '.join(str(ref.get('grupo') or '').split()).casefold())
    return (*ident,d.get('tipo_preco'),' '.join(str(d.get('unidade_medida') or '').split()).casefold())


def pendencias(pesquisa,refs):
    resumo=consolidar(refs,incluir_conferencia=True)
    dados=(pesquisa.get('relatorio') or vazio())['dados'];out=[]
    faltam=[label for k,label in [('orgao','Órgão/setor'),('responsavel','Responsável'),('processo','Processo/identificação'),
        ('metodo','Método da pesquisa'),('analise','Análise dos resultados'),('encaminhamento','Conclusão/encaminhamento')] if not str(dados.get(k) or '').strip()]
    if not str(pesquisa.get('objetivo') or '').strip():faltam.insert(0,'Objeto/objetivo da pesquisa')
    if faltam:out.append('Campos não preenchidos: '+', '.join(faltam)+'.')
    n=sum(r.get('situacao')=='A avaliar' for r in refs)
    if n:out.append(f'{n} referência(s) permanece(m) A avaliar.')
    if resumo['fora_do_resumo']:out.append(f"{resumo['fora_do_resumo']} referência(s) marcada(s) como Incluir não participa(m) dos cálculos. Os motivos constam das fichas.")
    if not resumo['grupos']:out.append('Ainda não há série apta para calcular o resumo de preços.')
    catalogo=pesquisa.get('itens',[]);utilizados=Counter(r.get('item_pesquisa_id') for r,a in zip(refs,resumo['conferencia']) if a['participa'])
    sem=[i for i in catalogo if not utilizados[i['id']]]
    if sem:out.append('Itens da pesquisa sem referências utilizadas nos cálculos: '+', '.join(f"{i['numero']} — {i['nome']}" for i in sem)+'.')
    semgrupo=sum(not str(r.get('grupo') or '').strip() for r in refs)
    if semgrupo:out.append(f'{semgrupo} referência(s) sem grupo ou item da pesquisa para comparação.')
    nao=sum(not r.get('conferencia') for r in refs)
    if nao:out.append(f'{nao} referência(s) sem conferência registrada com a base local.')
    alerta=sum(bool(r.get('conferencia')) and estado(r)!=SEM for r in refs)
    if alerta:out.append(f'{alerta} referência(s) com alerta na última conferência local. Consulte as fichas, datas e fontes antes de concluir.')
    return out


class Documento:
    def __init__(self):
        template=Path(__file__).parent/'modelos'/'relatorio.docx'
        with zipfile.ZipFile(template) as z:self.arquivos={i.filename:z.read(i) for i in z.infolist()}
        anterior=ET.fromstring(self.arquivos['word/document.xml'])
        self.secao=copy.deepcopy(anterior.find('{'+W+'}body/{'+W+'}sectPr'))
        self.root=ET.Element('{'+W+'}document');self.body=ET.SubElement(self.root,'{'+W+'}body')
        self.rels=ET.fromstring(self.arquivos['word/_rels/document.xml.rels'])
        self.rids={r.get('Id') for r in self.rels};self.contador=0

    def run(self,p,conteudo,*,bold=False):
        r=ET.SubElement(p,'{'+W+'}r')
        if bold:ET.SubElement(ET.SubElement(r,'{'+W+'}rPr'),'{'+W+'}b')
        linhas=texto(conteudo).replace('\r\n','\n').replace('\r','\n').split('\n')
        for n,linha in enumerate(linhas):
            if n:ET.SubElement(r,'{'+W+'}br')
            partes=linha.split('\t')
            for j,parte in enumerate(partes):
                if j:ET.SubElement(r,'{'+W+'}tab')
                t=ET.SubElement(r,'{'+W+'}t',{'{'+XML+'}space':'preserve'});t.text=parte
        return r

    def p(self,conteudo='',style=None,parent=None):
        p=ET.SubElement(self.body if parent is None else parent,'{'+W+'}p')
        if style:ET.SubElement(ET.SubElement(p,'{'+W+'}pPr'),'{'+W+'}pStyle',{'{'+W+'}val':style})
        self.run(p,conteudo);return p

    def campo(self,label,v,*,vazio_texto='Não informado'):
        p=self.p();self.run(p,label+': ',bold=True);self.run(p,exibir(v) if v not in (None,'') else vazio_texto)

    def bloco(self,conteudo,vazio_texto):
        # Parágrafos longos podem quebrar naturalmente; linhas em branco do usuário são mantidas.
        if not str(conteudo or '').strip():self.p(vazio_texto,'Caption');return
        for trecho in str(conteudo).replace('\r\n','\n').replace('\r','\n').split('\n'):self.p(trecho)

    def tabela(self,linhas,cabecalho=('Campo','Registro'),larguras=(3200,6200)):
        tbl=ET.SubElement(self.body,'{'+W+'}tbl');pr=ET.SubElement(tbl,'{'+W+'}tblPr')
        ET.SubElement(pr,'{'+W+'}tblW',{'{'+W+'}w':str(sum(larguras)),'{'+W+'}type':'dxa'})
        ET.SubElement(pr,'{'+W+'}tblLayout',{'{'+W+'}type':'fixed'})
        borders=ET.SubElement(pr,'{'+W+'}tblBorders')
        for edge in ('top','left','bottom','right','insideH','insideV'):
            ET.SubElement(borders,'{'+W+'}'+edge,{'{'+W+'}val':'single','{'+W+'}sz':'4','{'+W+'}color':'D9E1EA'})
        grid=ET.SubElement(tbl,'{'+W+'}tblGrid')
        for largura in larguras:ET.SubElement(grid,'{'+W+'}gridCol',{'{'+W+'}w':str(largura)})
        for i,linha in enumerate([cabecalho,*linhas]):
            row=ET.SubElement(tbl,'{'+W+'}tr')
            if i==0:ET.SubElement(ET.SubElement(row,'{'+W+'}trPr'),'{'+W+'}tblHeader')
            for j,valor in enumerate(linha):
                cell=ET.SubElement(row,'{'+W+'}tc');cp=ET.SubElement(cell,'{'+W+'}tcPr')
                ET.SubElement(cp,'{'+W+'}tcW',{'{'+W+'}w':str(larguras[j]),'{'+W+'}type':'dxa'})
                if i==0:ET.SubElement(cp,'{'+W+'}shd',{'{'+W+'}fill':'EAF0F6'})
                p=ET.SubElement(cell,'{'+W+'}p');self.run(p,valor,bold=i==0)
        self.p()

    def link(self,label,url):
        if not url:self.campo(label,None);return
        url=texto(url);p=self.p();self.run(p,label+': ',bold=True)
        try:parts=urlsplit(url);permitido=parts.scheme.lower() in ('http','https') and bool(parts.netloc)
        except ValueError:permitido=False
        if not permitido:self.run(p,url);return
        self.contador+=1;rid=f'rIdRADAR_PNCPReport{self.contador}'
        while rid in self.rids:self.contador+=1;rid=f'rIdRADAR_PNCPReport{self.contador}'
        self.rids.add(rid);ET.SubElement(self.rels,'{'+PKG+'}Relationship',{'Id':rid,'Type':R+'/hyperlink','Target':url,'TargetMode':'External'})
        h=ET.SubElement(p,'{'+W+'}hyperlink',{'{'+R+'}id':rid})
        r=self.run(h,url);pr=ET.Element('{'+W+'}rPr');ET.SubElement(pr,'{'+W+'}color',{'{'+W+'}val':'0563C1'});ET.SubElement(pr,'{'+W+'}u',{'{'+W+'}val':'single'});r.insert(0,pr)

    def ancora(self,rid,conteudo):
        p=self.p(conteudo,'Heading2')
        p.insert(1,ET.Element('{'+W+'}bookmarkStart',{'{'+W+'}id':str(rid),'{'+W+'}name':'Ref'+str(rid)}))
        p.append(ET.Element('{'+W+'}bookmarkEnd',{'{'+W+'}id':str(rid)}))

    def referencias(self,ids):
        p=self.p();self.run(p,'Referências utilizadas: ',bold=True)
        for j,rid in enumerate(ids):
            if j:self.run(p,', ')
            h=ET.SubElement(p,'{'+W+'}hyperlink',{'{'+W+'}anchor':'Ref'+str(rid)})
            r=self.run(h,'#'+str(rid));pr=ET.Element('{'+W+'}rPr');ET.SubElement(pr,'{'+W+'}color',{'{'+W+'}val':'0563C1'});r.insert(0,pr)

    def bytes(self,pesquisa,stamp):
        self.body.append(self.secao)
        self.arquivos['word/document.xml']=ET.tostring(self.root,encoding='utf-8',xml_declaration=True)
        # Alguns leitores exigem o namespace padrão nas partes de relações.
        # Serialize sem depender do registro global de prefixos do exportador Excel.
        rels=minidom.Document();raiz=rels.createElement('Relationships')
        raiz.setAttribute('xmlns',PKG);rels.appendChild(raiz)
        for item in self.rels:
            elemento=rels.createElement('Relationship')
            for nome,valor in item.attrib.items():elemento.setAttribute(nome,valor)
            raiz.appendChild(elemento)
        self.arquivos['word/_rels/document.xml.rels']=rels.toxml(encoding='utf-8')
        core=ET.fromstring(self.arquivos['docProps/core.xml'])
        dados=(pesquisa.get('relatorio') or vazio())['dados']
        for ns,name,value in [(DC,'title','Relatório da pesquisa — '+pesquisa['nome']),
            (DC,'creator',dados.get('responsavel') or 'Radar PNCP'),(CP,'revision',str(pesquisa['revisao']))]:
            el=core.find('{'+ns+'}'+name)
            if el is None:el=ET.SubElement(core,'{'+ns+'}'+name)
            el.text=texto(value)
        dt=datetime.fromisoformat(stamp).astimezone(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
        for name in ('created','modified'):
            el=core.find('{'+DT+'}'+name)
            if el is None:el=ET.SubElement(core,'{'+DT+'}'+name)
            el.set('{'+XSI+'}type','dcterms:W3CDTF');el.text=dt
        self.arquivos['docProps/core.xml']=ET.tostring(core,encoding='utf-8',xml_declaration=True)
        saida=BytesIO()
        with zipfile.ZipFile(saida,'w',zipfile.ZIP_DEFLATED) as z:
            for nome,conteudo in self.arquivos.items():z.writestr(nome,conteudo)
        return saida.getvalue()


def gerar_docx(pesquisa,referencias,*,gerado_em=None):
    if not referencias:raise ValueError('Adicione referências à cesta antes de gerar o relatório.')
    if len(referencias)>MAX_REFERENCIAS:raise ValueError('O relatório admite até 5.000 referências por cesta. Organize a pesquisa em cestas menores para o documento; o Excel continua disponível para listas maiores.')
    ids=[r['id'] for r in referencias]
    if any(type(i) is not int or i<=0 for i in ids) or len(set(ids))!=len(ids):raise ValueError('Identificações das referências inválidas ou duplicadas.')
    stamp=gerado_em or datetime.now().astimezone().isoformat(timespec='microseconds')
    resumo=consolidar(referencias,incluir_conferencia=True);auditoria={r['id']:r for r in resumo['conferencia']}
    metadados=pesquisa.get('relatorio') or vazio();dados=metadados['dados'];doc=Documento()
    doc.p('Relatório da pesquisa de preços','Title');doc.p('Minuta para revisão','Subtitle')
    doc.p(pesquisa['nome'],'Heading1')
    doc.p('Documento editável gerado a partir da cesta. Cabe ao responsável conferir a comparabilidade, completar a análise e definir o encaminhamento. Alterações feitas no Word não retornam ao Radar PNCP.','Caption')
    for label,k in [('Órgão / setor','orgao'),('Responsável','responsavel'),('Processo / identificação','processo')]:doc.campo(label,dados.get(k),vazio_texto='Não preenchido — completar na revisão')
    doc.campo('Gerado em',datetime.fromisoformat(stamp).strftime('%d/%m/%Y às %H:%M:%S %z'))
    doc.campo('Registro da cesta',f"Pesquisa #{pesquisa['id']} · revisão {pesquisa['revisao']} · Radar PNCP {VERSION}")
    doc.p('1. Objeto e contexto','Heading1');doc.bloco(pesquisa.get('objetivo'),'Objeto/objetivo não preenchido na cesta — completar na revisão.')
    doc.bloco(dados.get('contexto'),'Contexto e finalidade não informados pelo responsável.')
    doc.p('2. Itens e critérios de comparabilidade','Heading1')
    from itens_cesta import painel
    catalogo=pesquisa.get('itens',[])
    if catalogo:
        for item in painel(catalogo,referencias):
            doc.p(f"Item {item['numero']} — {item['nome']}",'Heading2')
            doc.campo('Critérios cadastrados',item['descricao']);doc.campo('Unidade pretendida',item['unidade'])
            doc.p(f"Referências vinculadas: {item['referencias']} · Incluir: {item['incluidas']} · Excluir: {item['excluidas']} · A avaliar: {item['a_avaliar']}.")
    else:doc.p('A cesta usa grupos livres; não há itens estruturados cadastrados. Os critérios devem ser explicitados pelo responsável.')
    livres=sorted({str(r.get('grupo') or '').strip() for r in referencias if not r.get('item_pesquisa_id') and str(r.get('grupo') or '').strip()},key=str.casefold)
    if livres:doc.p('Grupos livres presentes: '+'; '.join(livres)+'.')
    doc.p('Unidade pretendida e descrição orientam a comparação. Não convertem automaticamente unidade, moeda, período ou quantidade; a unidade de cada referência permanece preservada.','Caption')
    doc.p('3. Procedimento e critérios dos cálculos','Heading1');doc.bloco(dados.get('metodo'),'Método e notas de pesquisa não preenchidos — registrar fontes consultadas, período e critérios na revisão.')
    doc.p('As informações abaixo descrevem as regras de cálculo do Radar PNCP; não substituem o registro do método de pesquisa aplicado pelo responsável.')
    for regra in REGRAS:doc.p('• '+regra)
    doc.p('Os cálculos usam as cópias salvas na cesta. Conferências locais não substituem seus preços. A coleta pode ter lacunas; datas de seleção e conferência não garantem atualização ou completude do PNCP.')
    doc.p('4. Situação das referências e pontos para revisão','Heading1')
    cont=Counter(r.get('situacao') for r in referencias)
    doc.tabela([('Referências na cesta',str(len(referencias))),('Incluir',str(cont['Incluir'])),('Excluir',str(cont['Excluir'])),
        ('A avaliar',str(cont['A avaliar'])),('Utilizadas nos cálculos',str(resumo['utilizadas'])),
        ('Incluir fora dos cálculos',str(resumo['fora_do_resumo']))])
    avisos=pendencias(pesquisa,referencias)
    for aviso in avisos:doc.p('• '+aviso)
    if not avisos:doc.p('Não foram identificadas pendências nos campos e regras automáticos deste documento. Isso não atesta comparabilidade, adequação da pesquisa nem atualização completa das fontes.')
    doc.p('5. Resumo de preços por série','Heading1')
    utilizadas={}
    for ref in referencias:
        if auditoria[ref['id']]['participa']:utilizadas.setdefault(serie(ref),[]).append(ref['id'])
    if not resumo['grupos']:doc.p('Nenhuma série apta para cálculo. Complete a avaliação dos preços, grupos e unidades e gere novamente.')
    for grupo in resumo['grupos']:
        identidade=('item',grupo['item_pesquisa_id']) if grupo['item_pesquisa_id'] else ('livre',grupo['grupo'].casefold())
        chave=(*identidade,grupo['tipo_preco'],grupo['unidade'].casefold())
        titulo=grupo['grupo']+(' · item estruturado' if grupo['item_pesquisa_id'] else ' · grupo livre')
        doc.p(titulo,'Heading2');doc.p(f"Tipo de preço: {grupo['tipo_preco']} · Unidade: {grupo['unidade']} · Referências: {grupo['referencias']} · Contratações: {grupo['contratacoes']} · Fornecedores identificados: {grupo['fornecedores']}.")
        linhas=[(label,numero(grupo[k],moeda=True,estatistica=True) if grupo[k] is not None else 'Não calculado (exige pelo menos duas referências)')
            for label,k in [('Média','media'),('Mediana','mediana'),('Mínimo','minimo'),('Máximo','maximo'),('Desvio padrão amostral','desvio_padrao')]]
        cv=grupo['cv_percentual'];linhas.append(('Coeficiente de variação',f'{cv:.2f}%'.replace('.',',') if cv is not None else 'Não calculado (exige pelo menos duas referências)'))
        doc.tabela(linhas);doc.referencias(utilizadas.get(chave,[]))
    doc.p('Estatísticas monetárias apresentadas com quatro casas decimais; coeficiente de variação, com duas. Cálculos não arredondam previamente os preços. As fichas preservam a precisão decimal disponível de cada preço unitário.','Caption')
    doc.p('6. Análise dos resultados','Heading1');doc.bloco(dados.get('analise'),'Análise não preenchida — avaliar comparabilidade, diferenças de preços, condições e limites das referências.')
    doc.p('7. Conclusão / encaminhamento','Heading1');doc.bloco(dados.get('encaminhamento'),'Conclusão/encaminhamento não preenchido — completar pelo responsável. O sistema não define automaticamente o preço final.')
    doc.p('8. Fichas das referências e fontes','Heading1')
    doc.p('Inclui todas as referências da cesta, inclusive excluídas e ainda A avaliar. IDs correspondem à aba Referências do Excel. Os links no resumo levam às fichas abaixo. Preços e dados de origem refletem a cópia salva, não uma consulta em tempo real.')
    for ref in referencias:
        d=ref['dados'];a=auditoria[ref['id']]
        doc.ancora(ref['id'],f"Referência #{ref['id']} — {ref['situacao']}")
        doc.campo('Item / grupo da pesquisa',ref.get('grupo'))
        if ref.get('item_pesquisa_id'):doc.campo('Item estruturado',f"{ref.get('item_pesquisa_numero')} — {ref.get('item_pesquisa_nome')}")
        doc.campo('Participação nos cálculos',a['motivo']);doc.campo('Descrição salva',d.get('descricao'));doc.campo('Informação complementar',d.get('informacao_complementar'))
        doc.tabela([('Tipo de preço',exibir(d.get('tipo_preco'))),('Unidade',exibir(d.get('unidade_medida'))),
            ('Preço unitário salvo',numero(d.get('valor_unitario'),moeda=True)),('Quantidade informada',numero(d.get('quantidade'))),
            ('Valor total informado pela fonte',numero(d.get('valor_total'),moeda=True)),
            ('Órgão / entidade',exibir(d.get('orgao_razao_social'))),('CNPJ do órgão',exibir(d.get('orgao_cnpj'))),
            ('Unidade compradora / UF',exibir(d.get('unidade_nome'))+' / '+exibir(d.get('uf_sigla'))),
            ('Fornecedor',exibir(d.get('nome_fornecedor'))),('CNPJ / identificação do fornecedor',exibir(d.get('ni_fornecedor'))),
            ('Processo de origem',exibir(d.get('processo'))),('Compra / ano',exibir(d.get('numero_compra'))+' / '+exibir(d.get('ano_compra'))),
            ('Controle PNCP',exibir(d.get('numero_controle_pncp'))),('Item da contratação',exibir(d.get('numero_item'))),
            ('Sequencial do resultado',exibir(d.get('sequencial_resultado'))),('Data do resultado / preço',exibir(d.get('data_preco'))),
            ('Data de publicação',exibir(d.get('data_publicacao_pncp'))),('Fonte do preço',exibir(d.get('fonte_ultima'))),
            ('Importado / coletado em',exibir(d.get('importado_em'))),('Atualizado na fonte',exibir(d.get('data_atualizacao_fonte'))),
            ('Salva na cesta em',exibir(ref.get('selecionado_em'))),('Consulta local da seleção em',exibir(d.get('consultado_em'))),
            ('Situação na fonte na seleção',exibir(d.get('situacao_resultado_nome'))),('Cancelado na seleção',exibir(d.get('cancelado'))),
            ('Data / motivo do cancelamento',exibir(d.get('data_cancelamento_pncp'))+' / '+exibir(d.get('motivo_cancelamento')))])
        doc.campo('Justificativa da decisão',ref.get('justificativa'));doc.campo('Observações de comparabilidade',ref.get('observacoes'))
        doc.campo('Avaliação salva em',ref.get('revisado_em'))
        doc.link('Fonte / PNCP',d.get('url_pncp'));doc.link('Sistema de origem',d.get('link_sistema_origem'))
        doc.campo('Última conferência local',estado(ref));check=ref.get('conferencia')
        if check:
            atual=check.get('atual') or {}
            doc.campo('Conferência realizada em',check['consultado_em']);doc.campo('Preço unitário local na conferência (não usado nos cálculos)',numero(atual.get('valor_unitario'),moeda=True))
            doc.campo('Fonte / importação do preço local',exibir(atual.get('fonte_ultima'))+' / '+exibir(atual.get('importado_em')))
            doc.campo('Atualização na fonte do registro local',atual.get('data_atualizacao_fonte'));doc.campo('Consulta PNCP registrada',check.get('pncp_consultado_em'))
            for aviso in check.get('avisos',[]):doc.p(aviso)
            diffs=check.get('divergencias',[])
            if diffs:doc.tabela([(diff['rotulo'],'Salvo: '+exibir(diff['salvo'])+'\nLocal na conferência: '+exibir(diff['atual'])) for diff in diffs])
    doc.p('Dados deste documento correspondem à cesta e às avaliações salvas no momento da geração. Para incorporar mudanças feitas no Radar PNCP, gere novamente. O Word e o Excel baixados permanecem cópias daquele momento.','Caption')
    return doc.bytes(pesquisa,stamp)
