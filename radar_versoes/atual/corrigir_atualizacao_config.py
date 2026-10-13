"""Destrava a validação da configuração preservada nas versões até 0.7.21.

Não substitui a configuração nem altera código ou bancos da instalação.
Registra uma cópia antes de ajustar apenas seu hash no manifesto instalado.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sys
import uuid
import zipfile

# O Python portátil pode ignorar a pasta do script no sys.path.
sys.path.insert(0,str(Path(__file__).resolve().parent))
from atualizar_radar_pncp import exclusividade, gravar, validar_pacote
from pacotes_radar_pncp import inspecionar, versao
from radar_pncp_layout import raiz_da_instalacao


def corrigir(root, source):
    root=Path(root).resolve();source=Path(source).resolve()
    base=root/'radar_versoes'/'atual'
    versions=root/'radar_versoes'
    with exclusividade(versions/'solicitacao.lock'), exclusividade(root/'radar_dados'/'radar_pncp_atualizador.lock'):
        journal=root/'radar_dados'/'radar_pncp_atualizacao.json'
        if journal.exists() and json.loads(journal.read_text(encoding='utf-8')).get('fase') not in ('concluido','revertido'):
            raise RuntimeError('Existe uma atualização pendente. Abra iniciar.bat para concluir a recuperação antes desta correção.')
        pedido=versions/'solicitacao.json'
        if pedido.exists() and json.loads(pedido.read_text(encoding='utf-8')).get('status')=='iniciado':
            raise RuntimeError('Existe uma solicitação de atualização em andamento. Aguarde ou abra iniciar.bat para recuperar a tentativa anterior.')
        novo=validar_pacote(source)
        atual=validar_pacote(base,instalado=True)
        if versao(novo['version'])<=versao(atual['version']):
            print('A versão instalada já é igual ou posterior a este pacote. Nenhuma alteração realizada.')
            return None
        name='.streamlit/config.toml'
        if name not in atual['files']:
            raise ValueError('A configuração não está declarada na instalação. Nenhuma alteração realizada.')
        digest=hashlib.sha256((base/name).read_bytes()).hexdigest()
        # Prepara um ZIP normal, conferido, para a atualização pela interface.
        # A pasta extraída nunca vira a versão atual nem executa as coletas.
        pacotes=versions/'pacotes';pacotes.mkdir(exist_ok=True)
        destino=pacotes/('RADAR_PNCP_Atualizacao_'+novo['version']+'.zip')
        token=uuid.uuid4().hex
        temp=pacotes/('correcao_'+token+'.tmp')
        try:
            with zipfile.ZipFile(temp,'w',zipfile.ZIP_DEFLATED) as z:
                prefix='radar_compras_v'+novo['version'].replace('.','_')+'/'
                for n in (*novo['files'],'release.json'):z.write(source/n,prefix+n)
            info=inspecionar(temp,conferir_hashes=True)
            if destino.exists():
                if inspecionar(destino,conferir_hashes=True)['identity']!=info['identity']:
                    raise ValueError('Já existe outro pacote com esta versão. Confira o arquivo em radar_versoes/pacotes antes de continuar.')
            else:temp.replace(destino)
        finally:temp.unlink(missing_ok=True)
        if atual['files'][name]!=digest:
            backup=versions/'operacoes'/('correcao_config_'+token)
            backup.mkdir(parents=True,exist_ok=False)
            shutil.copy2(base/'release.json',backup/'release.json')
            shutil.copy2(base/name,backup/'config.toml')
            atual['files'][name]=digest
            gravar(base/'release.json',atual)
            print('Correção aplicada. Configuração preservada; cópia anterior em '+str(backup))
        else:print('A configuração instalada já passou na conferência.')
        print('Pacote '+novo['version']+' pronto em '+str(destino))
        print('No Radar PNCP: Sistema > Configurações > Atualizar sistema > Verificar pacotes > Atualizar sistema.')
        return destino


def main():
    source=Path(__file__).resolve().parent
    try:
        root=raiz_da_instalacao(source,obrigatoria=True)
        corrigir(root,source)
        return 0
    except Exception as exc:
        print('Não foi possível corrigir: '+str(exc))
        return 1


if __name__=='__main__':raise SystemExit(main())
