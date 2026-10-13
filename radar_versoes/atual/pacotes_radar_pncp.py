"""Inspeção e extração limitada dos pacotes locais de atualização."""
import hashlib
import json
import re
import stat
import zipfile
from pathlib import Path, PurePosixPath

MAX_BYTES=200*1024*1024
REQUIRED={'app.py','job_manager.py','radar_paths.py','iniciar_interface.bat'}


def versao(valor):
    if not isinstance(valor,str) or not re.fullmatch(r'\d+\.\d+\.\d+(?:\.\d+)?',valor):
        raise ValueError('Número de versão inválido.')
    return tuple(int(n) for n in valor.split('.'))+(0,)*(4-len(valor.split('.')))


def caminho_seguro(name):
    p=PurePosixPath(name)
    reserved={'CON','PRN','AUX','NUL',*[f'COM{i}' for i in range(1,10)],*[f'LPT{i}' for i in range(1,10)]}
    if not name or '\\' in name or ':' in name or p.is_absolute() or any(
        x in ('..','.') or x.endswith((' ','.')) or x.split('.')[0].upper() in reserved for x in p.parts):
        raise ValueError('Caminho inválido no pacote: '+name)
    if str(p)!=name:raise ValueError('Caminho não canônico no pacote: '+name)
    return p


def validar_manifesto(m):
    versao(m.get('version'))
    if m.get('layout')!=2 or m.get('update_protocol')!=1:
        raise ValueError('Pacote incompatível com atualização pela interface. Requer formato de instalação V0.7.6 ou posterior.')
    files=m.get('files')
    if not isinstance(files,dict) or not REQUIRED<=set(files):raise ValueError('Pacote incompleto.')
    seen=set()
    for name,digest in files.items():
        p=caminho_seguro(name)
        if p.suffix.lower() in ('.db','.sqlite','.sqlite3') or 'radar_dados' in p.parts:
            raise ValueError('O pacote não pode conter bancos ou radar_dados.')
        if name.casefold() in seen:raise ValueError('Nomes de arquivo conflitantes no Windows.')
        seen.add(name.casefold())
        if not isinstance(digest,str) or not re.fullmatch('[0-9a-f]{64}',digest):raise ValueError('Hash inválido: '+name)
    return m


def inspecionar(path, *, conferir_hashes=False):
    path=Path(path)
    with zipfile.ZipFile(path) as z:
        infos=[i for i in z.infolist() if not i.is_dir()]
        if len(infos)>5000 or sum(i.file_size for i in infos)>MAX_BYTES:raise ValueError('Pacote excede o tamanho permitido.')
        seen=set()
        for i in infos:
            caminho_seguro(i.filename)
            if i.filename.casefold() in seen:raise ValueError('Arquivos duplicados no ZIP.')
            seen.add(i.filename.casefold())
            if stat.S_ISLNK(i.external_attr>>16):raise ValueError('Links simbólicos não são aceitos no ZIP.')
        manifests=[i for i in infos if PurePosixPath(i.filename).name=='release.json']
        if len(manifests)!=1 or manifests[0].file_size>1024*1024:raise ValueError('Manifesto ausente ou ambíguo.')
        mi=manifests[0];m=validar_manifesto(json.loads(z.read(mi).decode('utf-8-sig')))
        prefix=mi.filename[:-len('release.json')]
        if {i.filename for i in infos}!={prefix+n for n in m['files']}|{mi.filename}:
            raise ValueError('Conteúdo do ZIP não corresponde ao manifesto.')
        if conferir_hashes:
            for name,digest in m['files'].items():
                if hashlib.sha256(z.read(prefix+name)).hexdigest()!=digest:raise ValueError('Arquivo corrompido: '+name)
        return {'path':str(path.resolve()),'version':m['version'],'manifest':m,'prefix':prefix,
                'identity':hashlib.sha256(z.read(mi)).hexdigest()}


def extrair(path, destination):
    info=inspecionar(path,conferir_hashes=True)
    destination=Path(destination);destination.mkdir(parents=True,exist_ok=False)
    with zipfile.ZipFile(path) as z:
        for name in (*info['manifest']['files'],'release.json'):
            target=destination/name;target.parent.mkdir(parents=True,exist_ok=True)
            data=z.read(info['prefix']+name)
            if name!='release.json' and hashlib.sha256(data).hexdigest()!=info['manifest']['files'][name]:
                raise ValueError('O pacote mudou durante a extração.')
            target.write_bytes(data)
    return destination


def descobrir(root):
    folder=Path(root)/'radar_versoes'
    candidates=[];errors=[]
    for path in sorted([*folder.glob('*.zip'),*(folder/'pacotes').glob('*.zip')]):
        try:candidates.append(inspecionar(path))
        except (OSError,ValueError,KeyError,zipfile.BadZipFile) as exc:errors.append(f'{path.name}: {exc}')
    candidates.sort(key=lambda p:versao(p['version']),reverse=True)
    if candidates and len({p['identity'] for p in candidates if versao(p['version'])==versao(candidates[0]['version'])})>1:
        errors.append('Há pacotes diferentes declarando a mesma versão mais recente. Mantenha apenas o pacote correto.')
        return [],errors
    return candidates,errors
