"""Identidade nativa de processos, sem formatar datas pelo adaptador CIM."""
import ctypes
from functools import lru_cache


class FileTime(ctypes.Structure):
    _fields_=[('baixo',ctypes.c_uint32),('alto',ctypes.c_uint32)]


@lru_cache(maxsize=1)
def _api():
    api=ctypes.WinDLL('kernel32',use_last_error=True)
    api.OpenProcess.argtypes=[ctypes.c_uint32,ctypes.c_int,ctypes.c_uint32]
    api.OpenProcess.restype=ctypes.c_void_p
    api.CloseHandle.argtypes=[ctypes.c_void_p];api.CloseHandle.restype=ctypes.c_int
    api.GetProcessTimes.argtypes=[ctypes.c_void_p,*([ctypes.POINTER(FileTime)]*4)]
    api.GetProcessTimes.restype=ctypes.c_int
    api.TerminateProcess.argtypes=[ctypes.c_void_p,ctypes.c_uint32]
    api.TerminateProcess.restype=ctypes.c_int
    api.WaitForSingleObject.argtypes=[ctypes.c_void_p,ctypes.c_uint32]
    api.WaitForSingleObject.restype=ctypes.c_uint32
    return api


def _inicio_handle(api,handle):
    tempos=[FileTime() for _ in range(4)]
    if not api.GetProcessTimes(handle,*[ctypes.byref(t) for t in tempos]):
        raise OSError('Não foi possível conferir o instante de criação do processo.')
    return str((tempos[0].alto<<32)|tempos[0].baixo)


def inicio(pid):
    api=_api();handle=api.OpenProcess(0x1000,False,int(pid))
    if not handle:return None
    try:return _inicio_handle(api,handle)
    except OSError:return None
    finally:api.CloseHandle(handle)


def encerrar_interface(pid,criado):
    """Chamado apenas após conferir o comando completo da interface.

    O handle mantém a identidade entre a conferência e o encerramento. Um PID
    reutilizado nunca é encerrado. Não fecha árvores nem processos de coleta.
    """
    if not criado:raise RuntimeError('Não foi possível confirmar a identidade da interface. Nenhum processo foi encerrado.')
    api=_api();handle=api.OpenProcess(0x1000|0x0001|0x00100000,False,int(pid))
    if not handle:
        if ctypes.get_last_error()==87:return False  # Já saiu.
        raise RuntimeError('Não foi possível acessar a interface para o reinício.')
    try:
        if _inicio_handle(api,handle)!=str(criado):return False
        if not api.TerminateProcess(handle,1):
            raise RuntimeError('Não foi possível encerrar a interface para o reinício.')
        if api.WaitForSingleObject(handle,10000)!=0:
            raise RuntimeError('A interface ainda não confirmou o encerramento.')
        return True
    finally:api.CloseHandle(handle)
