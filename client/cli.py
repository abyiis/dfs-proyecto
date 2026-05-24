#!/usr/bin/env python3
import os
import sys
import math
import hashlib
import grpc
from pathlib import Path

sys.path.insert(0, '/app/proto')
import dfs_pb2
import dfs_pb2_grpc

NAMENODE   = os.getenv('NAMENODE_ADDR', 'namenode:50051')
BLOCK_SIZE = int(os.getenv('BLOCK_SIZE', str(64 * 1024 * 1024)))
TOKEN_FILE = Path('/tmp/.dfs_token')
CHUNK_SIZE = 1024 * 1024  # 1 MB

def load_token() -> str:
    if TOKEN_FILE.exists(): return TOKEN_FILE.read_text().strip()
    return ''

def save_token(tok: str):
    TOKEN_FILE.write_text(tok)

def nn_stub():
    ch = grpc.insecure_channel(NAMENODE)
    return dfs_pb2_grpc.NameNodeStub(ch)

def dn_stub(addr: str):
    ch = grpc.insecure_channel(addr)
    return dfs_pb2_grpc.DataNodeStub(ch)

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def cmd_auth(u, p):
    resp = nn_stub().Authenticate(dfs_pb2.AuthRequest(username=u, password=p))
    if resp.ok:
        save_token(resp.token)
        print('Sesión iniciada con éxito. Token guardado.')
    else:
        print(f'Error de acceso: {resp.message}')
        sys.exit(1)

def cmd_put(local_path: str, dfs_path: str):
    token = load_token()
    if not token:
        print('Error: no autenticado.')
        sys.exit(1)

    lp = Path(local_path)
    if not lp.exists():
        print(f'Archivo local no existe: {local_path}')
        sys.exit(1)

    size = lp.stat().st_size
    num_blocks = math.ceil(size / BLOCK_SIZE) or 1

    print(f'Inicializando subida: {size} bytes en {num_blocks} bloque(s)…')
    init_resp = nn_stub().PutInit(dfs_pb2.PutRequest(
        token=token, filename=dfs_path, file_size=size, num_blocks=num_blocks
    ))

    if not init_resp.ok:
        print(f'Rechazado por NameNode: {init_resp.message}')
        sys.exit(1)

    with open(lp, 'rb') as f:
        for assignment in init_resp.assignments:
            block_data = f.read(BLOCK_SIZE)
            block_checksum = hashlib.sha256(block_data).hexdigest()
            
            success = False
            for target_dn in assignment.datanode_addresses:
                print(f'  -> Transmitiendo bloque {assignment.block_index} hacia {target_dn}…')
                try:
                    def chunk_generator(data=block_data, bid=assignment.block_id,
                                        pipeline=[a for a in assignment.datanode_addresses if a != target_dn],
                                        cksum=block_checksum):
                        offset = 0
                        while offset < len(data):
                            end   = min(offset + CHUNK_SIZE, len(data))
                            chunk = data[offset:end]
                            offset = end
                            yield dfs_pb2.BlockChunk(
                                block_id=bid,
                                data=chunk,
                                is_last=(offset >= len(data)),
                                replicate_to=pipeline,
                                checksum=cksum
                            )

                    ack = dn_stub(target_dn).StoreBlock(chunk_generator())
                    if ack.ok:
                        success = True
                        break
                    else:
                        print(f'     Rechazado por el nodo: {ack.message}')
                except Exception as e:
                    print(f'     Falla de conexión con nodo {target_dn}: {e}')

            if not success:
                print(f'ERROR Crítico: El bloque {assignment.block_index} no pudo ser replicado.')
                sys.exit(1)

    print('OK – Archivo distribuido de forma íntegra.')

def cmd_get(dfs_path: str, local_path: str):
    token = load_token()
    info = nn_stub().GetInfo(dfs_pb2.GetRequest(token=token, filename=dfs_path))
    if not info.ok:
        print(f'Error al buscar metadatos: {info.message}')
        sys.exit(1)

    with open(local_path, 'wb') as f:
        for assignment in info.assignments:
            downloaded = False
            for addr in assignment.datanode_addresses:
                try:
                    chunks = dn_stub(addr).RetrieveBlock(dfs_pb2.RetrieveRequest(block_id=assignment.block_id))
                    temp_block = bytearray()
                    received_checksum = ''
                    for c in chunks:
                        temp_block.extend(c.data)
                        if c.is_last:
                            received_checksum = c.checksum
                    
                    actual_checksum = hashlib.sha256(temp_block).hexdigest()
                    if received_checksum and received_checksum != actual_checksum:
                        print(f'  [WARN] Bloque corrupto recibido desde {addr}. Saltando a otra réplica.')
                        continue
                        
                    f.write(temp_block)
                    downloaded = True
                    break
                except Exception:
                    continue
            if not downloaded:
                print(f'  ERROR: Imposible recuperar bloque {assignment.block_index}. Clúster corrupto.')
                sys.exit(1)

    print(f'OK – Descargado de forma segura en {local_path}')

def cmd_ls():
    token = load_token()
    resp  = nn_stub().ListDir(dfs_pb2.ListRequest(token=token, path='/'))
    if not resp.ok:
        print('Error listando la raíz')
        sys.exit(1)
    if not resp.entries:
        print('(Directorio vacío)')
    for e in resp.entries:
        print(' ', e)

def cmd_rm(dfs_path: str):
    token = load_token()
    resp  = nn_stub().Remove(dfs_pb2.RemoveRequest(token=token, filename=dfs_path))
    print('OK' if resp.ok else f'Error: {resp.message}')

# Fix Menor: Métodos mkdir y rmdir con validaciones lógicas cruzadas en catálogo
def cmd_mkdir(dfs_path: str):
    token = load_token()
    if not token:
        print('Error: No autenticado.')
        sys.exit(1)
    resp = nn_stub().MakeDir(dfs_pb2.MkdirRequest(token=token, path=dfs_path))
    print(f'OK – directorio {dfs_path} creado.' if resp.ok else f'Error: {resp.message}')

def cmd_rmdir(dfs_path: str):
    token = load_token()
    if not token:
        print('Error: No autenticado.')
        sys.exit(1)
    ls_resp = nn_stub().ListDir(dfs_pb2.ListRequest(token=token, path='/'))
    if ls_resp.ok:
        hijos = [e for e in ls_resp.entries if dfs_path.rstrip('/') + '/' in e]
        if hijos:
            print(f'Error: El directorio {dfs_path} contiene {len(hijos)} archivo(s) adentro.')
            sys.exit(1)
    resp = nn_stub().Remove(dfs_pb2.RemoveRequest(token=token, filename=dfs_path))
    print(f'OK – directorio {dfs_path} eliminado.' if resp.ok else f'Error: {resp.message}')

USAGE = """Uso:
  python cli.py auth  <usuario> <contraseña>
  python cli.py put   <archivo_local> <ruta_dfs>
  python cli.py get   <ruta_dfs> <archivo_local>
  python cli.py ls
  python cli.py rm    <ruta_dfs>
  python cli.py mkdir <ruta_dfs>
  python cli.py rmdir <ruta_dfs>"""

if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print(USAGE)
        sys.exit(0)
    cmd = args[0]
    try:
        if cmd == 'auth' and len(args) == 3:
            cmd_auth(args[1], args[2])
        elif cmd == 'put' and len(args) == 3:
            cmd_put(args[1], args[2])
        elif cmd == 'get' and len(args) == 3:
            cmd_get(args[1], args[2])
        elif cmd == 'ls':
            cmd_ls()
        elif cmd == 'rm' and len(args) == 2:
            cmd_rm(args[1])
        elif cmd == 'mkdir' and len(args) == 2:
            cmd_mkdir(args[1])
        elif cmd == 'rmdir' and len(args) == 2:
            cmd_rmdir(args[1])
        else:
            print(USAGE)
            sys.exit(1)
    except grpc.RpcError as e:
        print(f'Falla de red gRPC [{e.code().name}]: {e.details()}')
        sys.exit(1)