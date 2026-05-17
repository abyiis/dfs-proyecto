#!/usr/bin/env python3
"""
Mini CLI para el DFS por bloques.
Uso:
  python cli.py auth <user> <pass>
  python cli.py put  <local_file> <dfs_path>
  python cli.py get  <dfs_path> <local_file>
  python cli.py ls
  python cli.py rm   <dfs_path>
"""
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
    if TOKEN_FILE.exists():
        return TOKEN_FILE.read_text().strip()
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

# ─── Commands ─────────────────────────────────────────────────────────────────

def cmd_auth(username: str, password: str):
    resp = nn_stub().Authenticate(
        dfs_pb2.AuthRequest(username=username, password=password))
    if resp.ok:
        save_token(resp.token)
        print(f'Autenticado como {username}. Token guardado.')
    else:
        print(f'Error: {resp.message}')
        sys.exit(1)

def cmd_put(local_path: str, dfs_path: str):
    token = load_token()
    path = Path(local_path)
    if not path.exists():
        print(f'Archivo no encontrado: {local_path}')
        sys.exit(1)

    file_size  = path.stat().st_size
    num_blocks = max(1, math.ceil(file_size / BLOCK_SIZE))
    print(f'Subiendo {path.name} ({file_size} bytes) → {num_blocks} bloque(s)')

    # 1. Pedir asignación al NameNode
    resp = nn_stub().PutInit(dfs_pb2.PutRequest(
        token=token, filename=dfs_path,
        file_size=file_size, num_blocks=num_blocks))
    if not resp.ok:
        print(f'Error NameNode: {resp.message}')
        sys.exit(1)

    # 2. Enviar cada bloque al DataNode primario (pipeline hace el resto)
    with open(path, 'rb') as f:
        for assignment in resp.assignments:
            data = f.read(BLOCK_SIZE)
            if not data:
                break
            block_id  = assignment.block_id
            addrs     = list(assignment.datanode_addresses)
            primary   = addrs[0]
            pipeline  = addrs[1:]  # DataNodes que el primario debe replicar
            checksum  = sha256_bytes(data)

            print(f'  Bloque {assignment.block_index} ({len(data)} bytes) → {primary}'
                  + (f' + replica {pipeline}' if pipeline else ''))

            stub = dn_stub(primary)

            def chunk_gen(block_id, data, pipeline, checksum):
                offset = 0
                first  = True
                while offset < len(data):
                    end     = min(offset + CHUNK_SIZE, len(data))
                    piece   = data[offset:end]
                    is_last = end >= len(data)
                    yield dfs_pb2.BlockChunk(
                        block_id    = block_id,
                        data        = piece,
                        is_last     = is_last,
                        checksum    = checksum if is_last else '',
                        replicate_to= pipeline if first else [],
                    )
                    first   = False
                    offset  = end

            ack = stub.StoreBlock(chunk_gen(block_id, data, pipeline, checksum))
            if not ack.ok:
                print(f'  ERROR guardando bloque {assignment.block_index}: {ack.message}')
                sys.exit(1)

    print(f'OK – {dfs_path} subido correctamente.')

def cmd_get(dfs_path: str, local_path: str):
    token = load_token()
    resp  = nn_stub().GetInfo(dfs_pb2.GetRequest(token=token, filename=dfs_path))
    if not resp.ok:
        print(f'Error: {resp.message}')
        sys.exit(1)

    print(f'Descargando {dfs_path} ({resp.file_size} bytes, '
          f'{len(resp.assignments)} bloque(s))')

    with open(local_path, 'wb') as out:
        for assignment in sorted(resp.assignments, key=lambda a: a.block_index):
            downloaded = False
            for addr in assignment.datanode_addresses:
                try:
                    stub  = dn_stub(addr)
                    total = 0
                    for chunk in stub.RetrieveBlock(
                            dfs_pb2.RetrieveRequest(block_id=assignment.block_id)):
                        out.write(chunk.data)
                        total += len(chunk.data)
                    print(f'  Bloque {assignment.block_index} ({total} bytes) ← {addr}')
                    downloaded = True
                    break
                except Exception as e:
                    print(f'  Fallo en {addr}: {e} – intentando réplica…')
            if not downloaded:
                print(f'  ERROR: no se pudo recuperar bloque {assignment.block_index}')
                sys.exit(1)

    print(f'OK – guardado en {local_path}')

def cmd_ls():
    token = load_token()
    resp  = nn_stub().ListDir(dfs_pb2.ListRequest(token=token, path='/'))
    if not resp.ok:
        print('Error listando archivos')
        sys.exit(1)
    if not resp.entries:
        print('(no hay archivos)')
    for e in resp.entries:
        print(' ', e)

def cmd_rm(dfs_path: str):
    token = load_token()
    resp  = nn_stub().Remove(dfs_pb2.RemoveRequest(token=token, filename=dfs_path))
    print('OK' if resp.ok else f'Error: {resp.message}')

# ─── Entry point ──────────────────────────────────────────────────────────────
USAGE = """
Uso:
  python cli.py auth <usuario> <contraseña>
  python cli.py put  <archivo_local> <ruta_dfs>
  python cli.py get  <ruta_dfs> <archivo_local>
  python cli.py ls
  python cli.py rm   <ruta_dfs>
"""

if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print(USAGE)
        sys.exit(0)
    cmd = args[0]
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
    else:
        print(USAGE)
        sys.exit(1)
