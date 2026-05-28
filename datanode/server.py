import os
import time
import hashlib
import threading
import grpc
from concurrent import futures
from pathlib import Path

import sys
sys.path.insert(0, '/app/proto')
import dfs_pb2
import dfs_pb2_grpc

# ─── Configuración de Entorno Local ───────────────────────────────────────────
NODE_ID      = os.getenv('DATANODE_ID', 'dn1')
NODE_ADDR    = os.getenv('DATANODE_ADDR', 'datanode1:50052')
PORT         = int(os.getenv('DATANODE_PORT', '50052'))
NAMENODE     = os.getenv('NAMENODE_ADDR', 'namenode:50051')
STORAGE_DIR  = Path(os.getenv('STORAGE_DIR', '/data/blocks'))
HB_INTERVAL  = int(os.getenv('HB_INTERVAL', '5'))
CHUNK_SIZE   = 1024 * 1024 

STORAGE_DIR.mkdir(parents=True, exist_ok=True)

def block_path(block_id: str) -> Path:
    return STORAGE_DIR / block_id

def free_bytes() -> int:
    st = os.statvfs(STORAGE_DIR)
    return st.f_bavail * st.f_frsize

def stored_blocks() -> list[str]:
    return [f.name for f in STORAGE_DIR.iterdir() if f.is_file()]

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

# ─── Loops Asíncronos de Comunicación (Heartbeat & Reportes) ──────────────────
def _heartbeat_loop():
    while True:
        try:
            ch = grpc.insecure_channel(NAMENODE)
            stub = dfs_pb2_grpc.NameNodeStub(ch)
            stub.Heartbeat(dfs_pb2.HeartbeatRequest(
                datanode_id=NODE_ID, address=NODE_ADDR,
                free_bytes=free_bytes(), block_count=len(stored_blocks())
            ))
        except Exception:
            pass
        time.sleep(HB_INTERVAL)

def _block_report_loop():
    while True:
        try:
            ch = grpc.insecure_channel(NAMENODE)
            stub = dfs_pb2_grpc.NameNodeStub(ch)
            stub.BlockReport(dfs_pb2.BlockReportRequest(
                datanode_id=NODE_ADDR, block_ids=stored_blocks()
            ))
        except Exception:
            pass
        time.sleep(HB_INTERVAL * 3)

threading.Thread(target=_heartbeat_loop, daemon=True).start()
threading.Thread(target=_block_report_loop, daemon=True).start()

# ─── Servicer DataNode con Escritura Segura de Bloques ────────────────────────
class DataNodeServicer(dfs_pb2_grpc.DataNodeServicer):

    def StoreBlock(self, request_iterator, ctx):
        block_id = None
        tmp_path = None
        replicate_to = []
        file_handler = None

        try:
            for chunk in request_iterator:
                if block_id is None:
                    block_id = chunk.block_id
                    replicate_to = list(chunk.replicate_to)
                    # Arreglo crítico: Archivo temporal único y aislado por hilo
                    tmp_path = STORAGE_DIR / f'_tmp_{block_id}_{threading.get_ident()}.tmp'
                    file_handler = open(tmp_path, 'wb')
                
                file_handler.write(chunk.data)
                if chunk.is_last:
                    break

            if file_handler:
                file_handler.close()

            if not tmp_path or not tmp_path.exists():
                raise Exception("Flujo de chunks vacío.")

            final = block_path(block_id)
            tmp_path.rename(final)
            
            print(f'[Físico Guardado] Bloque: {block_id[:8]} -> Completado con éxito.', flush=True)

            # Replicación en Pipeline (Transmisión Directa entre DataNodes)
            if replicate_to:
                next_node = replicate_to[0]
                rem = replicate_to[1:]
                self._replicate(block_id, final, next_node, rem)

            return dfs_pb2.StoreAck(ok=True, block_id=block_id)

        except Exception as e:
            if file_handler and not file_handler.closed: file_handler.close()
            if tmp_path and tmp_path.exists(): tmp_path.unlink()
            return dfs_pb2.StoreAck(ok=False, block_id=block_id or '', message=str(e))

    def _replicate(self, block_id, path, next_addr, remaining):
        def gen():
            with open(path, 'rb') as f:
                while True:
                    data = f.read(CHUNK_SIZE)
                    if not data: break
                    yield dfs_pb2.BlockChunk(
                        block_id=block_id, data=data,
                        is_last=len(data) < CHUNK_SIZE, replicate_to=remaining
                    )
        try:
            ch = grpc.insecure_channel(next_addr)
            stub = dfs_pb2_grpc.DataNodeStub(ch)
            stub.StoreBlock(gen())
        except Exception as e:
            print(f'[Pipeline Fail] Error reenviando a {next_addr}: {e}', flush=True)

    def RetrieveBlock(self, req, ctx):
        path = block_path(req.block_id)
        if not path.exists():
            ctx.abort(grpc.StatusCode.NOT_FOUND, f'Bloque {req.block_id} inexistente.')
            return
        
        with open(path, 'rb') as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data: break
                yield dfs_pb2.BlockChunk(block_id=req.block_id, data=data, is_last=len(data) < CHUNK_SIZE)

    def DeleteBlock(self, req, ctx):
        path = block_path(req.block_id)
        if path.exists():
            path.unlink()
            return dfs_pb2.StatusResponse(ok=True)
        return dfs_pb2.StatusResponse(ok=False, message='No encontrado')

    def HasBlock(self, req, ctx):
        return dfs_pb2.StatusResponse(ok=block_path(req.block_id).exists())

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_DataNodeServicer_to_server(DataNodeServicer(), server)
    server.add_insecure_port(f'[::]:{PORT}')
    server.start()
    print(f'[DataNode Ejecutándose] Corriendo en el puerto físico :{PORT}', flush=True)
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
