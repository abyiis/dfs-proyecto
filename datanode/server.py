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

# ─── Config ──────────────────────────────────────────────────────────────────
NODE_ID      = os.getenv('DATANODE_ID', 'dn1')
NODE_ADDR    = os.getenv('DATANODE_ADDR', 'datanode1:50052')   # how clients reach us
PORT         = int(os.getenv('DATANODE_PORT', '50052'))
NAMENODE     = os.getenv('NAMENODE_ADDR', 'namenode:50051')
STORAGE_DIR  = Path(os.getenv('STORAGE_DIR', '/data/blocks'))
HB_INTERVAL  = int(os.getenv('HB_INTERVAL', '30'))
CHUNK_SIZE   = 1024 * 1024  # 1 MB chunks for streaming

STORAGE_DIR.mkdir(parents=True, exist_ok=True)

# ─── Helpers ─────────────────────────────────────────────────────────────────
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
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b''):
            h.update(chunk)
    return h.hexdigest()

# ─── Heartbeat thread ─────────────────────────────────────────────────────────
def _heartbeat_loop():
    time.sleep(5)  # wait for namenode to start
    while True:
        try:
            with grpc.insecure_channel(NAMENODE) as ch:
                stub = dfs_pb2_grpc.NameNodeStub(ch)
                stub.Heartbeat(dfs_pb2.HeartbeatRequest(
                    datanode_id=NODE_ID,
                    address=NODE_ADDR,
                    free_bytes=free_bytes(),
                    block_count=len(stored_blocks()),
                ))
        except Exception as e:
            print(f'[HB] Error contacting NameNode: {e}', flush=True)
        time.sleep(HB_INTERVAL)

def _block_report_loop():
    time.sleep(10)
    while True:
        try:
            with grpc.insecure_channel(NAMENODE) as ch:
                stub = dfs_pb2_grpc.NameNodeStub(ch)
                stub.BlockReport(dfs_pb2.BlockReportRequest(
                    datanode_id=NODE_ID,
                    block_ids=stored_blocks(),
                ))
        except Exception as e:
            print(f'[BR] Error: {e}', flush=True)
        time.sleep(300)  # every 5 minutes

threading.Thread(target=_heartbeat_loop, daemon=True).start()
threading.Thread(target=_block_report_loop, daemon=True).start()

# ─── gRPC Servicer ────────────────────────────────────────────────────────────
class DataNodeServicer(dfs_pb2_grpc.DataNodeServicer):

    def StoreBlock(self, request_iterator, ctx):
        block_id = None
        tmp_path = None
        replicate_to = []

        try:
            with open('/tmp/_dfs_tmp', 'wb') as f:
                for chunk in request_iterator:
                    if block_id is None:
                        block_id = chunk.block_id
                        replicate_to = list(chunk.replicate_to)
                        tmp_path = Path(f'/tmp/_dfs_{block_id}')
                        print(f'[Store] Receiving block {block_id[:8]}…', flush=True)
                    f.write(chunk.data)
                    if chunk.is_last:
                        break

            # Move to final location
            final = block_path(block_id)
            Path('/tmp/_dfs_tmp').rename(final)
            checksum = sha256_file(final)
            print(f'[Store] Stored {block_id[:8]}… ({final.stat().st_size} bytes) sha256={checksum[:8]}…', flush=True)

            # Pipeline replication: forward to next node if any
            if replicate_to:
                next_addr = replicate_to[0]
                remaining = replicate_to[1:]
                print(f'[Replicate] Forwarding {block_id[:8]}… → {next_addr}', flush=True)
                self._replicate(block_id, final, next_addr, remaining)

            return dfs_pb2.StoreAck(ok=True, block_id=block_id)

        except Exception as e:
            print(f'[Store] Error: {e}', flush=True)
            return dfs_pb2.StoreAck(ok=False, block_id=block_id or '', message=str(e))

    def _replicate(self, block_id: str, path: Path, addr: str, remaining: list):
        try:
            with grpc.insecure_channel(addr) as ch:
                stub = dfs_pb2_grpc.DataNodeStub(ch)

                def chunks():
                    with open(path, 'rb') as f:
                        first = True
                        while True:
                            data = f.read(CHUNK_SIZE)
                            if not data:
                                break
                            is_last = len(data) < CHUNK_SIZE or f.peek(1) == b''
                            yield dfs_pb2.BlockChunk(
                                block_id=block_id,
                                data=data,
                                is_last=is_last,
                                replicate_to=remaining if first else [],
                            )
                            first = False
                            if is_last:
                                break

                stub.StoreBlock(chunks())
        except Exception as e:
            print(f'[Replicate] Failed to forward to {addr}: {e}', flush=True)

    def RetrieveBlock(self, req, ctx):
        path = block_path(req.block_id)
        if not path.exists():
            ctx.abort(grpc.StatusCode.NOT_FOUND, f'Block {req.block_id} not found')
            return
        print(f'[Retrieve] Sending block {req.block_id[:8]}…', flush=True)
        with open(path, 'rb') as f:
            while True:
                data = f.read(CHUNK_SIZE)
                if not data:
                    break
                is_last = f.peek(1) == b'' if hasattr(f, 'peek') else len(data) < CHUNK_SIZE
                yield dfs_pb2.BlockChunk(block_id=req.block_id, data=data, is_last=is_last)

    def DeleteBlock(self, req, ctx):
        path = block_path(req.block_id)
        if path.exists():
            path.unlink()
            return dfs_pb2.StatusResponse(ok=True)
        return dfs_pb2.StatusResponse(ok=False, message='Not found')

    def HasBlock(self, req, ctx):
        ok = block_path(req.block_id).exists()
        return dfs_pb2.StatusResponse(ok=ok)

# ─── Main ─────────────────────────────────────────────────────────────────────
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_DataNodeServicer_to_server(DataNodeServicer(), server)
    server.add_insecure_port(f'[::]:{PORT}')
    server.start()
    print(f'[DataNode:{NODE_ID}] Listening on :{PORT} | storage={STORAGE_DIR}', flush=True)
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
