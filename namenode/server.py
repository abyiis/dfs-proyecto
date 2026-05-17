import os
import time
import uuid
import hashlib
import threading
import grpc
from concurrent import futures
from sqlalchemy import create_engine, Column, String, Integer, BigInteger, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker

import sys
sys.path.insert(0, '/app/proto')
import dfs_pb2
import dfs_pb2_grpc

# ─── Config ──────────────────────────────────────────────────────────────────
BLOCK_SIZE      = int(os.getenv('BLOCK_SIZE', str(64 * 1024 * 1024)))  # 64 MB
REPLICATION     = int(os.getenv('REPLICATION_FACTOR', '2'))
HB_TIMEOUT      = int(os.getenv('HEARTBEAT_TIMEOUT', '90'))
PORT            = int(os.getenv('NAMENODE_PORT', '50051'))
DB_PATH         = os.getenv('DB_PATH', '/data/namenode.db')

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ─── Database ─────────────────────────────────────────────────────────────────
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    username = Column(String, primary_key=True)
    password_hash = Column(String)

class FileRecord(Base):
    __tablename__ = 'files'
    id        = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    owner     = Column(String)
    path      = Column(String)
    file_size = Column(BigInteger, default=0)
    created   = Column(Float, default=time.time)

class BlockRecord(Base):
    __tablename__ = 'blocks'
    block_id    = Column(String, primary_key=True)
    file_path   = Column(String)
    owner       = Column(String)
    block_index = Column(Integer)
    datanode_id = Column(String)
    datanode_addr = Column(String)

engine = engine = create_engine(f'sqlite:///{DB_PATH}', connect_args={'check_same_thread': False})
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

def seed_users():
    s = Session()
    if not s.query(User).filter_by(username='admin').first():
        s.add(User(username='admin',
                   password_hash=hashlib.sha256('admin123'.encode()).hexdigest()))
        s.add(User(username='juan',
                   password_hash=hashlib.sha256('upb2026'.encode()).hexdigest()))
        s.commit()
    s.close()

seed_users()

# ─── DataNode Registry ────────────────────────────────────────────────────────
_lock = threading.Lock()
_datanodes: dict[str, dict] = {}  # id → {addr, last_hb, free_bytes, block_count}

def active_nodes():
    now = time.time()
    with _lock:
        return [v for v in _datanodes.values() if now - v['last_hb'] < HB_TIMEOUT]

def pick_nodes(n: int) -> list[str]:
    nodes = sorted(active_nodes(), key=lambda x: -x['free_bytes'])
    if len(nodes) < n:
        return [nd['addr'] for nd in nodes]
    return [nd['addr'] for nd in nodes[:n]]

# ─── Token store (in-memory, good enough for demo) ────────────────────────────
_tokens: dict[str, str] = {}  # token → username

def make_token(username: str) -> str:
    tok = hashlib.sha256(f'{username}{time.time()}{uuid.uuid4()}'.encode()).hexdigest()
    _tokens[tok] = username
    return tok

def resolve_token(token: str):
    return _tokens.get(token)

# ─── Heartbeat monitor ────────────────────────────────────────────────────────
def _monitor():
    while True:
        time.sleep(30)
        now = time.time()
        dead = []
        with _lock:
            for dn_id, info in _datanodes.items():
                if now - info['last_hb'] >= HB_TIMEOUT:
                    dead.append(dn_id)
        if dead:
            print(f'[NameNode] Dead nodes detected: {dead}', flush=True)

threading.Thread(target=_monitor, daemon=True).start()

# ─── gRPC Servicer ────────────────────────────────────────────────────────────
class NameNodeServicer(dfs_pb2_grpc.NameNodeServicer):

    def Authenticate(self, req, ctx):
        s = Session()
        ph = hashlib.sha256(req.password.encode()).hexdigest()
        user = s.query(User).filter_by(username=req.username, password_hash=ph).first()
        s.close()
        if not user:
            return dfs_pb2.AuthResponse(ok=False, message='Invalid credentials')
        tok = make_token(req.username)
        print(f'[Auth] {req.username} authenticated', flush=True)
        return dfs_pb2.AuthResponse(ok=True, token=tok, message='OK')

    def PutInit(self, req, ctx):
        owner = resolve_token(req.token)
        if not owner:
            return dfs_pb2.PutResponse(ok=False, message='Unauthorized')

        nodes = pick_nodes(REPLICATION)
        if not nodes:
            return dfs_pb2.PutResponse(ok=False, message='No DataNodes available')

        assignments = []
        s = Session()

        # Remove existing file record if re-uploading
        s.query(FileRecord).filter_by(owner=owner, path=req.filename).delete()
        s.query(BlockRecord).filter_by(owner=owner, file_path=req.filename).delete()

        s.add(FileRecord(owner=owner, path=req.filename,
                         file_size=req.file_size, created=time.time()))

        for i in range(req.num_blocks):
            block_id = hashlib.sha256(
                f'{owner}/{req.filename}/{i}/{time.time()}'.encode()
            ).hexdigest()[:32]

            # Round-robin rotation so load is spread
            rotated = nodes[i % len(nodes):] + nodes[:i % len(nodes)]
            chosen = rotated[:REPLICATION]

            for addr in chosen:
                dn_id = addr  # use addr as id in demo
                s.add(BlockRecord(
                    block_id=block_id, file_path=req.filename, owner=owner,
                    block_index=i, datanode_id=dn_id, datanode_addr=addr
                ))

            assignments.append(dfs_pb2.BlockAssignment(
                block_id=block_id, block_index=i,
                datanode_addresses=chosen
            ))
            print(f'[PutInit] block {i} ({block_id[:8]}…) → {chosen}', flush=True)

        s.commit()
        s.close()
        return dfs_pb2.PutResponse(ok=True, assignments=assignments)

    def GetInfo(self, req, ctx):
        owner = resolve_token(req.token)
        if not owner:
            return dfs_pb2.GetResponse(ok=False, message='Unauthorized')

        s = Session()
        frec = s.query(FileRecord).filter_by(owner=owner, path=req.filename).first()
        if not frec:
            s.close()
            return dfs_pb2.GetResponse(ok=False, message='File not found')

        blocks = (s.query(BlockRecord)
                  .filter_by(owner=owner, file_path=req.filename)
                  .order_by(BlockRecord.block_index, BlockRecord.datanode_addr)
                  .all())
        s.close()

        # Group by block_index
        by_index: dict[int, list] = {}
        for b in blocks:
            by_index.setdefault(b.block_index, []).append(b.datanode_addr)

        assignments = [
            dfs_pb2.BlockAssignment(
                block_id=blocks[[b.block_index for b in blocks].index(idx)].block_id
                         if any(b.block_index == idx for b in blocks) else '',
                block_index=idx,
                datanode_addresses=addrs
            )
            for idx, addrs in sorted(by_index.items())
        ]
        # Re-fetch block_ids properly
        assignments2 = []
        seen = {}
        for b in blocks:
            if b.block_index not in seen:
                seen[b.block_index] = {'block_id': b.block_id, 'addrs': []}
            seen[b.block_index]['addrs'].append(b.datanode_addr)
        assignments2 = [
            dfs_pb2.BlockAssignment(
                block_id=v['block_id'], block_index=idx,
                datanode_addresses=v['addrs']
            )
            for idx, v in sorted(seen.items())
        ]

        return dfs_pb2.GetResponse(ok=True, assignments=assignments2,
                                   file_size=frec.file_size)

    def ListDir(self, req, ctx):
        owner = resolve_token(req.token)
        if not owner:
            return dfs_pb2.ListResponse(ok=False)
        s = Session()
        files = s.query(FileRecord).filter_by(owner=owner).all()
        s.close()
        entries = [f'{f.path} ({f.file_size} bytes)' for f in files]
        return dfs_pb2.ListResponse(ok=True, entries=entries)

    def Remove(self, req, ctx):
        owner = resolve_token(req.token)
        if not owner:
            return dfs_pb2.StatusResponse(ok=False, message='Unauthorized')
        s = Session()
        s.query(FileRecord).filter_by(owner=owner, path=req.filename).delete()
        s.query(BlockRecord).filter_by(owner=owner, file_path=req.filename).delete()
        s.commit()
        s.close()
        return dfs_pb2.StatusResponse(ok=True, message='Deleted')

    def MakeDir(self, req, ctx):
        return dfs_pb2.StatusResponse(ok=True, message='OK')

    def Heartbeat(self, req, ctx):
        with _lock:
            _datanodes[req.datanode_id] = {
                'addr': req.address,
                'last_hb': time.time(),
                'free_bytes': req.free_bytes,
                'block_count': req.block_count,
            }
        print(f'[HB] {req.datanode_id} @ {req.address} | '
              f'free={req.free_bytes//1024//1024}MB blocks={req.block_count}', flush=True)
        return dfs_pb2.HeartbeatResponse(ok=True)

    def BlockReport(self, req, ctx):
        print(f'[BlockReport] {req.datanode_id}: {len(req.block_ids)} blocks', flush=True)
        return dfs_pb2.StatusResponse(ok=True)

# ─── Main ─────────────────────────────────────────────────────────────────────
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_NameNodeServicer_to_server(NameNodeServicer(), server)
    server.add_insecure_port(f'[::]:{PORT}')
    server.start()
    print(f'[NameNode] Listening on :{PORT}', flush=True)
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
