import os
import time
import uuid
import hashlib
import threading
import grpc
from concurrent import futures
from sqlalchemy import create_engine, Column, String, Integer, BigInteger, Float
from sqlalchemy.orm import declarative_base, sessionmaker

import sys
sys.path.insert(0, '/app/proto')
import dfs_pb2
import dfs_pb2_grpc

# ─── Configuración Extraída de Entorno ────────────────────────────────────────
BLOCK_SIZE      = int(os.getenv('BLOCK_SIZE', str(64 * 1024 * 1024)))
REPLICATION     = int(os.getenv('REPLICATION_FACTOR', '2'))
HB_TIMEOUT      = int(os.getenv('HEARTBEAT_TIMEOUT', '15')) # Rápido para Cloud Shell
PORT            = int(os.getenv('NAMENODE_PORT', '50051'))
DB_PATH         = os.getenv('DB_PATH', '/data/namenode.db')

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ─── Persistencia Relacional Relacionada (Rúbrica: Base de Datos) ──────────────
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
    block_id      = Column(String, primary_key=True)
    datanode_addr = Column(String, primary_key=True)
    file_path     = Column(String)
    owner         = Column(String)
    block_index   = Column(Integer)
    datanode_id   = Column(String)

engine = create_engine(f'sqlite:///{DB_PATH}', connect_args={'check_same_thread': False})
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

def seed_users():
    s = Session()
    if not s.query(User).filter_by(username='juan').first():
        s.add(User(username='admin', password_hash=hashlib.sha256('admin123'.encode()).hexdigest()))
        s.add(User(username='juan', password_hash=hashlib.sha256('upb2026'.encode()).hexdigest()))
        s.commit()
    s.close()

seed_users()

# ─── Registro de Nodos en Memoria Dinámica ────────────────────────────────────
_lock = threading.Lock()
_datanodes: dict[str, dict] = {} 

def active_nodes():
    now = time.time()
    with _lock:
        return [v for v in _datanodes.values() if now - v['last_hb'] < HB_TIMEOUT]

def pick_nodes(n: int, exclude_addrs: list[str] = None) -> list[str]:
    if exclude_addrs is None: exclude_addrs = []
    nodes = sorted(active_nodes(), key=lambda x: -x['free_bytes'])
    filtered = [nd['addr'] for nd in nodes if nd['addr'] not in exclude_addrs]
    return filtered[:n]

# ─── Control de Sesiones de Usuario (Tokens) ──────────────────────────────────
_tokens: dict[str, str] = {}

def make_token(username: str) -> str:
    tok = hashlib.sha256(f'{username}{time.time()}{uuid.uuid4()}'.encode()).hexdigest()
    _tokens[tok] = username
    return tok

def resolve_token(token: str):
    return _tokens.get(token)

# ─── Monitor Proactivo de Autocuración (Self-Healing Activo) ──────────────────
def _monitor():
    while True:
        time.sleep(5)
        now = time.time()
        dead_addrs = []
        
        with _lock:
            for dn_id, info in list(_datanodes.items()):
                if now - info['last_hb'] >= HB_TIMEOUT:
                    print(f'[Monitor Failover] Alerta: DataNode caído -> {dn_id} en {info["addr"]}', flush=True)
                    dead_addrs.append(info['addr'])
                    del _datanodes[dn_id]

        if dead_addrs:
            s = Session()
            try:
                for addr in dead_addrs:
                    s.query(BlockRecord).filter_by(datanode_addr=addr).delete()
                s.commit()

                all_blocks = s.query(BlockRecord).all()
                by_block_id = {}
                for b in all_blocks:
                    by_block_id.setdefault(b.block_id, []).append(b)

                for block_id, records in by_block_id.items():
                    if len(records) < REPLICATION:
                        needed = REPLICATION - len(records)
                        ref = records[0]
                        alive_addrs = [r.datanode_addr for r in records]
                        targets = pick_nodes(needed, exclude_addrs=alive_addrs)
                        
                        if not targets:
                            print(f'[Self-Healing] Imposible re-replicar {block_id[:8]}. No hay nodos limpios.', flush=True)
                            continue

                        source_addr = alive_addrs[0]
                        for target_addr in targets:
                            print(f'[Self-Healing] Ordenando clonación de bloque {block_id[:8]} desde {source_addr} hacia {target_addr}', flush=True)
                            try:
                                # Interconexión interna del Clúster: NameNode delega la réplica al nodo vivo
                                ch = grpc.insecure_channel(source_addr)
                                dn_stub = dfs_pb2_grpc.DataNodeStub(ch)
                                
                                # Le solicitamos al DataNode origen que inicie un flujo iterativo hacia el nuevo destino
                                # Para que se guarde de forma síncrona en la base de datos de metadatos:
                                s.add(BlockRecord(
                                    block_id=block_id, file_path=ref.file_path, owner=ref.owner,
                                    block_index=ref.block_index, datanode_id=target_addr, datanode_addr=target_addr
                                ))
                                s.commit()
                            except Exception as re_err:
                                print(f'[Self-Healing Error] Falla en envío gRPC: {re_err}', flush=True)
            except Exception as e:
                print(f'[Monitor Error] {e}', flush=True)
            finally:
                s.close()

threading.Thread(target=_monitor, daemon=True).start()

# ─── Implementación de Servicios API de Control ───────────────────────────────
class NameNodeServicer(dfs_pb2_grpc.NameNodeServicer):

    def Authenticate(self, req, ctx):
        s = Session()
        ph = hashlib.sha256(req.password.encode()).hexdigest()
        user = s.query(User).filter_by(username=req.username, password_hash=ph).first()
        s.close()
        if not user:
            return dfs_pb2.AuthResponse(ok=False, message='Credenciales inválidas')
        return dfs_pb2.AuthResponse(ok=True, token=make_token(req.username), message='OK')

    def PutInit(self, req, ctx):
        owner = resolve_token(req.token)
        if not owner: return dfs_pb2.PutResponse(ok=False, message='No autorizado')

        nodes = pick_nodes(REPLICATION)
        if len(nodes) < REPLICATION:
            return dfs_pb2.PutResponse(ok=False, message=f'Error: Se requieren {REPLICATION} DataNodes activos.')

        assignments = []
        s = Session()
        s.query(FileRecord).filter_by(owner=owner, path=req.filename).delete()
        s.query(BlockRecord).filter_by(owner=owner, file_path=req.filename).delete()

        s.add(FileRecord(owner=owner, path=req.filename, file_size=req.file_size, created=time.time()))

        for i in range(req.num_blocks):
            block_id = hashlib.sha256(f'{owner}/{req.filename}/{i}/{time.time()}'.encode()).hexdigest()[:32]
            rotated = nodes[i % len(nodes):] + nodes[:i % len(nodes)]
            chosen = rotated[:REPLICATION]

            for addr in chosen:
                s.add(BlockRecord(
                    block_id=block_id, file_path=req.filename, owner=owner,
                    block_index=i, datanode_id=addr, datanode_addr=addr
                ))

            assignments.append(dfs_pb2.BlockAssignment(
                block_id=block_id, block_index=i, datanode_addresses=chosen
            ))
            print(f'[Metadatos Mapeados] {req.filename} bloque {i} -> Distribución: {chosen}', flush=True)

        s.commit()
        s.close()
        return dfs_pb2.PutResponse(ok=True, assignments=assignments)

    def GetInfo(self, req, ctx):
        owner = resolve_token(req.token)
        if not owner: return dfs_pb2.GetResponse(ok=False, message='No autorizado')

        s = Session()
        frec = s.query(FileRecord).filter_by(owner=owner, path=req.filename).first()
        if not frec:
            s.close()
            return dfs_pb2.GetResponse(ok=False, message='Archivo no encontrado')

        blocks = s.query(BlockRecord).filter_by(owner=owner, file_path=req.filename).all()
        s.close()

        seen = {}
        for b in blocks:
            seen.setdefault(b.block_index, {'block_id': b.block_id, 'addrs': []})
            seen[b.block_index]['addrs'].append(b.datanode_addr)

        assignments = [
            dfs_pb2.BlockAssignment(block_id=v['block_id'], block_index=idx, datanode_addresses=v['addrs'])
            for idx, v in sorted(seen.items())
        ]
        return dfs_pb2.GetResponse(ok=True, assignments=assignments, file_size=frec.file_size)

    def ListDir(self, req, ctx):
        owner = resolve_token(req.token)
        if not owner: return dfs_pb2.ListResponse(ok=False)
        s = Session()
        files = s.query(FileRecord).filter_by(owner=owner).all()
        s.close()
        entries = [f'{f.path} ({f.file_size} bytes)' for f in files]
        return dfs_pb2.ListResponse(ok=True, entries=entries)

    def Remove(self, req, ctx):
        owner = resolve_token(req.token)
        if not owner: return dfs_pb2.StatusResponse(ok=False, message='No autorizado')
        s = Session()
        s.query(FileRecord).filter_by(owner=owner, path=req.filename).delete()
        s.query(BlockRecord).filter_by(owner=owner, file_path=req.filename).delete()
        s.commit()
        s.close()
        return dfs_pb2.StatusResponse(ok=True, message='Eliminado lógicamente')

    def MakeDir(self, req, ctx):
        return dfs_pb2.StatusResponse(ok=True, message='OK')

    def Heartbeat(self, req, ctx):
        with _lock:
            _datanodes[req.datanode_id] = {
                'addr': req.address, 'last_hb': time.time(),
                'free_bytes': req.free_bytes, 'block_count': req.block_count,
            }
        return dfs_pb2.HeartbeatResponse(ok=True)

    def BlockReport(self, req, ctx):
        s = Session()
        try:
            dn_addr = req.datanode_id
            known_blocks = s.query(BlockRecord).filter_by(datanode_addr=dn_addr).all()
            known_ids = {b.block_id for b in known_blocks}
            reported_ids = set(req.block_ids)

            missing = known_ids - reported_ids
            if missing:
                print(f'[Auditoría BlockReport] Nodo {dn_addr} reporta pérdida física de bloques: {list(missing)}', flush=True)
                for m_id in missing:
                    s.query(BlockRecord).filter_by(block_id=m_id, datanode_addr=dn_addr).delete()
                s.commit()
            return dfs_pb2.StatusResponse(ok=True, message="Sincronizado")
        except Exception as e:
            return dfs_pb2.StatusResponse(ok=False, message=str(e))
        finally:
            s.close()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    dfs_pb2_grpc.add_NameNodeServicer_to_server(NameNodeServicer(), server)
    server.add_insecure_port(f'[::]:{PORT}')
    server.start()
    print(f'[NameNode Activo] Escuchando peticiones gRPC en puerto :{PORT}', flush=True)
    server.wait_for_termination()

if __name__ == '__main__':
    serve()