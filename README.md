# DFS por Bloques – Mini HDFS
**Juan David Parra Sierra | UPB 2026**

Sistema de archivos distribuido minimalista con NameNode central, 3 DataNodes y
replicación pipeline. Stack: Python 3.12 + gRPC + SQLAlchemy + SQLite + Docker Compose.

---

## Estructura del proyecto

```
dfs/
├── proto/
│   └── dfs.proto              # Definición de servicios gRPC (NameNode + DataNode)
├── namenode/
│   ├── Dockerfile
│   └── server.py              # NameNode: metadatos (SQLite), tokens, self-healing
├── datanode/
│   ├── Dockerfile
│   └── server.py              # DataNode: almacenamiento + heartbeat + replicación pipeline
├── client/
│   ├── Dockerfile
│   └── cli.py                 # CLI: auth / put / get / ls / rm
├── test_files/                # Archivos de prueba (se monta en el cliente)
├── docker-compose.yml
└── README.md
```

---

## Requisitos

- Docker >= 24
- Docker Compose v2 (`docker compose` sin guion)
- 4 GB de RAM disponibles

---

## 1. Levantar el clúster

```bash
# Desde la carpeta dfs/
cd dfs

# Construir imágenes (solo la primera vez o cuando cambie el código)
docker compose build

# Levantar todos los nodos en segundo plano
docker compose up -d

# Ver logs en tiempo real (ctrl+C para salir, no para el clúster)
docker compose logs -f
```

Esperar ~10 segundos a que los DataNodes envíen el primer heartbeat al NameNode.
Deberías ver en los logs:

```
namenode  | [NameNode Activo] Escuchando peticiones gRPC en puerto :50051
datanode1 | [DataNode Ejecutándose] Corriendo en el puerto físico :50052
datanode1 | [HB] datanode1:50052 | free=...MB blocks=0
```

---

## 2. Usar la CLI

```bash
# Abrir shell interactivo en el contenedor cliente
docker compose exec client bash

# Dentro del contenedor:

# Autenticarse (usuarios por defecto: admin/admin123 o juan/upb2026)
python /app/client/cli.py auth juan upb2026

# Crear un archivo de prueba de 10 MB
dd if=/dev/urandom of=/files/prueba.bin bs=1M count=10

# Subir al DFS
python /app/client/cli.py put /files/prueba.bin /prueba.bin

# Listar archivos en el DFS
python /app/client/cli.py ls

# Descargar y verificar integridad
python /app/client/cli.py get /prueba.bin /files/descargado.bin
md5sum /files/prueba.bin /files/descargado.bin   # deben ser iguales

# Eliminar
python /app/client/cli.py rm /prueba.bin
```

---

## 3. Verificar la replicación

```bash
# Ver qué bloques tiene cada DataNode
docker exec datanode1 ls /data/blocks/
docker exec datanode2 ls /data/blocks/
docker exec datanode3 ls /data/blocks/

# Con RF=2, cada bloque debe aparecer en exactamente 2 DataNodes
```

---

## 4. Probar tolerancia a fallos

```bash
# Simular caída de DataNode1
docker compose stop datanode1

# Descargar el archivo (debe funcionar usando las réplicas en dn2 o dn3)
docker compose exec client bash -c \
  "python /app/client/cli.py get /prueba.bin /files/recuperado.bin"

# Verificar integridad
docker exec dfs_client md5sum /files/prueba.bin /files/recuperado.bin

# Volver a levantar datanode1
docker compose start datanode1
```

---

## 5. Ver estado del clúster

```bash
# Logs del NameNode (heartbeats, asignaciones, self-healing)
docker compose logs namenode

# Logs de un DataNode específico
docker compose logs datanode1

# Estado de todos los contenedores
docker compose ps

# Uso de disco por DataNode
docker exec datanode1 df -h /data/blocks
docker exec datanode2 df -h /data/blocks
docker exec datanode3 df -h /data/blocks
```

---

## 6. Apagar el clúster

```bash
# Parar sin borrar datos
docker compose down

# Parar Y borrar volúmenes (reset completo)
docker compose down -v
```

---

## Variables de entorno configurables

| Variable            | Default      | Descripción                                        |
|---------------------|--------------|----------------------------------------------------|
| `BLOCK_SIZE`        | 67108864     | Tamaño de bloque en bytes (64 MB)                  |
| `REPLICATION_FACTOR`| 2            | Número de réplicas por bloque                      |
| `HEARTBEAT_TIMEOUT` | 15           | Segundos sin HB para declarar nodo muerto          |
| `HB_INTERVAL`       | 4            | Intervalo de heartbeat en segundos                 |

Para cambiar un valor, editar `docker-compose.yml` en la sección `environment` del servicio correspondiente y hacer `docker compose up -d --build`.

---

## Usuarios por defecto

| Usuario | Contraseña |
|---------|------------|
| admin   | admin123   |
| juan    | upb2026    |

---

## Protocolos implementados

- **Cliente ↔ NameNode**: gRPC unario (auth, put-init, get-info, ls, rm)
- **Cliente ↔ DataNode**: gRPC streaming (transferencia de bloques en chunks de 1 MB)
- **DataNode ↔ NameNode**: gRPC unario (heartbeat cada 4s, block report cada 12s)
- **DataNode ↔ DataNode**: gRPC streaming (replicación pipeline, iniciada por el nodo origen)
- **Self-Healing**: hilo monitor en NameNode detecta caídas (cada 5s) y re-replica bloques huérfanos
