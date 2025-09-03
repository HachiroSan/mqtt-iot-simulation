import os
import json
import time
import uuid
import hashlib
import mimetypes
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

from config import MQTTConfig
from mqtt_publisher import MQTTPublisher
from mqtt_subscriber import MQTTSubscriber

logger = logging.getLogger(__name__)


def generate_file_id(file_path: str) -> str:
    name = os.path.basename(file_path)
    size = os.path.getsize(file_path)
    return f"{name}-{size}-{uuid.uuid4().hex[:8]}"


class FileTransferTopics:
    def __init__(self, prefix: str, file_id: str):
        self.base = f"{prefix}/file/{file_id}"
        self.meta = f"{self.base}/meta"
        self.chunk = f"{self.base}/chunk"
        self.ack = f"{self.base}/ack"
        self.retry = f"{self.base}/retry"
        self.status = f"{self.base}/status"


class ChunkedFilePublisher:
    """Publishes a file in chunks with retry and ack handling."""

    def __init__(self, publisher: Optional[MQTTPublisher] = None, config: Optional[MQTTConfig] = None):
        self.config = config or MQTTConfig()
        self.publisher = publisher or MQTTPublisher(self.config)

    def send_file(self, file_path: str, chunk_size: int = 256 * 1024, qos: Optional[int] = None) -> str:
        if not os.path.isfile(file_path):
            raise FileNotFoundError(file_path)

        file_id = generate_file_id(file_path)
        topics = FileTransferTopics(self.config.TOPIC_PREFIX, file_id)
        qos = qos if qos is not None else self.config.QOS

        # Connect if needed
        if not self.publisher.is_connected:
            if not self.publisher.connect():
                raise RuntimeError("Failed to connect to broker")

        total_size = os.path.getsize(file_path)
        total_chunks = (total_size + chunk_size - 1) // chunk_size

        # First pass: compute manifest (file hash and per-chunk hashes)
        chunk_hashes: List[str] = []
        file_hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                file_hasher.update(data)
                chunk_hashes.append(hashlib.sha256(data).hexdigest())

        content_type, _ = mimetypes.guess_type(file_path)
        manifest = {
            "schema": "orca.file.manifest.v1",
            "file_id": file_id,
            "name": os.path.basename(file_path),
            "size": total_size,
            "chunk_size": chunk_size,
            "total_chunks": total_chunks,
            "file_sha256": file_hasher.hexdigest(),
            "chunk_sha256": chunk_hashes,
            "content_type": content_type or "application/octet-stream",
            "timestamp": int(time.time()),
        }
        # Publish manifest before sending chunks
        self.publisher.publish(topics.meta, manifest, qos)

        # Second pass: publish all chunks with their hash for convenience
        with open(file_path, "rb") as f:
            index = 0
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                payload = {
                    "file_id": file_id,
                    "chunk_index": index,
                    "sha256": manifest["chunk_sha256"][index],
                    "data": data.hex(),  # hex for safe JSON over MQTT
                }
                self.publisher.publish(topics.chunk, payload, qos)
                index += 1

        # Send status inquiry to encourage ACK or retry flow
        self.publisher.publish(topics.status, {"request": "status"}, qos)

        return file_id


class ChunkedFileSubscriber:
    """Receives file chunks and reconstructs files with persistence and retry support."""

    def __init__(
        self,
        storage_dir: str = ".transfer",
        subscriber: Optional[MQTTSubscriber] = None,
        config: Optional[MQTTConfig] = None,
    ):
        self.config = config or MQTTConfig()
        self.subscriber = subscriber or MQTTSubscriber(config=self.config)
        self.storage_root = Path(storage_dir)
        self.storage_root.mkdir(parents=True, exist_ok=True)

        # Subscribe to all file topics under prefix
        topics = [f"{self.config.TOPIC_PREFIX}/file/+/+"]
        self.subscriber._external_handler = self._on_message  # set handler
        self.subscriber._requested_topics = []  # we'll call subscribe explicitly after connect
        self._subscribe_topics = topics

    def start(self):
        if not self.subscriber.connect():
            raise RuntimeError("Subscriber failed to connect")
        self.subscriber.subscribe(self._subscribe_topics)

    # State helpers
    def _state_dir(self, file_id: str) -> Path:
        d = self.storage_root / file_id
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _state_path(self, file_id: str) -> Path:
        return self._state_dir(file_id) / "state.json"

    def _data_path(self, meta: Dict) -> Path:
        return self._state_dir(meta["file_id"]) / meta["name"]

    def _load_state(self, file_id: str) -> Dict:
        p = self._state_path(file_id)
        if p.exists():
            return json.loads(p.read_text())
        return {
            "file_id": file_id,
            "name": None,
            "size": None,
            "chunk_size": None,
            "total_chunks": None,
            "file_sha256": None,
            "chunk_sha256": None,
            "received": [],
            "complete": False,
            "ack_sent": False,
        }

    def _save_state(self, state: Dict):
        p = self._state_path(state["file_id"])
        p.write_text(json.dumps(state))

    # MQTT handlers
    def _on_message(self, topic: str, payload: bytes):
        try:
            text = payload.decode("utf-8")
            data = json.loads(text)
        except Exception:
            logger.warning("Non-JSON payload on file topic; ignored")
            return

        parts = topic.split("/")
        # .../{prefix}/file/{file_id}/{kind}
        kind = parts[-1]
        file_id = parts[-2]

        if kind == "meta":
            self._handle_meta(data)
        elif kind == "chunk":
            self._handle_chunk(data)
        elif kind == "status":
            # Publisher asked for status → emit status
            self._emit_status(file_id)

    def _handle_meta(self, meta: Dict):
        state = self._load_state(meta["file_id"])
        state.update({
            "name": meta["name"],
            "size": meta["size"],
            "chunk_size": meta["chunk_size"],
            "total_chunks": meta["total_chunks"],
            "file_sha256": meta.get("file_sha256"),
            "chunk_sha256": meta.get("chunk_sha256"),
        })
        self._save_state(state)
        # On new meta, emit status so publisher can retry missing chunks
        self._emit_status(meta["file_id"])

    def _handle_chunk(self, chunk: Dict):
        file_id = chunk["file_id"]
        idx = int(chunk["chunk_index"])
        blob = bytes.fromhex(chunk["data"]) if isinstance(chunk["data"], str) else chunk["data"]

        state = self._load_state(file_id)
        # Ensure meta received; if not, buffer state minimal
        if state["chunk_size"] is None:
            # Accept but we can't place correctly without chunk_size; still write sequential if possible
            pass

        # Append or positionally write
        data_path = self._data_path({"file_id": file_id, "name": state.get("name", f"{file_id}.bin")})

        # Ensure file exists
        if not data_path.exists():
            data_path.write_bytes(b"")

        with open(data_path, "r+b") as fp:
            fp.seek(idx * (state["chunk_size"] or len(blob)))
            fp.write(blob)

        # Chunk hash verification if present
        expected_chunk_hashes = state.get("chunk_sha256")
        if expected_chunk_hashes and idx < len(expected_chunk_hashes):
            actual = hashlib.sha256(blob).hexdigest()
            expected = expected_chunk_hashes[idx]
            if expected and actual != expected:
                logger.warning(f"Chunk hash mismatch for {file_id} chunk {idx}")
                # Do not mark as received; request retry
                self._emit_status(file_id)
                return

        if idx not in state["received"]:
            state["received"].append(idx)
        self._save_state(state)

        # Completion check
        total = state.get("total_chunks")
        if total is not None and len(state["received"]) >= total and not state["complete"]:
            state["complete"] = True
            self._save_state(state)
            # Validate full file hash before ACK
            try:
                data_path = self._data_path({"file_id": file_id, "name": state.get("name", f"{file_id}.bin")})
                hasher = hashlib.sha256()
                with open(data_path, "rb") as fp:
                    for chunk_data in iter(lambda: fp.read(state["chunk_size"] or 1024 * 1024), b""):
                        hasher.update(chunk_data)
                actual_file_hash = hasher.hexdigest()
                expected_file_hash = state.get("file_sha256")
                if expected_file_hash and actual_file_hash != expected_file_hash:
                    logger.warning(f"File hash mismatch for {file_id}; requesting retry of all chunks")
                    # Reset completion and request retry (missing will be computed as any chunk not in received)
                    state["complete"] = False
                    self._save_state(state)
                    self._emit_status(file_id)
                else:
                    self._emit_ack(file_id)
            except Exception as exc:
                logger.error(f"Error verifying full file hash for {file_id}: {exc}")
                self._emit_status(file_id)
        else:
            # periodically emit status for backpressure/flow control
            if len(state["received"]) % 50 == 0:
                self._emit_status(file_id)

    def _emit_status(self, file_id: str):
        topics = FileTransferTopics(self.config.TOPIC_PREFIX, file_id)
        state = self._load_state(file_id)
        total = state.get("total_chunks")
        missing: List[int] = []
        if total is not None:
            have = set(state["received"]) if state["received"] else set()
            missing = [i for i in range(total) if i not in have]
        payload = {
            "file_id": file_id,
            "received": len(state.get("received", [])),
            "total": total,
            "missing": missing[:500],  # cap list size per message
            "complete": state.get("complete", False),
        }
        # Send status, and if missing exists, also send a retry request
        self.subscriber.client.publish(topics.status, json.dumps(payload), self.config.QOS)
        if missing:
            retry = {"file_id": file_id, "missing": missing[:500]}
            self.subscriber.client.publish(topics.retry, json.dumps(retry), self.config.QOS)

    def _emit_ack(self, file_id: str):
        topics = FileTransferTopics(self.config.TOPIC_PREFIX, file_id)
        state = self._load_state(file_id)
        if state.get("ack_sent"):
            return
        ack = {
            "file_id": file_id,
            "status": "ok",
            "timestamp": int(time.time()),
        }
        self.subscriber.client.publish(topics.ack, json.dumps(ack), self.config.QOS)
        state["ack_sent"] = True
        self._save_state(state)


