"""Message broker abstraction — Kafka implementation with graceful degradation.

Topic topology:
  nl2sql.task.request  – FastAPI → Worker (task submit)
  nl2sql.task.status   – Worker → SSE/API (progress events)
  nl2sql.task.result   – Worker → SSE/API (final result)
  nl2sql.task.dlq      – Worker → manual (dead-letter, retries exhausted)
  nl2sql.task.feedback – FastAPI → Worker (human correction guidance)
"""

import json
import logging
import os
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

_log = logging.getLogger("nl2sql.broker")

# ── Topic constants ──────────────────────────────────────────────────────────

TOPIC_REQUEST = "nl2sql.task.request"
TOPIC_STATUS = "nl2sql.task.status"
TOPIC_RESULT = "nl2sql.task.result"
TOPIC_DLQ = "nl2sql.task.dlq"
TOPIC_FEEDBACK = "nl2sql.task.feedback"

ALL_TOPICS = [TOPIC_REQUEST, TOPIC_STATUS, TOPIC_RESULT, TOPIC_DLQ, TOPIC_FEEDBACK]

# ── Connection config ─────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_DEFAULT_TIMEOUT_MS = 5000


@dataclass
class TaskMessage:
    """Normalised message envelope for all broker implementations."""
    task_id: str
    event: str           # "submitted" | "running" | "node_done" | "success" | "failed" | "timeout" | "cancelled"
    payload: dict = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps({"task_id": self.task_id, "event": self.event,
                           "payload": self.payload}, ensure_ascii=False, default=str)

    @staticmethod
    def from_json(raw: str) -> "TaskMessage":
        d = json.loads(raw)
        return TaskMessage(task_id=d["task_id"], event=d["event"], payload=d.get("payload", {}))


# ── Abstract interface ───────────────────────────────────────────────────────

class MessageBroker(ABC):
    """Transport-agnostic message broker. Implementations: KafkaBroker, RedisBroker, etc."""

    @abstractmethod
    def publish(self, topic: str, msg: TaskMessage) -> None:
        """Send a message to a topic."""

    @abstractmethod
    def subscribe(self, topic: str, group_id: str,
                  callback, timeout_ms: int = 5000):
        """Blocking consume loop. Calls callback(TaskMessage) for each message."""

    @abstractmethod
    def create_topics(self, topics: list[str]) -> None:
        """Ensure topics exist."""

    @abstractmethod
    def close(self) -> None:
        """Release resources."""


# ── Kafka implementation ─────────────────────────────────────────────────────

class KafkaBroker(MessageBroker):
    """Kafka-backed broker using kafka-python.

    Graceful degradation: if Kafka is unreachable, publish is a no-op
    and subscribe logs a warning — the system degrades to sync mode.
    """

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP,
                 client_id: str = "nl2sql-agent", timeout_ms: int = KAFKA_DEFAULT_TIMEOUT_MS):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.timeout_ms = timeout_ms
        self._producer = None
        self._connected = False
        self._try_connect()

    # ── Connection ────────────────────────────────────────────────────────

    def _try_connect(self) -> bool:
        try:
            from kafka import KafkaAdminClient
            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"{self.client_id}-admin",
                request_timeout_ms=self.timeout_ms,
            )
            admin.list_topics()
            admin.close()
            self._connected = True
            _log.info("Kafka broker connected: %s", self.bootstrap_servers)
            return True
        except Exception as e:
            self._connected = False
            _log.warning("Kafka unavailable (%s) — broker disabled, system degrades to sync", e)
            return False

    def _get_producer(self):
        if not self._connected:
            return None
        if self._producer is not None:
            return self._producer
        try:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"{self.client_id}-producer",
                value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
                request_timeout_ms=self.timeout_ms,
                max_block_ms=3000,
            )
            return self._producer
        except Exception as e:
            _log.warning("Kafka producer failed: %s", e)
            return None

    # ── Public API ─────────────────────────────────────────────────────────

    def create_topics(self, topics: list[str] = ALL_TOPICS) -> None:
        if not self._connected:
            return
        try:
            from kafka import KafkaAdminClient
            from kafka.admin import NewTopic
            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"{self.client_id}-admin",
                request_timeout_ms=self.timeout_ms,
            )
            existing = set(admin.list_topics())
            new_topics = [
                NewTopic(name=t, num_partitions=1, replication_factor=1)
                for t in topics if t not in existing
            ]
            if new_topics:
                admin.create_topics(new_topics)
                _log.info("Topics created: %s", [t.name for t in new_topics])
            else:
                _log.info("All %d topics already exist", len(topics))
            admin.close()
        except Exception as e:
            _log.warning("Failed to create topics: %s", e)

    def publish(self, topic: str, msg: TaskMessage) -> bool:
        if not self._connected:
            return False
        producer = self._get_producer()
        if producer is None:
            return False
        try:
            future = producer.send(topic, key=msg.task_id.encode("utf-8"),
                                   value=msg.to_json())
            future.get(timeout=5)
            return True
        except Exception as e:
            _log.error("Publish to %s failed: %s", topic, e)
            return False

    def subscribe(self, topic: str, group_id: str,
                  callback, timeout_ms: int = 5000):
        if not self._connected:
            _log.warning("Kafka not connected — subscribe loop not started")
            return
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            client_id=f"{self.client_id}-consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=False,     # manual commit after successful processing
            value_deserializer=lambda v: v.decode("utf-8") if v else "",
            max_poll_interval_ms=600000,  # 10 min — long enough for LangGraph
            request_timeout_ms=timeout_ms,
        )
        _log.info("Subscribed to %s (group=%s)", topic, group_id)
        try:
            for record in consumer:
                try:
                    msg = TaskMessage.from_json(record.value)
                    callback(msg)
                    consumer.commit()
                except Exception as e:
                    _log.error("Consumer callback error for task %s: %s",
                               record.key.decode() if record.key else "?", e)
                    # Don't commit — message will be retried on next poll
        finally:
            consumer.close()

    def close(self) -> None:
        if self._producer:
            self._producer.close()
            self._producer = None
        _log.info("Kafka broker closed")


# ── Module-level singleton ───────────────────────────────────────────────────

_broker: KafkaBroker | None = None


def get_broker() -> KafkaBroker:
    global _broker
    if _broker is None:
        _broker = KafkaBroker()
    return _broker
