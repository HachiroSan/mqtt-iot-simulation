import json
import time
import logging
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union
import paho.mqtt.client as mqtt
from config import MQTTConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


TopicList = Iterable[Union[str, Tuple[str, int]]]


class MQTTSubscriber:
    """MQTT Subscriber for IoT simulation testing"""

    def __init__(
        self,
        config: MQTTConfig = None,
        topics: Optional[TopicList] = None,
        message_handler: Optional[Callable[[str, bytes], None]] = None,
        client_id_suffix: Optional[str] = "_sub",
    ):
        """Initialize MQTT Subscriber.

        Args:
            config: Optional MQTTConfig; defaults to new MQTTConfig().
            topics: Optional list of topics or (topic, qos) tuples to subscribe to after connect.
            message_handler: Optional callback invoked as fn(topic: str, payload: bytes) per message.
            client_id_suffix: Optional suffix appended to config.CLIENT_ID to avoid ID collisions.
        """
        self.config = config or MQTTConfig()
        # Avoid client ID collision with publisher by default
        client_id = self.config.CLIENT_ID + (client_id_suffix or "")
        self._requested_topics: List[Tuple[str, int]] = self._normalize_topics(topics) if topics else []
        self.client = mqtt.Client(client_id=client_id, clean_session=self.config.CLEAN_SESSION)
        self.is_connected = False
        self.message_count = 0
        self._external_handler = message_handler

        # Set authentication if provided
        if self.config.USERNAME and self.config.PASSWORD:
            self.client.username_pw_set(self.config.USERNAME, self.config.PASSWORD)

        # Configure callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_log = self._on_log

        if self.config.AUTO_RECONNECT:
            self.client.reconnect_delay_set(min_delay=1, max_delay=120)

    def _normalize_topics(self, topics: TopicList) -> List[Tuple[str, int]]:
        normalized: List[Tuple[str, int]] = []
        for item in topics:
            if isinstance(item, tuple):
                normalized.append((item[0], int(item[1]) if len(item) > 1 else self.config.QOS))
            else:
                normalized.append((str(item), self.config.QOS))
        return normalized

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.is_connected = True
            logger.info(f"Connected to MQTT broker at {self.config.BROKER_HOST}:{self.config.BROKER_PORT}")
            # Subscribe to any pre-configured topics
            if self._requested_topics:
                self.subscribe(self._requested_topics)
        else:
            self.is_connected = False
            logger.error(f"Failed to connect to MQTT broker. Return code: {rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.is_connected = False
        if rc != 0:
            logger.warning(f"Unexpected disconnection. Return code: {rc}")
        else:
            logger.info("Disconnected from MQTT broker")

    def _on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        self.message_count += 1
        try:
            logger.debug(f"Received message on {msg.topic}: {msg.payload}")
            if self._external_handler:
                self._external_handler(msg.topic, msg.payload)
        except Exception as exc:
            logger.error(f"Error in message handler: {exc}")

    def _on_log(self, client, userdata, level, buf):
        logger.debug(f"MQTT Log: {buf}")

    def connect(self) -> bool:
        try:
            logger.info(f"Connecting to MQTT broker at {self.config.BROKER_HOST}:{self.config.BROKER_PORT}")
            self.client.connect(self.config.BROKER_HOST, self.config.BROKER_PORT, self.config.KEEPALIVE)
            self.client.loop_start()

            timeout = 10
            start = time.time()
            while not self.is_connected and (time.time() - start) < timeout:
                time.sleep(0.1)
            return self.is_connected
        except Exception as exc:
            logger.error(f"Failed to connect to MQTT broker: {exc}")
            return False

    def disconnect(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
        finally:
            self.is_connected = False

    def subscribe(self, topics: TopicList):
        """Subscribe to topics. Accepts strings or (topic, qos) tuples, or an iterable of them."""
        topics_list = self._normalize_topics(topics)
        for topic, qos in topics_list:
            result, mid = self.client.subscribe(topic, qos)
            if result == mqtt.MQTT_ERR_SUCCESS:
                logger.info(f"Subscribed to topic: {topic} (QoS {qos})")
            else:
                logger.error(f"Failed to subscribe to {topic} (rc={result})")

    def get_stats(self) -> dict:
        return {
            "connected": self.is_connected,
            "message_count": self.message_count,
            "broker_host": self.config.BROKER_HOST,
            "broker_port": self.config.BROKER_PORT,
            "client_id": self.client._client_id.decode() if isinstance(self.client._client_id, (bytes, bytearray)) else self.client._client_id,
        }


