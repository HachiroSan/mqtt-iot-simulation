import json
import time
import logging
from typing import Any, Dict, Optional
import paho.mqtt.client as mqtt
from config import MQTTConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MQTTPublisher:
    """MQTT Publisher for IoT device simulation"""
    
    def __init__(self, config: MQTTConfig = None):
        """Initialize MQTT Publisher"""
        self.config = config or MQTTConfig()
        self.client = None
        self.is_connected = False
        self.message_count = 0
        
        # Setup MQTT client
        self._setup_client()
    
    def _setup_client(self):
        """Setup MQTT client with callbacks"""
        self.client = mqtt.Client(
            client_id=self.config.CLIENT_ID,
            clean_session=self.config.CLEAN_SESSION
        )
        
        # Set callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        self.client.on_log = self._on_log
        
        # Set authentication if provided
        if self.config.USERNAME and self.config.PASSWORD:
            self.client.username_pw_set(self.config.USERNAME, self.config.PASSWORD)
        
        # Enable automatic reconnection
        if self.config.AUTO_RECONNECT:
            self.client.reconnect_delay_set(min_delay=1, max_delay=120)
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker"""
        if rc == 0:
            self.is_connected = True
            logger.info(f"Connected to MQTT broker at {self.config.BROKER_HOST}:{self.config.BROKER_PORT}")
        else:
            self.is_connected = False
            logger.error(f"Failed to connect to MQTT broker. Return code: {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback when disconnected from MQTT broker"""
        self.is_connected = False
        if rc != 0:
            logger.warning(f"Unexpected disconnection. Return code: {rc}")
        else:
            logger.info("Disconnected from MQTT broker")
    
    def _on_publish(self, client, userdata, mid):
        """Callback when message is published"""
        logger.debug(f"Message published with message ID: {mid}")
    
    def _on_log(self, client, userdata, level, buf):
        """Callback for MQTT client logs"""
        logger.debug(f"MQTT Log: {buf}")
    
    def connect(self) -> bool:
        """Connect to MQTT broker"""
        try:
            logger.info(f"Connecting to MQTT broker at {self.config.BROKER_HOST}:{self.config.BROKER_PORT}")
            self.client.connect(
                self.config.BROKER_HOST,
                self.config.BROKER_PORT,
                self.config.KEEPALIVE
            )
            
            # Start the loop in a non-blocking way
            self.client.loop_start()
            
            # Wait for connection
            timeout = 10
            start_time = time.time()
            while not self.is_connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            
            return self.is_connected
            
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from MQTT broker"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            self.is_connected = False
            logger.info("Disconnected from MQTT broker")
    
    def publish(self, topic: str, payload: Any, qos: int = None) -> bool:
        """Publish message to MQTT topic"""
        if not self.is_connected:
            logger.error("Not connected to MQTT broker")
            return False
        
        try:
            # Convert payload to JSON if it's a dict
            if isinstance(payload, dict):
                payload = json.dumps(payload)
            elif not isinstance(payload, (str, bytes)):
                payload = str(payload)
            
            # Use config QoS if not specified
            qos = qos if qos is not None else self.config.QOS
            
            # Publish message
            result = self.client.publish(topic, payload, qos)
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.message_count += 1
                logger.info(f"Published message #{self.message_count} to topic: {topic}")
                return True
            else:
                logger.error(f"Failed to publish message. Return code: {result.rc}")
                return False
                
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            return False
    
    def publish_sensor_data(self, sensor_id: str, data: Dict[str, Any], qos: int = None) -> bool:
        """Publish sensor data to MQTT topic"""
        topic = f"{self.config.TOPIC_PREFIX}/sensor/{sensor_id}"
        
        # Add timestamp to data
        message = {
            "timestamp": int(time.time()),
            "sensor_id": sensor_id,
            "data": data
        }
        
        return self.publish(topic, message, qos)
    
    def publish_device_status(self, device_id: str, status: str, metadata: Dict[str, Any] = None, qos: int = None) -> bool:
        """Publish device status to MQTT topic"""
        topic = f"{self.config.TOPIC_PREFIX}/device/{device_id}/status"
        
        message = {
            "timestamp": int(time.time()),
            "device_id": device_id,
            "status": status
        }
        
        if metadata:
            message["metadata"] = metadata
        
        return self.publish(topic, message, qos)
    
    def publish_alert(self, alert_type: str, message: str, severity: str = "info", metadata: Dict[str, Any] = None, qos: int = None) -> bool:
        """Publish alert to MQTT topic"""
        topic = f"{self.config.TOPIC_PREFIX}/alerts/{alert_type}"
        
        alert_message = {
            "timestamp": int(time.time()),
            "type": alert_type,
            "message": message,
            "severity": severity
        }
        
        if metadata:
            alert_message["metadata"] = metadata
        
        return self.publish(topic, alert_message, qos)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get publisher statistics"""
        return {
            "connected": self.is_connected,
            "message_count": self.message_count,
            "broker_host": self.config.BROKER_HOST,
            "broker_port": self.config.BROKER_PORT,
            "client_id": self.config.CLIENT_ID
        }
