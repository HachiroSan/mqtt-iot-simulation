import os
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

"""Load environment variables from a local .env file if present.

Order of precedence:
- ENV_FILE env var path, if set
- .env.dev in project root, if exists
- .env in project root, if exists
"""

# Attempt to load environment variables from a file for local dev
_env_file = os.getenv("ENV_FILE")
if _env_file:
    load_dotenv(_env_file)
else:
    # Prefer .env.dev for local development, fallback to .env
    if Path(".env.dev").exists():
        load_dotenv(".env.dev")
    elif Path(".env").exists():
        load_dotenv(".env")


class MQTTConfig:
    """MQTT Configuration settings"""
    
    # MQTT Broker settings
    BROKER_HOST = os.getenv('MQTT_BROKER_HOST', 'localhost')
    BROKER_PORT = int(os.getenv('MQTT_BROKER_PORT', 1883))
    CLIENT_ID = os.getenv('MQTT_CLIENT_ID', 'orca_iot_publisher')
    USERNAME = os.getenv('MQTT_USERNAME', None)
    PASSWORD = os.getenv('MQTT_PASSWORD', None)
    KEEPALIVE = int(os.getenv('MQTT_KEEPALIVE', 60))
    
    # Publisher settings
    PUBLISH_INTERVAL = int(os.getenv('PUBLISH_INTERVAL', 5))
    TOPIC_PREFIX = os.getenv('TOPIC_PREFIX', 'orca/iot')
    
    # QoS settings (0, 1, or 2)
    QOS = int(os.getenv('MQTT_QOS', os.getenv('QOS', 1)))
    
    # Connection settings
    CLEAN_SESSION = True
    AUTO_RECONNECT = True
