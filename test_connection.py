#!/usr/bin/env python3
"""
Test script to verify connection to an MQTT broker.
"""

import time
import logging
from mqtt_publisher import MQTTPublisher
from config import MQTTConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_mqtt_connection():
    """Continuously publish test messages to verify broker connection"""
    logger.info("Testing MQTT connection to configured broker...")

    # Create publisher with default config
    publisher = MQTTPublisher()

    try:
        # Attempt to connect
        logger.info(f"Connecting to {publisher.config.BROKER_HOST}:{publisher.config.BROKER_PORT}")
        logger.info(f"Using credentials: {publisher.config.USERNAME}")

        if not publisher.connect():
            logger.error("❌ Failed to connect to MQTT broker")
            return False

        logger.info("✅ Successfully connected to MQTT broker!")

        # Continuous publish loop
        test_topic = f"{publisher.config.TOPIC_PREFIX}/test/connection"
        interval = publisher.config.PUBLISH_INTERVAL
        logger.info(f"Starting continuous publish every {interval}s to: {test_topic}")
        logger.info("Press Ctrl+C to stop...")

        count = 0
        while True:
            count += 1
            test_payload = {
                "message": "Connection test",
                "sequence": count,
                "timestamp": int(time.time()),
                "broker": f"{publisher.config.BROKER_HOST}:{publisher.config.BROKER_PORT}"
            }

            success = publisher.publish(test_topic, test_payload, publisher.config.QOS)
            if success:
                logger.info(f"✅ Published test message #{count}")
            else:
                logger.warning("⚠️ Test message publish failed (will retry)")

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Stopping...")
    except Exception as e:
        logger.error(f"❌ Error during connection test: {e}")
        return False
    finally:
        # Disconnect
        try:
            publisher.disconnect()
        except Exception:
            pass
        logger.info("Disconnected from MQTT broker")

    return True

if __name__ == "__main__":
    logger.info("Starting MQTT connection test...")
    logger.info("=" * 50)
    
    success = test_mqtt_connection()
    
    logger.info("=" * 50)
    if success:
        logger.info("🎉 MQTT connection test completed successfully!")
    else:
        logger.error("💥 MQTT connection test failed!")
    
    logger.info("If your broker has a web UI, check it for incoming messages.")
