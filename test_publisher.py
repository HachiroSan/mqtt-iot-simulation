#!/usr/bin/env python3
"""
Test script for MQTT Publisher

This script tests the MQTT publisher functionality without requiring
an actual MQTT broker connection.
"""

import unittest
from unittest.mock import Mock, patch
from mqtt_publisher import MQTTPublisher
from config import MQTTConfig

class TestMQTTPublisher(unittest.TestCase):
    """Test cases for MQTTPublisher class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.config = MQTTConfig()
        self.publisher = MQTTPublisher(self.config)
    
    def test_config_defaults(self):
        """Test configuration default values"""
        self.assertEqual(self.config.BROKER_HOST, 'localhost')
        self.assertEqual(self.config.BROKER_PORT, 1883)
        self.assertEqual(self.config.CLIENT_ID, 'orca_iot_publisher')
        self.assertEqual(self.config.QOS, 1)
    
    def test_publisher_initialization(self):
        """Test publisher initialization"""
        self.assertIsNotNone(self.publisher.client)
        self.assertFalse(self.publisher.is_connected)
        self.assertEqual(self.publisher.message_count, 0)
    
    def test_publish_sensor_data(self):
        """Test publishing sensor data"""
        # Mock the publish method
        with patch.object(self.publisher, 'publish') as mock_publish:
            mock_publish.return_value = True
            
            sensor_id = "test_sensor"
            data = {"temperature": 25.5, "humidity": 60.0}
            
            result = self.publisher.publish_sensor_data(sensor_id, data)
            
            self.assertTrue(result)
            mock_publish.assert_called_once()
            
            # Check the topic format
            call_args = mock_publish.call_args
            topic = call_args[0][0]
            self.assertEqual(topic, f"{self.config.TOPIC_PREFIX}/sensor/{sensor_id}")
    
    def test_publish_device_status(self):
        """Test publishing device status"""
        with patch.object(self.publisher, 'publish') as mock_publish:
            mock_publish.return_value = True
            
            device_id = "test_device"
            status = "online"
            metadata = {"version": "1.0.0"}
            
            result = self.publisher.publish_device_status(device_id, status, metadata)
            
            self.assertTrue(result)
            mock_publish.assert_called_once()
            
            # Check the topic format
            call_args = mock_publish.call_args
            topic = call_args[0][0]
            self.assertEqual(topic, f"{self.config.TOPIC_PREFIX}/device/{device_id}/status")
    
    def test_publish_alert(self):
        """Test publishing alerts"""
        with patch.object(self.publisher, 'publish') as mock_publish:
            mock_publish.return_value = True
            
            alert_type = "temperature_high"
            message = "Temperature exceeded threshold"
            severity = "warning"
            metadata = {"threshold": 30.0}
            
            result = self.publisher.publish_alert(alert_type, message, severity, metadata)
            
            self.assertTrue(result)
            mock_publish.assert_called_once()
            
            # Check the topic format
            call_args = mock_publish.call_args
            topic = call_args[0][0]
            self.assertEqual(topic, f"{self.config.TOPIC_PREFIX}/alerts/{alert_type}")
    
    def test_get_stats(self):
        """Test getting publisher statistics"""
        stats = self.publisher.get_stats()
        
        expected_keys = ['connected', 'message_count', 'broker_host', 'broker_port', 'client_id']
        for key in expected_keys:
            self.assertIn(key, stats)
        
        self.assertEqual(stats['connected'], False)
        self.assertEqual(stats['message_count'], 0)
        self.assertEqual(stats['broker_host'], 'localhost')
        self.assertEqual(stats['broker_port'], 1883)
        self.assertEqual(stats['client_id'], 'orca_iot_publisher')

def run_tests():
    """Run the test suite"""
    print("Running MQTT Publisher Tests...")
    print("=" * 40)
    
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMQTTPublisher)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 40)
    if result.wasSuccessful():
        print("✅ All tests passed!")
    else:
        print("❌ Some tests failed!")
    
    return result.wasSuccessful()

if __name__ == "__main__":
    run_tests()
