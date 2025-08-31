#!/usr/bin/env python3
"""
IoT Device Monitoring Kafka Producer for Shift-Left Architecture Demo
Generates realistic IoT sensor data with various quality scenarios
"""

import sys
import json
import random
import time
import uuid
import math
import argparse
from datetime import datetime, timedelta

from confluent_kafka import Producer
from confluent_kafka import KafkaError, KafkaException

class IoTDeviceSimulator:
    """Simulates different types of IoT devices and their telemetry data"""
    
    def __init__(self):
        # Device configurations
        self.device_types = {
            "temperature_sensor": {
                "metrics": ["temperature", "humidity"],
                "normal_ranges": {
                    "temperature": (18.0, 25.0),  # Celsius
                    "humidity": (30.0, 60.0)       # Percentage
                }
            },
            "pressure_sensor": {
                "metrics": ["pressure", "temperature"],
                "normal_ranges": {
                    "pressure": (1000.0, 1020.0),  # hPa
                    "temperature": (15.0, 30.0)
                }
            },
            "vibration_sensor": {
                "metrics": ["vibration_x", "vibration_y", "vibration_z", "temperature"],
                "normal_ranges": {
                    "vibration_x": (-2.0, 2.0),    # g-force
                    "vibration_y": (-2.0, 2.0),
                    "vibration_z": (-2.0, 2.0),
                    "temperature": (20.0, 40.0)
                }
            },
            "energy_meter": {
                "metrics": ["voltage", "current", "power", "power_factor"],
                "normal_ranges": {
                    "voltage": (220.0, 240.0),      # Volts
                    "current": (0.0, 30.0),         # Amperes
                    "power": (0.0, 7000.0),         # Watts
                    "power_factor": (0.85, 0.99)   # ratio
                }
            }
        }
        
        # Initialize device fleet
        self.devices = self._initialize_devices()
        
        # Anomaly patterns for testing
        self.anomaly_patterns = {
            "spike": lambda v, range: v * random.uniform(2, 5),
            "drop": lambda v, range: v * random.uniform(0.1, 0.3),
            "drift": lambda v, range: v + (range[1] - range[0]) * 0.5,
            "noise": lambda v, range: v + random.uniform(-range[1], range[1]),
            "stuck": lambda v, range: v  # Same value repeatedly
        }
    
    def _initialize_devices(self, count=50):
        """Initialize a fleet of IoT devices"""
        devices = []
        for i in range(count):
            device_type = random.choice(list(self.device_types.keys()))
            device = {
                "device_id": f"{device_type}_{uuid.uuid4().hex[:8]}",
                "device_type": device_type,
                "location": {
                    "building": f"Building-{random.randint(1, 5)}",
                    "floor": random.randint(1, 10),
                    "zone": random.choice(["A", "B", "C", "D"])
                },
                "manufacturer": random.choice(["SensorCorp", "IoTech", "SmartDevices"]),
                "firmware_version": random.choice(["1.0.0", "1.1.0", "2.0.0", "2.1.0"]),
                "last_maintenance": datetime.now() - timedelta(days=random.randint(0, 365)),
                "status": "active",
                "anomaly_state": None  # Track if device is in anomaly state
            }
            devices.append(device)
        return devices
    
    def generate_valid_telemetry(self, device):
        """Generate valid telemetry data for a device"""
        device_config = self.device_types[device["device_type"]]
        
        # Generate readings based on sine waves for realistic patterns
        time_factor = time.time() / 100  # Slow oscillation
        
        readings = {}
        for metric in device_config["metrics"]:
            normal_range = device_config["normal_ranges"][metric]
            
            # Add realistic variation using sine waves and random noise
            base_value = (normal_range[0] + normal_range[1]) / 2
            amplitude = (normal_range[1] - normal_range[0]) / 4
            
            value = base_value + amplitude * math.sin(time_factor + hash(device["device_id"]) % 100)
            value += random.uniform(-amplitude * 0.1, amplitude * 0.1)  # Add noise
            
            readings[metric] = round(value, 2)
        
        return {
            "event_id": str(uuid.uuid4()),
            "device_id": device["device_id"],
            "device_type": device["device_type"],
            "timestamp": int(time.time() * 1000),  # milliseconds
            "readings": readings,
            "metadata": {
                "location": device["location"],
                "firmware_version": device["firmware_version"],
                "signal_strength": random.randint(60, 100),  # RSSI
                "battery_level": None if "meter" in device["device_type"] else random.randint(20, 100)
            }
        }
    
    def generate_invalid_schema_telemetry(self, device):
        """Generate telemetry with schema violations"""
        invalid_types = [
            # Missing required fields
            {
                "device_id": device["device_id"],
                "timestamp": int(time.time() * 1000),
                "readings": {"temperature": 25.0}
                # Missing event_id and device_type
            },
            # Wrong data types
            {
                "event_id": str(uuid.uuid4()),
                "device_id": device["device_id"],
                "device_type": device["device_type"],
                "timestamp": "not-a-timestamp",  # Should be integer
                "readings": "not-a-dict"  # Should be dict
            },
            # Malformed structure
            {
                "event_id": str(uuid.uuid4()),
                "device_id": device["device_id"],
                "device_type": device["device_type"],
                "timestamp": int(time.time() * 1000),
                "temperature": 25.0  # Should be inside readings
            }
        ]
        return random.choice(invalid_types)
    
    def generate_anomaly_telemetry(self, device):
        """Generate telemetry with anomalies (valid schema but abnormal values)"""
        telemetry = self.generate_valid_telemetry(device)
        device_config = self.device_types[device["device_type"]]
        
        # Select metrics to make anomalous
        anomaly_metrics = random.sample(
            device_config["metrics"], 
            random.randint(1, len(device_config["metrics"]))
        )
        
        # Apply anomaly pattern
        anomaly_type = random.choice(list(self.anomaly_patterns.keys()))
        
        for metric in anomaly_metrics:
            normal_range = device_config["normal_ranges"][metric]
            normal_value = telemetry["readings"][metric]
            
            if anomaly_type == "spike" or anomaly_type == "drop":
                # Out of range values
                telemetry["readings"][metric] = round(
                    self.anomaly_patterns[anomaly_type](normal_value, normal_range), 2
                )
            elif anomaly_type == "stuck":
                # Device stuck at same value
                telemetry["readings"][metric] = round(normal_range[0], 2)
        
        # Add anomaly metadata
        telemetry["metadata"]["anomaly_detected"] = True
        telemetry["metadata"]["anomaly_type"] = anomaly_type
        
        return telemetry
    
    def generate_edge_case_telemetry(self, device):
        """Generate edge cases like offline devices, missing readings, etc."""
        edge_cases = [
            # Device coming back online after outage
            lambda d: {
                "event_id": str(uuid.uuid4()),
                "device_id": d["device_id"],
                "device_type": d["device_type"],
                "timestamp": int((time.time() - 3600) * 1000),  # 1 hour old
                "readings": {},  # No readings
                "metadata": {
                    "status": "reconnected",
                    "downtime_seconds": 3600
                }
            },
            # Partial readings (some sensors failed)
            lambda d: {
                "event_id": str(uuid.uuid4()),
                "device_id": d["device_id"],
                "device_type": d["device_type"],
                "timestamp": int(time.time() * 1000),
                "readings": {
                    metric: None 
                    for metric in self.device_types[d["device_type"]]["metrics"][:1]
                },
                "metadata": {
                    "error": "sensor_failure",
                    "failed_sensors": self.device_types[d["device_type"]]["metrics"][1:]
                }
            },
            # Future timestamp (clock sync issue)
            lambda d: {
                **self.generate_valid_telemetry(d),
                "timestamp": int((time.time() + 3600) * 1000)  # 1 hour in future
            }
        ]
        
        return random.choice(edge_cases)(device)
    
    def generate_telemetry(self, scenario="mixed"):
        """Generate telemetry based on scenario"""
        device = random.choice(self.devices)
        
        if scenario == "valid":
            return self.generate_valid_telemetry(device)
        elif scenario == "invalid_schema":
            return self.generate_invalid_schema_telemetry(device)
        elif scenario == "anomaly":
            return self.generate_anomaly_telemetry(device)
        elif scenario == "edge_case":
            return self.generate_edge_case_telemetry(device)
        elif scenario == "mixed":
            # 70% valid, 15% anomalies, 10% edge cases, 5% invalid schema
            rand = random.random()
            if rand < 0.7:
                return self.generate_valid_telemetry(device)
            elif rand < 0.85:
                return self.generate_anomaly_telemetry(device)
            elif rand < 0.95:
                return self.generate_edge_case_telemetry(device)
            else:
                return self.generate_invalid_schema_telemetry(device)

def main():
    parser = argparse.ArgumentParser(description='IoT Kafka Producer for Shift-Left Demo')
    parser.add_argument('--bootstrap-servers', default='localhost:9092',
                        help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', default='iot-telemetry',
                        help='Kafka topic to produce to (default: iot-telemetry)')
    parser.add_argument('--scenario', 
                        choices=['valid', 'invalid_schema', 'anomaly', 'edge_case', 'mixed'],
                        default='mixed', help='Type of telemetry to generate')
    parser.add_argument('--rate', type=int, default=10,
                        help='Messages per second (default: 10)')
    parser.add_argument('--duration', type=int, default=0,
                        help='Duration in seconds (0 for infinite, default: 0)')
    
    args = parser.parse_args()
    
    # Initialize Kafka producer
    producer_config = {
        'bootstrap.servers': args.bootstrap_servers,
        'acks': 'all',
        'retries': 3,
        'max.in.flight.requests.per.connection': 1,
        'enable.idempotence': True
    }
    producer = Producer(producer_config)
    
    def delivery_report(err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            pass  # Success, handled in main loop
    
    simulator = IoTDeviceSimulator()
    messages_sent = 0
    errors = 0
    start_time = time.time()
    
    print(f"Starting IoT Kafka producer...")
    print(f"Bootstrap servers: {args.bootstrap_servers}")
    print(f"Topic: {args.topic}")
    print(f"Scenario: {args.scenario}")
    print(f"Rate: {args.rate} messages/second")
    print(f"Simulating {len(simulator.devices)} IoT devices")
    print(f"Press Ctrl+C to stop\n")
    
    try:
        while True:
            # Check duration
            if args.duration > 0 and (time.time() - start_time) > args.duration:
                break
            
            # Generate and send telemetry
            telemetry = simulator.generate_telemetry(args.scenario)
            
            # Use device_id as key for partition assignment
            key = telemetry.get('device_id', str(uuid.uuid4()))
            
            try:
                # Serialize the data
                key_bytes = key.encode('utf-8') if key else None
                value_bytes = json.dumps(telemetry).encode('utf-8')
                
                # Produce message
                producer.produce(
                    topic=args.topic,
                    key=key_bytes,
                    value=value_bytes,
                    callback=delivery_report
                )
                
                # Trigger delivery reports
                producer.poll(0)
                
                messages_sent += 1
                
                # Print sample messages
                if messages_sent % 50 == 1:
                    print(f"\nSent message {messages_sent}")
                    print(f"Sample: {json.dumps(telemetry, indent=2)}")
                    
            except Exception as e:
                errors += 1
                print(f"Error sending message: {e}")
            
            # Control rate
            time.sleep(1.0 / args.rate)
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        # Wait for any outstanding messages to be delivered
        print("\nFlushing outstanding messages...")
        producer.flush(30)  # 30 seconds timeout
        
        duration = time.time() - start_time
        print(f"\nProducer stopped.")
        print(f"Total messages sent: {messages_sent}")
        print(f"Errors: {errors}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Actual rate: {messages_sent/duration:.2f} messages/second")

if __name__ == "__main__":
    main()