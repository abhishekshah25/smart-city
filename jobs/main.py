import os
import time
import uuid
import random
import simplejson as json
from datetime import datetime, timedelta
from confluent_kafka import SerializingProducer

# Coordinates extracted from internet
BENGALURU_COORDINATES = {
    "latitude": 12.9715,
    "longitude": 77.5945
}

CHENNAI_COORDINATES = {
    "latitude": 13.0674,
    "longitude": 80.2376
}

# Calculate Movement Increments
LATITUDE_INCREMENT = (CHENNAI_COORDINATES['latitude'] - BENGALURU_COORDINATES['latitude'])/100
LONGITUDE_INCREMENT = (CHENNAI_COORDINATES['longitude'] - BENGALURU_COORDINATES['longitude'])/100

# Environmental Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC','vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC','gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC','traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC','weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC','emergency_data')

random.seed(42)
start_time = datetime.now()
start_location = BENGALURU_COORDINATES.copy()

def generate_gps_data(vehicle_id, timestamp, vehicle_type = 'private'):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': timestamp,
        'speed': random.uniform(0,40),
        'direction': 'East',   # Static
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(vehicle_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def generate_weather_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(25,45),
        'weatherCondition': random.choice(['Sunny','Humid','Cloudy','Rain']),
        'precipitation': random.uniform(30,75),
        'windSpeed': random.uniform(0,50),
        'humidity': random.randint(45,100),
        'airQualityIndex': random.uniform(225,500)
    }

def generate_emergency_incident_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident','Medical','Police','None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active','Resolved']),
        'description': 'Description of the Incident'
    }

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60))
    return start_time

def simulate_vehicle_movement():
    global start_location

    # Move towards destination
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # Adding randomness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(15,45),
        'direction': 'East',  # Static
        'make': 'Mercedes',   # Static
        'model': 'Z-series',  # Static
        'year': 2024,         # Static
        'fuelType': 'Hybrid'  # Static
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON Serializable.')

def delivery_report(err,msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')   

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key = str(data['id']),
        value = json.dumps(data,default=json_serializer).encode('utf-8'),
        on_delivery = delivery_report
    )
    producer.flush()

def simulate_journey(producer, vehicle_id):
    while True:
        vehicle_data = generate_vehicle_data(vehicle_id)
        gps_data = generate_gps_data(vehicle_id,vehicle_data['timestamp'])
        traffic_data = generate_traffic_camera_data(vehicle_id,vehicle_data['timestamp'],vehicle_data['location'],'Nikon-Plus')
        weather_data = generate_weather_data(vehicle_id,vehicle_data['timestamp'],vehicle_data['location'])
        emergency_data = generate_emergency_incident_data(vehicle_id,vehicle_data['timestamp'],vehicle_data['location'])

        if(vehicle_data['location'][0] >= CHENNAI_COORDINATES['latitude'] and vehicle_data['location'][1] <= CHENNAI_COORDINATES['longitude']):
            print('Vehicle has reached Chennai. Simulation Ending...')
            break
        
        produce_data_to_kafka(producer,VEHICLE_TOPIC,vehicle_data)
        produce_data_to_kafka(producer,GPS_TOPIC,gps_data)
        produce_data_to_kafka(producer,TRAFFIC_TOPIC,traffic_data)
        produce_data_to_kafka(producer,WEATHER_TOPIC,weather_data)
        produce_data_to_kafka(producer,EMERGENCY_TOPIC,emergency_data)

        time.sleep(2)

if __name__ == "main":

    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka Error: {err}')
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer,'Vehicle-Smudge-49')
    except KeyboardInterrupt:
        print('Simulation ended by user!') 
    except Exception as e:
        print(f'Unexpected Error Occured: {e}')        
