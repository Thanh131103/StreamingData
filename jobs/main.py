import os
from confluent_kafka import SerializingProducer
import random
import simplejson as json
from datetime import datetime
from datetime import timedelta
import time
import uuid
LONDON_COORDINATES = {"latitude": 51.5074, "longitude":-0.1278}
BIRMINGHAM_COORDINATES= {"latitude":52.4863, "longitude":-1.8904}

#CALCULATE MOVEMENT
LATITUDE_INCREMENT=(BIRMINGHAM_COORDINATES['latitude']-LONDON_COORDINATES['latitude'])/1000
LONGITUDE_INCREMENT=(BIRMINGHAM_COORDINATES['longitude']-LONDON_COORDINATES['longitude'])/1000

KAFKA_BOOTSTRAP_SERVERS= os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC','vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC','gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC','traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC','weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC','emergency_data')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()
def get_next_time():
    global start_time
    start_time+= timedelta(seconds=random.randint(30,40))
    return start_time
def simulate_vehicle_movement():
    global start_location

    start_location['latitude']+=LATITUDE_INCREMENT
    start_location['longitude']+=LONGITUDE_INCREMENT

    start_location['latitude']+=random.uniform(-0.0005,0.0005)
    start_location['longitude']+=random.uniform(-0.0005,0.0005)
    return start_location
def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': vehicle_id,
        'timestamp' : get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10,40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': '2024',
        'fluType': 'Hybird'
    }
def generate_weather_data(vehicle_id,timestamp,location):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': timestamp,
        'location': location,
        'tempertature': random.uniform(10,30),
        'weatherCondition': random.choice(['Sunny','Cloudy','Rainy']),
        'precipitation': random.uniform(0,25),
        'windSpeed': random.uniform(0,100),
        'humidity': random.uniform(0,100),
        'airQuality': random.uniform(0,500)
    }
def generate_gps_data(vehicle_id,timestamp,vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'timestamp': timestamp,
        'speed': random.uniform(10,40),
        'direction': 'North-East',
        'Vehicle_type': vehicle_type
    }
def generate_traffic_camera_data(vehicle_id,timestamp,location,camera_id):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'location': location,
        'camera_id': camera_id,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodingString'
    }
def generate_emergency_incident_data(vehicle_id,timestamp,location):
    return {
        'id': uuid.uuid4(),
        'vehicle_id': vehicle_id,
        'location': location,
        'incident_id': uuid.uuid4(),
        'type': random.choice(['Accident','Fire','Medical','Police','None']),
        'timestamp': timestamp,
        'status': random.choice(['Active','Resolved']),
        'description': 'Description of accident'
    }
def json_serializer(obj):
    if isinstance(obj,uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')
def send_report(err,msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
def produce_data_to_kafka(producer,topic,data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data,default=json_serializer).encode('utf-8'),
        on_delivery= send_report
    )
    producer.flush()
def simulate_journey(producer,vehicle_id):
    while True:
        vehicle_data=generate_vehicle_data(vehicle_id)
        gps_data= generate_gps_data(vehicle_id,vehicle_data['timestamp'])
        traffic_camera_data= generate_traffic_camera_data(vehicle_id,vehicle_data['timestamp'],vehicle_data['location'],'Nikon_cam_123')
        weather_data= generate_weather_data(vehicle_id,vehicle_data['timestamp'],vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(vehicle_id,vehicle_data['timestamp'],vehicle_data['location'])
        if(vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude'] 
           and vehicle_data['location'][1] <= BIRMINGHAM_COORDINATES['longitude']):
            print('End of simulation')
            break
        produce_data_to_kafka(producer,VEHICLE_TOPIC,vehicle_data)
        produce_data_to_kafka(producer,GPS_TOPIC,gps_data)
        produce_data_to_kafka(producer,TRAFFIC_TOPIC,traffic_camera_data)
        produce_data_to_kafka(producer,WEATHER_TOPIC,weather_data)
        produce_data_to_kafka(producer,EMERGENCY_TOPIC,emergency_incident_data)
        time.sleep(5)

if __name__ == '__main__':
    producer_config = {
        'bootstrap.servers':KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda error: print(f'Kafka error: {error}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-Thanh-123')
    except KeyboardInterrupt:
        print('User ended the simulation')
    except Exception as e:
        print(f'Unexpected error occurred: {e}')