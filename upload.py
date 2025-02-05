import serial
import requests
import csv
import time
import json
from datetime import datetime
from tabulate import tabulate
from confluent_kafka import Producer

# Configurações do Serial
serial_port = 'COM7'  # Modifique conforme necessário
baud_rate = 9600

# Configurações das APIs
wunderground_station_id = "IPVOAD5"
wunderground_api_key = "a99d276be0384c889d276be0386c8848"
wunderground_url = "https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php"

pwsweather_station_id = 'ROBOTICAEAVEROMAR'
pwsweather_password = '7df56fe4398995e8049a9ab6215983fe'

php_script_url = "http://aeaveromar.pt/meteo/receive_weather_data.php"

# Configuração do Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "weather_data"
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def kafka_delivery_report(err, msg):
    if err is not None:
        print(f"Erro ao enviar mensagem para Kafka: {err}")
    else:
        print(f"Mensagem enviada para {msg.topic()} [{msg.partition()}]")

def send_to_kafka(data):
    message = json.dumps(data)
    producer.produce(KAFKA_TOPIC, key=str(datetime.utcnow().timestamp()), value=message, callback=kafka_delivery_report)
    producer.flush()

# Inicializa comunicação serial
ser = serial.Serial(serial_port, baud_rate, timeout=1)

# Abre CSV
csv_file = open('data.csv', 'w', newline='')
csv_writer = csv.writer(csv_file)
csv_writer.writerow(['Timestamp', 'Temperature (C)', 'Humidity (%)', 'Pressure', 'Wind Speed (m/s)', 'Wind Gust (m/s)', 'Dew Point (C)', 'Sensor WorkingTime (H)', 'Rainfall (mm)', '1 Hour Rainfall (mm)', 'Raw Rainfall'])

def read_sensor_data():
    data = {}
    while len(data) < 10:
        line = ser.readline().decode().strip()
        if ":" in line:
            key, value = line.split(":", 1)
            try:
                data[key.strip()] = float(value.split()[0])
            except ValueError:
                continue
    return data

def send_data_to_services(data):
    date_utc = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    data['timestamp'] = date_utc
    send_to_kafka(data)
    
    # Envio para Wunderground
    wunderground_payload = {
        "ID": wunderground_station_id,
        "PASSWORD": wunderground_api_key,
        "dateutc": "now",
        "tempf": data['Temperature'] * 1.8 + 32,
        "humidity": data['Humidity'],
        "baromin": data['Pressure'] * 0.0295300,
        "windspeedmph": abs(data['WindSpeed'] * 0.6213711922),
        "windgustmph": abs(data['WindGust'] * 0.6213711922),
        "dewptf": data['DewPoint'] * 1.8 + 32,
    }
    requests.post(wunderground_url, data=wunderground_payload)
    
    # Envio para PHP
    php_payload = {
        "ID": pwsweather_station_id,
        "tempf": data['Temperature'] * 1.8 + 32,
        "humidity": data['Humidity'],
        "baromin": data['Pressure'] * 0.0295300,
        "windspeedmph": abs(data['WindSpeed'] * 0.6213711922),
        "windgustmph": abs(data['WindGust'] * 0.6213711922),
        "dewptf": data['DewPoint'] * 1.8 + 32,
        "rainin": data['1 Hour Rainfall'] * 0.0393701,
        "dateutc": date_utc
    }
    requests.get(php_script_url, params=php_payload)

try:
    while True:
        try:
            sensor_data = read_sensor_data()
            if len(sensor_data) < 10:
                print("Dados incompletos lidos do sensor, ignorando...")
                continue
            
            send_data_to_services(sensor_data)
            
            # Escrever no CSV
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            csv_writer.writerow([timestamp] + list(sensor_data.values()))
            print(tabulate([[timestamp] + list(sensor_data.values())], headers=['Timestamp', 'Temperature (C)', 'Humidity (%)', 'Pressure', 'Wind Speed (m/s)', 'Wind Gust (m/s)', 'Dew Point (C)', 'Sensor WorkingTime (H)', 'Rainfall (mm)', '1 Hour Rainfall (mm)', 'Raw Rainfall']))
            
            time.sleep(30)
        
        except Exception as e:
            print(f"Erro: {e}")
            time.sleep(5)
            continue

except KeyboardInterrupt:
    print("\nEncerrando programa...")
    ser.close()
    csv_file.close()
    producer.flush()
    exit(0)
