
# Este script lê dados de um sensor de estação meteorológica conectado a uma porta serial e envia esses dados para um tópico Kafka, para um servidor PHP e para a API do Wunderground.
#
# pip install serial requests confluent-kafka tabulate
# Start Kafka Broker: confluent local services start

# bin/zookeeper-server-start.sh config/zookeeper.properties &
# bin/kafka-server-start.sh config/server.properties &
# bin/kafka-topics.sh --create --topic weather_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
# bin/kafka-console-consumer.sh --topic weather_data --from-beginning --bootstrap-server localhost:9092
#
# Além disso utiliza o dataset do IPMA PT2.nc para prever o risco de cheias.
# Bem como os dados das marés da API do NOAA.


import json
import time
import netCDF4 as nc
import pandas as pd
import requests
import numpy as np
from confluent_kafka import Consumer, KafkaException
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

# Configuração do Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "weather_data"
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'flood_predictor',
    'auto.offset.reset': 'latest'
})
consumer.subscribe([KAFKA_TOPIC])

# Carregar dados do PT02 do IPMA
pt02_dataset = nc.Dataset("PT02.nc")
pt02_precip = pt02_dataset.variables['precipitation'][:]
pt02_temp = pt02_dataset.variables['temperature'][:]
pt02_time = nc.num2date(pt02_dataset.variables['time'][:], pt02_dataset.variables['time'].units)

data_ipma = pd.DataFrame({
    'timestamp': pt02_time,
    'precipitation': pt02_precip,
    'temperature': pt02_temp
})

# Função para obter dados da maré
TIDE_API_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
def get_tide_data():
    params = {
        "station": "PovoaDeVarzim",
        "product": "water_level",
        "datum": "MSL",
        "units": "metric",
        "time_zone": "gmt",
        "format": "json"
    }
    response = requests.get(TIDE_API_URL, params=params)
    if response.status_code == 200:
        return response.json()['data'][-1]['v']
    return None

# Modelo de previsão
model = Pipeline([
    ('scaler', StandardScaler()),
    ('classifier', RandomForestClassifier(n_estimators=100, random_state=42))
])

# Criar dados de treino
X = data_ipma[['temperature', 'precipitation']]
y = np.random.choice([0, 1], size=len(X))  # Simulando valores binários de cheia
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model.fit(X_train, y_train)

def process_kafka_message():
    msg = consumer.poll(1.0)
    if msg is None:
        return None
    if msg.error():
        if msg.error().code() == KafkaException._PARTITION_EOF:
            return None
        else:
            print(f"Erro no Kafka: {msg.error()}")
            return None
    return json.loads(msg.value().decode('utf-8'))

while True:
    try:
        weather_data = process_kafka_message()
        if weather_data:
            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            temp = weather_data['temperature']
            precip = weather_data['rainfall']
            tide_level = get_tide_data()
            
            if tide_level is not None:
                X_input = pd.DataFrame([[temp, precip]], columns=['temperature', 'precipitation'])
                flood_risk = model.predict(X_input)[0]
                
                print(f"{timestamp} - Temperatura: {temp}°C, Precipitação: {precip}mm, Maré: {tide_level}m")
                print(f"⚠️ Risco de cheia: {'ALTO' if flood_risk else 'BAIXO'}")
                
            time.sleep(30)  # Atualiza a cada 30 segundos
    except KeyboardInterrupt:
        print("Interrompido pelo utilizador.")
        consumer.close()
        break
