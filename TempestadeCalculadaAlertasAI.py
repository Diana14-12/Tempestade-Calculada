import json
import requests
import netCDF4 as nc
import pandas as pd
import numpy as np
from datetime import datetime
from confluent_kafka import Consumer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# Configuração do Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "weather_data"
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'flood_prediction',
    'auto.offset.reset': 'latest'
})
consumer.subscribe([KAFKA_TOPIC])

# APIs externas
WUNDERGROUND_URL = "https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php"
PWSWEATHER_URL = "https://pwsupdate.pwsweather.com/api/v1/submitwx"
TIDE_API_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

# Carregar dados históricos do IPMA (PT02 Dataset)
PT02_FILE = "PT02.nc"
data = nc.Dataset(PT02_FILE)
precipitation = data.variables['precipitation'][:]
temperature = data.variables['temperature'][:]

def get_tide_data():
    params = {
        'station': 'Póvoa de Varzim',
        'product': 'water_level',
        'datum': 'MLLW',
        'units': 'metric',
        'time_zone': 'gmt',
        'format': 'json'
    }
    response = requests.get(TIDE_API_URL, params=params)
    return response.json() if response.status_code == 200 else None

def get_weather_data(api_url, params):
    response = requests.get(api_url, params=params)
    return response.json() if response.status_code == 200 else None

def preprocess_data():
    df = pd.DataFrame({'temperature': temperature, 'precipitation': precipitation})
    df.dropna(inplace=True)
    X = df[['temperature', 'precipitation']]
    y = (df['precipitation'] > 50).astype(int)  # Definir cheia como > 50mm de precipitação
    return train_test_split(X, y, test_size=0.2, random_state=42)

def train_model():
    X_train, X_test, y_train, y_test = preprocess_data()
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    print(f"Acurácia do modelo: {accuracy_score(y_test, predictions) * 100:.2f}%")
    return model

def process_kafka_messages(model):
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Erro no Kafka: {msg.error()}")
            continue
        
        data = json.loads(msg.value().decode('utf-8'))
        temperature = data.get("temperature", 0)
        precipitation = data.get("rainfall", 0)
        location = data.get("coordinates", "Desconhecido")
        
        tide_data = get_tide_data()
        tide_level = tide_data['data'][0]['v'] if tide_data else 0
        
        prediction = model.predict([[temperature, precipitation]])[0]
        risk = "ALTO" if prediction == 1 and tide_level > 1.5 else "BAIXO"
        
        print(f"[ {datetime.now()} ] Estação: {location} | Risco de cheia: {risk}")

if __name__ == "__main__":
    model = train_model()
    process_kafka_messages(model)
