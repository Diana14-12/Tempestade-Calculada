# Alerta com treinamento de modelo de risco de cheia em tempo real treinado com GPU Intel
import numpy as np
import json
import requests
from kafka import KafkaConsumer
from openvino.runtime import Core

# Configuração do Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "weather_data"
consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda x: json.loads(x.decode("utf-8")))

# Carregar modelo otimizado OpenVINO
ie = Core()
model = ie.compile_model("flood_model_openvino.xml", "GPU")

# Função para processar e inferir dados
def infer_real_time():
    print("Aguardando dados em tempo real...")
    for message in consumer:
        data = message.value
        X_real_time = np.array([[  
            data["temperature"], data["humidity"], data["pressure"],  
            data["wind_speed"], data["rainfall"]  
        ]], dtype=np.float32)
        
        # Inferência
        flood_risk = model(X_real_time)[0][0]
        
        if flood_risk > 0.7:
            print(f"🚨 ALERTA! Risco de cheia elevado: {flood_risk:.2f}")
        else:
            print(f"✅ Risco baixo de cheia: {flood_risk:.2f}")

infer_real_time()
