#codigo para alertas em tempo real com modelo treinado por GPU NVIDIA

import torch
import torch.nn as nn
import torch.optim as optim
import json
from confluent_kafka import Consumer

# Configuração do Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "weather_data"

# Configuração do consumidor Kafka
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'flood_alerts',
    'auto.offset.reset': 'latest'
})
consumer.subscribe([KAFKA_TOPIC])

# Carregar modelo treinado
MODEL_PATH = "flood_prediction_model.pth"
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

class FloodPredictor(nn.Module):
    def __init__(self, input_dim):
        super(FloodPredictor, self).__init__()
        self.fc1 = nn.Linear(input_dim, 64)
        self.fc2 = nn.Linear(64, 32)
        self.fc3 = nn.Linear(32, 1)
        self.relu = nn.ReLU()
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.sigmoid(self.fc3(x))
        return x

# Supondo que o modelo foi treinado com 10 variáveis de entrada
INPUT_DIM = 10
model = FloodPredictor(INPUT_DIM).to(device)
model.load_state_dict(torch.load(MODEL_PATH, map_location=device))
model.eval()

# Limite para alerta de cheia
FLOOD_THRESHOLD = 0.7  # Ajuste conforme necessário

def process_message(msg):
    try:
        data = json.loads(msg.value().decode('utf-8'))
        features = torch.tensor([
            data['temperature'],
            data['humidity'],
            data['pressure'],
            data['wind_speed'],
            data['wind_gust'],
            data['dew_point'],
            data['rainfall'],
            data['hourly_rainfall'],
            data['tide_level'],
            data['sensor_working_time']
        ], dtype=torch.float32).to(device)

        with torch.no_grad():
            prediction = model(features.unsqueeze(0)).item()

        if prediction >= FLOOD_THRESHOLD:
            print(f"⚠️ ALERTA: Alta probabilidade de cheia ({prediction * 100:.2f}%)")
        else:
            print(f"✅ Status seguro: {prediction * 100:.2f}% de probabilidade de cheia")
    
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")

# Loop de consumo de dados em tempo real
print("📡 Monitorando dados em tempo real...")
while True:
    msg = consumer.poll(1.0)  # Aguarda nova mensagem
    if msg is None:
        continue
    if msg.error():
        print(f"Erro no Kafka: {msg.error()}")
        continue
    process_message(msg)
