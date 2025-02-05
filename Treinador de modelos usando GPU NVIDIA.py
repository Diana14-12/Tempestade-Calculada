#Treinador de modelos usando GPU NVIDIA

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import numpy as np

# Verifica se CUDA está disponível (para GPUs NVIDIA)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")

# Simulação de dados de entrada (substitua com os seus dados reais)
def generate_data(samples=10000):
    X = np.random.rand(samples, 10).astype(np.float32)  # 10 features (exemplo)
    y = (X.sum(axis=1) > 5).astype(np.float32)  # Simulação de risco de cheia
    return torch.tensor(X), torch.tensor(y)

# Carregar os dados
X, y = generate_data()
dataset = TensorDataset(X, y)
dataloader = DataLoader(dataset, batch_size=64, shuffle=True)

# Definição do modelo de rede neural
class FloodPredictionNN(nn.Module):
    def __init__(self):
        super(FloodPredictionNN, self).__init__()
        self.layers = nn.Sequential(
            nn.Linear(10, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x):
        return self.layers(x)

# Inicializa modelo e move para GPU se disponível
model = FloodPredictionNN().to(device)

# Configuração do otimizador e função de perda
criterion = nn.BCELoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Treinamento do modelo
num_epochs = 10
for epoch in range(num_epochs):
    for batch in dataloader:
        inputs, labels = batch
        inputs, labels = inputs.to(device), labels.to(device)
        
        optimizer.zero_grad()
        outputs = model(inputs).squeeze()
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()
    
    print(f"Epoch {epoch+1}/{num_epochs}, Loss: {loss.item():.4f}")

# Salvando modelo treinado
torch.save(model.state_dict(), "flood_prediction_model.pth")
print("Modelo salvo com sucesso!")
