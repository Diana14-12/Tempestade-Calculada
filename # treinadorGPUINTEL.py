# pip install tensorflow openvino-dev pandas kafka-python netCDF4 requests
# Treinador GPU Intel
import tensorflow as tf
import numpy as np
import pandas as pd
import netCDF4 as nc
import requests
from openvino.tools.mo import convert_model

# Configuração do dataset PT02 (NetCDF)
PT02_FILE = "PT02.nc"
pt02_data = nc.Dataset(PT02_FILE)

# Carregar dados de nível de marés (exemplo com API pública)
TIDE_API_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?station=12345&product=predictions&datum=MLLW&interval=h&units=metric&time_zone=gmt&format=json"
tide_data = requests.get(TIDE_API_URL).json()

# Pré-processamento dos dados
def preprocess_data():
    df_pt02 = pd.DataFrame({
        "precipitation": pt02_data["precipitation"][:],  
        "temperature": pt02_data["temperature"][:],  
        "humidity": pt02_data["humidity"][:],  
        "wind_speed": pt02_data["wind_speed"][:]
    })
    
    df_tide = pd.DataFrame(tide_data["predictions"])
    df_tide["value"] = df_tide["value"].astype(float)
    
    df = pd.concat([df_pt02, df_tide["value"]], axis=1).dropna()
    X = df.drop("value", axis=1).values
    y = df["value"].values  
    return X, y

X, y = preprocess_data()

# Criar e treinar modelo de previsão de cheias
model = tf.keras.Sequential([
    tf.keras.layers.Dense(64, activation="relu"),
    tf.keras.layers.Dense(32, activation="relu"),
    tf.keras.layers.Dense(1, activation="sigmoid")  
])

model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])
model.fit(X, y, epochs=10, batch_size=32, verbose=1)

# Exportar modelo para OpenVINO
model.save("flood_model.h5")
ov_model = convert_model("flood_model.h5", compress_to_fp16=True)
ov_model.save("flood_model_openvino.xml")
print("Modelo treinado e otimizado com OpenVINO para GPU Intel.")
