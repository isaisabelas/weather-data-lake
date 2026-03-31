import requests #makes http requests 
import pandas as pd 
from datetime import datetime #provides classes for manipulating dates, times, and time intervals 
import os #serves as a bridge between python and the operating system's functions, promoting cross platform compatibility 

API_KEY= " "
CTIY="Sao Paulo" 
URL= f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric" #the f prefix before a string denotes a formatted string literal 

#function to fetch the API data 
def fetch_weather(): 
  response = requests.get(URL)
  data = response.json() #saves the json result as a python dict
  

  #creates a data frame with formatted data 
  df = pd.DataFrame([{
    "dt": datetime.now(), #data da ingestão 
    "city": CITY, #declarada anteriormente
    "temp": data["main"]["temp"],
    "feels_like": data["main"]["feels_like"],
    "humidity": data["main"]["humidity"], 
    "pressure": data["main"]["pressure"], 
    "weather": data["weather"][0]["description"], 
    "wind_speed": data["wind"]["speed"]
  })]

#save as parquet (bronze layer) 
folder = "data/bronze"
os.makedirs(folder, exist_ok=True) #cheks if the directory already exists 
file = folder + f"weather_{datetime.now().date()}.parquet"

df.to_parquet(file, index=False)
print(f"Arquivo salvo: {file}")

if __name__ == "__main__": 
  fetch_weather()



