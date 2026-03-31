import pandas as pd
import os

def generate_gold():
    df = pd.read_parquet("data/silver/weather_clean.parquet")

    df["date"] = df["dt"].dt.date
    gold = df.groupby("date").agg(
        avg_temp=("temp", "mean"),
        avg_humidity=("humidity", "mean"),
        max_temp=("temp", "max"),
        min_temp=("temp", "min"),
        weather_modes=("weather", lambda x: x.mode()[0] if not x.mode().empty else None)
    ).reset_index()

    os.makedirs("data/gold/", exist_ok=True)
    gold.to_parquet("data/gold/weather_daily_metrics.parquet", index=False)

    print("Gold gerado com sucesso.")

if __name__ == "__main__":
    generate_gold()
