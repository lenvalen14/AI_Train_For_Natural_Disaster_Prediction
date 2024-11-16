import pandas as pd
import numpy as np
import json
import glob
from datetime import datetime

class WeatherDataProcessor:
    def __init__(self):
        self.weather_features = [
            'temp_c', 'humidity', 'wind_kph', 'pressure_mb',
            'precip_mm', 'cloud', 'feelslike_c', 'dewpoint_c',
            'wind_degree', 'gust_kph', 'condition_text', 'hour', 'month', 'is_day', 'timestamp'
        ]
        self.disaster_features = ['disaster_name', 'description', 'date']

    def load_json_files(self, data_path, prefix):
        """Load tất cả file JSON từ thư mục data."""
        all_data = []
        json_files = glob.glob(f"{data_path}/{prefix}_*.json")
        
        for file_path in sorted(json_files):
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    all_data.append(data)
            except Exception as e:
                print(f"Error loading file {file_path}: {e}")
        
        return all_data

    def extract_weather_features(self, json_data):
        """Trích xuất features từ dữ liệu JSON thời tiết, xử lý dữ liệu thiếu."""
        records = []
        for data in json_data:
            for forecast_day in data['forecast']['forecastday']:
                for hour in forecast_day['hour']:
                    record = {
                        'temp_c': hour.get('temp_c', np.nan),
                        'humidity': hour.get('humidity', np.nan),
                        'wind_kph': hour.get('wind_kph', np.nan),
                        'pressure_mb': hour.get('pressure_mb', np.nan),
                        'precip_mm': hour.get('precip_mm', np.nan),
                        'cloud': hour.get('cloud', np.nan),
                        'feelslike_c': hour.get('feelslike_c', np.nan),
                        'dewpoint_c': hour.get('dewpoint_c', np.nan),
                        'wind_degree': hour.get('wind_degree', np.nan),
                        'gust_kph': hour.get('gust_kph', np.nan),
                        'condition_text': hour.get('condition', {}).get('text', "Unknown"),
                        'hour': pd.to_datetime(hour.get('time')).hour if 'time' in hour else np.nan,
                        'month': pd.to_datetime(hour.get('time')).month if 'time' in hour else np.nan,
                        'is_day': hour.get('is_day', np.nan),
                        'timestamp': pd.to_datetime(hour.get('time')) if 'time' in hour else np.nan
                    }
                    records.append(record)
        return pd.DataFrame(records)

    def extract_disaster_features(self, json_data):
        """Trích xuất features từ dữ liệu JSON thiên tai."""
        records = []
        for data in json_data:
            for report in data['data']:
                record = {
                    'disaster_name': report['fields']['title'],
                    'description': report['href'],
                    'date': pd.to_datetime(report['fields']['date']['created']) if 'date' in report['fields'] and 'created' in report['fields']['date'] else np.nan
                }
                records.append(record)
        return pd.DataFrame(records)
    
    def handle_missing_values(self, df, features):
        """Xử lý giá trị thiếu."""
        for feature in features:
            if feature not in ['hour', 'month', 'is_day', 'condition_text', 'disaster_name', 'description', 'date']:
                df[feature] = df[feature].interpolate(method='time')
        df['condition_text'] = df['condition_text'].fillna("Unknown")
        return df
    
    def remove_outliers(self, df, features):
        """Loại bỏ outliers sử dụng IQR method."""
        for feature in features:
            if feature not in ['hour', 'month', 'is_day', 'condition_text', 'disaster_name', 'description', 'date']:
                Q1 = df[feature].quantile(0.25)
                Q3 = df[feature].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                df[feature] = df[feature].clip(lower_bound, upper_bound)
        return df
    
    def create_sequences(self, df, features, lookback=24, forecast_horizon=24):
        """Tạo chuỗi dữ liệu cho training."""
        X, y = [], []
        for i in range(len(df) - lookback - forecast_horizon + 1):
            X.append(df[features].iloc[i:(i + lookback)].values)
            y.append(df['temp_c'].iloc[(i + lookback):(i + lookback + forecast_horizon)].values)
        return np.array(X), np.array(y)

def main():
    processor = WeatherDataProcessor()
    
    print("Loading weather data...")
    weather_json_data = processor.load_json_files("data", "weather")
    print("Loading disaster data...")
    disaster_json_data = processor.load_json_files("data", "disaster")
    
    print("Processing weather data...")
    weather_df = processor.extract_weather_features(weather_json_data)
    weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
    weather_df.set_index('timestamp', inplace=True)
    weather_df = processor.handle_missing_values(weather_df, processor.weather_features)
    weather_df = processor.remove_outliers(weather_df, processor.weather_features)
    
    print("Processing disaster data...")
    disaster_df = processor.extract_disaster_features(disaster_json_data)
    disaster_df['date'] = pd.to_datetime(disaster_df['date'])
    disaster_df.set_index('date', inplace=True)
    
    print("Creating sequences for weather data...")
    X, y = processor.create_sequences(weather_df, processor.weather_features)
    
    np.save('data/processed_X.npy', X)
    np.save('data/processed_y.npy', y)
    
    weather_df.to_csv('data/processed_weather_data.csv')
    disaster_df.to_csv('data/processed_disaster_data.csv')
    
    print(f"Processed weather data saved: X shape {X.shape}, y shape {y.shape}")
    print("Processed disaster data saved to CSV")

if __name__ == "__main__":
    main()
