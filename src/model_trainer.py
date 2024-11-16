import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
import pandas as pd

class DisasterPredictor:
    def __init__(self):
        self.scaler = StandardScaler()
        self.model = RandomForestClassifier(
            n_estimators=200,
            max_depth=15,
            min_samples_split=5,
            class_weight='balanced',
            random_state=42
        )
    
    def extract_disaster_features(self, weather_data):
        """Extract features for disaster prediction"""
        features = []
        for day_data in weather_data:
            daily_features = {
                'rainfall_mm': day_data.get('rain_mm', np.nan),
                'rainfall_3day_sum': self._calculate_rainfall_sum(day_data, days=3),
                'rainfall_7day_sum': self._calculate_rainfall_sum(day_data, days=7),
                'max_wind_kph': day_data.get('wind_kph', np.nan),
                'gust_wind_kph': day_data.get('gust_kph', np.nan),
                'sustained_wind_hours': self._calculate_sustained_wind(day_data),
                'temp_change_24h': self._calculate_temp_change(day_data),
                'extreme_temp_hours': self._count_extreme_temp_hours(day_data),
                'pressure_mb': day_data.get('pressure_mb', np.nan),
                'pressure_change_6h': self._calculate_pressure_change(day_data),
                'humidity': day_data.get('humidity', np.nan),
                'humidity_change_24h': self._calculate_humidity_change(day_data),
                'max_temp': day_data.get('max_temp_c', np.nan),
                'min_temp': day_data.get('min_temp_c', np.nan),
                'fog_visibility': day_data.get('visibility_km', np.nan),
                'snowfall_mm': day_data.get('snow_mm', np.nan)
            }
            daily_features.update(self._calculate_disaster_indices(daily_features))
            features.append(daily_features)
        return pd.DataFrame(features)
    
    def _calculate_rainfall_sum(self, day_data, days):
        # Placeholder method; needs implementation based on actual data structure
        return day_data.get('rain_mm', 0) * days
    
    def _calculate_sustained_wind(self, day_data):
        # Placeholder method; needs implementation based on actual data structure
        return day_data.get('wind_kph', 0) * 1.5
    
    def _calculate_temp_change(self, day_data):
        # Placeholder method; needs implementation based on actual data structure
        return day_data.get('temp_max_c', 0) - day_data.get('temp_min_c', 0)
    
    def _count_extreme_temp_hours(self, day_data):
        # Placeholder method; needs implementation based on actual data structure
        return len([hour for hour in day_data.get('hourly', []) if hour.get('temp_c', 0) > 30 or hour.get('temp_c', 0) < 0])
    
    def _calculate_pressure_change(self, day_data):
        # Placeholder method; needs implementation based on actual data structure
        return day_data.get('pressure_mb', 0) - 1000
    
    def _calculate_humidity_change(self, day_data):
        # Placeholder method; needs implementation based on actual data structure
        return day_data.get('humidity', 0) - 50
    
    def _calculate_disaster_indices(self, features):
        indices = {
            'flood_risk_index': self._calculate_flood_risk(features['rainfall_3day_sum'], features['rainfall_7day_sum']),
            'storm_risk_index': self._calculate_storm_risk(features['max_wind_kph'], features['pressure_mb'], features['pressure_change_6h']),
            'drought_risk_index': self._calculate_drought_risk(features['rainfall_7day_sum'], features['humidity'], features['temp_change_24h']),
            'tornado_risk_index': self._calculate_tornado_risk(features['max_wind_kph'], features['gust_wind_kph']),
            'fog_risk_index': self._calculate_fog_risk(features['fog_visibility']),
            'cold_wave_risk_index': self._calculate_cold_wave_risk(features['min_temp'], features['snowfall_mm']),
            'heatwave_risk_index': self._calculate_heatwave_risk(features['max_temp'], features['humidity'])
        }
        return indices
    
    def _calculate_flood_risk(self, rain_3d, rain_7d):
        risk = 0
        if rain_3d > 100:
            risk += 3
        if rain_7d > 200:
            risk += 2
        return risk
    
    def _calculate_storm_risk(self, wind_speed, pressure, pressure_change):
        risk = 0
        if wind_speed > 100:
            risk += 3
        if pressure < 960:
            risk += 2
        if abs(pressure_change) > 10:
            risk += 2
        return risk
    
    def _calculate_drought_risk(self, rain_7d, humidity, temp_change):
        risk = 0
        if rain_7d < 5:
            risk += 2
        if humidity < 30:
            risk += 2
        if temp_change > 5:
            risk += 1
        return risk
    
    def _calculate_tornado_risk(self, wind_speed, gust_speed):
        risk = 0
        if gust_speed > 100:
            risk += 3
        if wind_speed > 75:
            risk += 2
        return risk
    
    def _calculate_fog_risk(self, visibility):
        risk = 0
        if visibility < 1:
            risk += 2
        return risk
    
    def _calculate_cold_wave_risk(self, min_temp, snowfall):
        risk = 0
        if min_temp < 0:
            risk += 2
        if snowfall > 5:
            risk += 1
        return risk
    
    def _calculate_heatwave_risk(self, max_temp, humidity):
        risk = 0
        if max_temp > 35:
            risk += 2
        if humidity < 20:
            risk += 1
        return risk
    
    def predict_disasters(self, features):
        predictions = {
            'flood_warning': self._predict_flood(features),
            'storm_warning': self._predict_storm(features),
            'drought_warning': self._predict_drought(features),
            'tornado_warning': self._predict_tornado(features),
            'fog_warning': self._predict_fog(features),
            'cold_wave_warning': self._predict_cold_wave(features),
            'heatwave_warning': self._predict_heatwave(features)
        }
        return predictions

    def _predict_flood(self, features):
        return features['flood_risk_index'] > 2
    
    def _predict_storm(self, features):
        return features['storm_risk_index'] > 2
    
    def _predict_drought(self, features):
        return features['drought_risk_index'] > 2
    
    def _predict_tornado(self, features):
        return features['tornado_risk_index'] > 2
    
    def _predict_fog(self, features):
        return features['fog_risk_index'] > 1
    
    def _predict_cold_wave(self, features):
        return features['cold_wave_risk_index'] > 1
    
    def _predict_heatwave(self, features):
        return features['heatwave_risk_index'] > 1

    def train_model(self, X, y):
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled, y)
    
    def evaluate_model(self, X, y):
        X_scaled = self.scaler.transform(X)
        predictions = self.model.predict(X_scaled)
        print(classification_report(y, predictions))
        print(f"Accuracy: {accuracy_score(y, predictions)}")

    def load_weather_data_from_json(self, directory="data"):
        weather_data_list = []
        for filename in os.listdir(directory):
            if filename.endswith(".json"):
                file_path = os.path.join(directory, filename)
                with open(file_path, 'r') as f:
                    weather_data = json.load(f)
                    # Giả sử file JSON chứa dữ liệu theo dạng ngày, thêm thông tin vào danh sách
                    weather_data_list.append(weather_data)
        
        # Giả sử weather_data_list chứa các thông tin có thể chuyển thành DataFrame
        return pd.DataFrame(weather_data_list)


def main():
    predictor = DisasterPredictor()
    
    # Đọc dữ liệu từ các file JSON trong thư mục
    weather_data = predictor.load_weather_data_from_json('data')
    
    # Trích xuất các đặc trưng từ dữ liệu thời tiết
    features = predictor.extract_disaster_features(weather_data)
    
    # Giả sử disaster_labels là danh sách nhãn (thảm họa) cho mỗi ngày
    disaster_labels = np.array([0, 1, 0, 0, 1, 0, 0])  # Đây là ví dụ, bạn cần thay thế bằng nhãn thật của bạn
    
    # Chia dữ liệu thành tập huấn luyện và kiểm tra (Ví dụ: 80% huấn luyện, 20% kiểm tra)
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(features, disaster_labels, test_size=0.2, random_state=42)
    
    # Huấn luyện mô hình
    predictor.train_model(X_train, y_train)
    
    # Đánh giá mô hình
    predictor.evaluate_model(X_test, y_test)

if __name__ == "__main__":
    main()
