import joblib
import numpy as np
from datetime import datetime, timedelta


class WeatherPredictor:
    def __init__(self, model_path='models/weather_model.joblib'):
        """
        Khởi tạo lớp WeatherPredictor, tải mô hình và scaler từ file.
        """
        # Load model và scaler từ file
        model_artifacts = joblib.load(model_path)
        self.model = model_artifacts['model']
        self.scaler = model_artifacts['scaler']
        self.features_order = model_artifacts.get('features_order', [])
    
    def prepare_input(self, current_data):
        """
        Chuẩn bị dữ liệu đầu vào cho dự báo.
        :param current_data: dict chứa thông tin thời tiết hiện tại.
        :return: mảng dữ liệu đã được chuẩn hóa.
        """
        # Lấy giờ hiện tại và điều chỉnh trạng thái ngày/đêm
        current_hour = datetime.now().hour
        is_day = 1 if 6 <= current_hour <= 18 else 0

        # Tạo mảng dữ liệu với thứ tự các đặc trưng
        features = [
            current_data.get('temp_c', 0),
            current_data.get('humidity', 0),
            current_data.get('wind_kph', 0),
            current_data.get('pressure_mb', 0),
            current_data.get('precip_mm', 0),
            current_data.get('cloud', 0),
            current_data.get('feelslike_c', 0),
            current_data.get('dewpoint_c', 0),
            current_hour,
            datetime.now().month,
            is_day
        ]

        # Chuyển đổi sang numpy array và chuẩn hóa
        features = np.array(features).reshape(1, -1)
        features_scaled = self.scaler.transform(features)
        return features_scaled
    
    def predict(self, current_data, horizon=24):
        """
        Thực hiện dự báo thời tiết cho một khoảng thời gian nhất định.
        :param current_data: dict chứa thông tin thời tiết hiện tại.
        :param horizon: số giờ dự báo.
        :return: list dự báo nhiệt độ (hoặc các thông số khác tùy model).
        """
        # Chuẩn bị input
        input_features = self.prepare_input(current_data)
        predictions = []
        
        for _ in range(horizon):
            # Dự báo giá trị tiếp theo
            next_prediction = self.model.predict(input_features)[0]
            predictions.append(next_prediction)
            
            # Cập nhật input cho vòng lặp tiếp theo
            input_features = np.roll(input_features, -1, axis=1)
            input_features[0, -1] = next_prediction
        
        return predictions

    def format_predictions(self, predictions):
        """
        Định dạng kết quả dự báo với timestamp.
        :param predictions: list dự báo.
        :return: list chứa dict {time, prediction}.
        """
        now = datetime.now()
        formatted = [
            {'time': (now + timedelta(hours=i)).strftime('%Y-%m-%d %H:%M:%S'), 'prediction': pred}
            for i, pred in enumerate(predictions)
        ]
        return formatted


def main():
    # Tải dữ liệu hiện tại (giả lập hoặc từ API)
    current_data = {
        "temp_c": 26.5,
        "humidity": 70,
        "wind_kph": 12.5,
        "pressure_mb": 1015,
        "precip_mm": 0,
        "cloud": 50,
        "feelslike_c": 28,
        "dewpoint_c": 19
    }
    
    # Khởi tạo WeatherPredictor và dự báo
    predictor = WeatherPredictor(model_path='models/weather_model.joblib')
    predictions = predictor.predict(current_data, horizon=24)
    formatted_predictions = predictor.format_predictions(predictions)
    
    # Hiển thị kết quả
    print("Dự báo thời tiết:")
    for pred in formatted_predictions:
        print(f"{pred['time']}: {pred['prediction']}°C")


if __name__ == "__main__":
    main()
