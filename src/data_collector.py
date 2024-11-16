import requests
import json
import threading
from datetime import datetime, timedelta
import time
import os

class WeatherDataCollector:
    def __init__(self, weather_api_key, disaster_api_url):
        self.weather_api_key = weather_api_key
        self.weather_base_url = "http://api.weatherapi.com/v1"
        self.disaster_api_url = disaster_api_url
        
    def collect_historical_data(self, lat, lon, start_date, end_date):
        """Thu thập dữ liệu lịch sử thời tiết bằng tọa độ"""
        data = []
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            url = f"{self.weather_base_url}/history.json?key={self.weather_api_key}&q={lat},{lon}&dt={date_str}"
            
            try:
                response = requests.get(url)
                response.raise_for_status()
                weather_data = response.json()
                data.append(weather_data)
                
                print(f"Collected data for {lat},{lon} on {date_str}")
                
                # Tạo thư mục nếu chưa tồn tại
                directory = "data"
                if not os.path.exists(directory):
                    os.makedirs(directory)
                
                filename = f"{directory}/weather_{lat}_{lon}_{date_str}.json"
                with open(filename, 'w') as f:
                    json.dump(weather_data, f)
                
                current_date += timedelta(days=1)
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                print(f"Error collecting data for {lat},{lon} on {date_str}: {str(e)}")
                current_date += timedelta(days=1)
        
        return data

    def collect_disaster_history(self, country="Vietnam", limit=50, offset=0):
        """Thu thập dữ liệu lịch sử thiên tai từ API ReliefWeb"""
        try:
            url = f"{self.disaster_api_url}?query[value]={country}&limit={limit}&offset={offset}"
            response = requests.get(url)
            response.raise_for_status()
            disaster_data = response.json()
            return disaster_data
        
        except requests.exceptions.RequestException as e:
            print(f"Error collecting disaster history: {str(e)}")
            return None

    def collect_data_for_province(self, province, start_date, end_date):
        name = province["name"]
        lat = province["lat"]
        lon = province["lon"]
        
        # Thu thập dữ liệu thời tiết
        data = self.collect_historical_data(lat, lon, start_date, end_date)
        print(f"Collected weather data for {name} ({lat}, {lon}): {len(data)} days")
        
        # Thu thập dữ liệu thiên tai từ ReliefWeb
        disaster_data = self.collect_disaster_history(country=name, limit=50, offset=0)
        if disaster_data:
            print(f"Collected disaster history for {name}: {len(disaster_data['data'])} reports found.")
        else:
            print(f"No disaster history data found for {name}")

def main():
    weather_api_key = "3e1141883f7b46f9986103021241011"
    disaster_api_url = "https://api.reliefweb.int/v1/reports"
    collector = WeatherDataCollector(weather_api_key, disaster_api_url)
    
    # Danh sách tọa độ của 63 tỉnh thành
    provinces_coords = [
    {"name": "Hà Nội", "lat": 21.0285, "lon": 105.8542},
    {"name": "Hồ Chí Minh", "lat": 10.8231, "lon": 106.6297},
    {"name": "Đà Nẵng", "lat": 16.0471, "lon": 108.2068},
    {"name": "Hải Phòng", "lat": 20.8449, "lon": 106.6881},
    {"name": "Cần Thơ", "lat": 10.0452, "lon": 105.7469},
    {"name": "An Giang", "lat": 10.5216, "lon": 105.1259},
    {"name": "Bà Rịa - Vũng Tàu", "lat": 10.5417, "lon": 107.2429},
    {"name": "Bắc Giang", "lat": 21.2730, "lon": 106.1946},
    {"name": "Bắc Kạn", "lat": 22.1450, "lon": 105.8390},
    {"name": "Bạc Liêu", "lat": 9.2856, "lon": 105.7243},
    {"name": "Bắc Ninh", "lat": 21.1850, "lon": 106.0763},
    {"name": "Bến Tre", "lat": 10.2320, "lon": 106.3758},
    {"name": "Bình Định", "lat": 13.7827, "lon": 109.2190},
    {"name": "Bình Dương", "lat": 11.1732, "lon": 106.6893},
    {"name": "Bình Phước", "lat": 11.7512, "lon": 106.7235},
    {"name": "Bình Thuận", "lat": 11.0905, "lon": 108.0721},
    {"name": "Cà Mau", "lat": 9.1765, "lon": 105.1524},
    {"name": "Cao Bằng", "lat": 22.6646, "lon": 106.2566},
    {"name": "Đắk Lắk", "lat": 12.6667, "lon": 108.0333},
    {"name": "Đắk Nông", "lat": 12.2646, "lon": 107.6098},
    {"name": "Điện Biên", "lat": 21.3860, "lon": 103.0230},
    {"name": "Đồng Nai", "lat": 10.9447, "lon": 106.8243},
    {"name": "Đồng Tháp", "lat": 10.5825, "lon": 105.6341},
    {"name": "Gia Lai", "lat": 13.9833, "lon": 108.0000},
    {"name": "Hà Giang", "lat": 22.8037, "lon": 104.9784},
    {"name": "Hà Nam", "lat": 20.5830, "lon": 105.9166},
    {"name": "Hà Tĩnh", "lat": 18.3521, "lon": 105.8931},
    {"name": "Hải Dương", "lat": 20.9373, "lon": 106.3146},
    {"name": "Hậu Giang", "lat": 9.7879, "lon": 105.4662},
    {"name": "Hòa Bình", "lat": 20.8511, "lon": 105.3383},
    {"name": "Hưng Yên", "lat": 20.6460, "lon": 106.0512},
    {"name": "Khánh Hòa", "lat": 12.2388, "lon": 109.1967},
    {"name": "Kiên Giang", "lat": 10.0125, "lon": 105.0809},
    {"name": "Kon Tum", "lat": 14.3525, "lon": 107.9843},
    {"name": "Lai Châu", "lat": 22.3964, "lon": 103.4550},
    {"name": "Lâm Đồng", "lat": 11.9404, "lon": 108.4583},
    {"name": "Lạng Sơn", "lat": 21.8537, "lon": 106.7615},
    {"name": "Lào Cai", "lat": 22.4856, "lon": 103.9707},
    {"name": "Long An", "lat": 10.6244, "lon": 106.4136},
    {"name": "Nam Định", "lat": 20.4336, "lon": 106.1777},
    {"name": "Nghệ An", "lat": 19.3338, "lon": 104.8984},
    {"name": "Ninh Bình", "lat": 20.2523, "lon": 105.9741},
    {"name": "Ninh Thuận", "lat": 11.5670, "lon": 108.9887},
    {"name": "Phú Thọ", "lat": 21.3833, "lon": 105.2333},
    {"name": "Phú Yên", "lat": 13.0882, "lon": 109.0929},
    {"name": "Quảng Bình", "lat": 17.4929, "lon": 106.6057},
    {"name": "Quảng Nam", "lat": 15.5334, "lon": 108.2206},
    {"name": "Quảng Ngãi", "lat": 15.1205, "lon": 108.7923},
    {"name": "Quảng Ninh", "lat": 21.0064, "lon": 107.2925},
    {"name": "Quảng Trị", "lat": 16.7531, "lon": 107.1906},
    {"name": "Sóc Trăng", "lat": 9.6027, "lon": 105.9738},
    {"name": "Sơn La", "lat": 21.1159, "lon": 103.9057},
    {"name": "Tây Ninh", "lat": 11.3545, "lon": 106.1477},
    {"name": "Thái Bình", "lat": 20.4519, "lon": 106.3363},
    {"name": "Thái Nguyên", "lat": 21.5662, "lon": 105.8360},
    {"name": "Thanh Hóa", "lat": 20.0523, "lon": 105.7741},
    {"name": "Thừa Thiên Huế", "lat": 16.4498, "lon": 107.5624},
    {"name": "Tiền Giang", "lat": 10.4493, "lon": 106.3423},
    {"name": "Trà Vinh", "lat": 9.8128, "lon": 106.2993},
    {"name": "Tuyên Quang", "lat": 21.8229, "lon": 105.2141},
    {"name": "Vĩnh Long", "lat": 10.2536, "lon": 105.9722},
    {"name": "Vĩnh Phúc", "lat": 21.3089, "lon": 105.5916},
    {"name": "Yên Bái", "lat": 21.7228, "lon": 104.9115}
]
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    threads = []
    for province in provinces_coords:
        thread = threading.Thread(target=collector.collect_data_for_province, args=(province, start_date, end_date))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
