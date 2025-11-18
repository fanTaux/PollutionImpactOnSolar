import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry

# 1. Setup Open-Meteo API client dengan cache & retry
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# 2. Konfigurasi URL dan Parameter
# PENTING: Kita gunakan 'archive-api' untuk mengambil data masa lalu
url = "https://archive-api.open-meteo.com/v1/archive"

params = {
	"latitude": -8.53035,
	"longitude": 115.26933,
    "start_date": "2025-06-22", # Sesuai data Polusi Anda
    "end_date": "2025-11-18",   # Sesuai data Polusi Anda
    "hourly": [
        "temperature_2m", 
        "cloud_cover", 
        "relative_humidity_2m", 
        "precipitation", # Ganti probabilitas (forecast) jadi curah hujan real (archive)
        "shortwave_radiation", 
        "direct_normal_irradiance", 
        "diffuse_radiation"
    ],
    "timezone": "Asia/Makassar" # WIB
}

print("Sedang mengambil data dari Open-Meteo Archive...")
responses = openmeteo.weather_api(url, params=params)

# 3. Proses Response (Ambil lokasi pertama)
response = responses[0]
print(f"Koordinat: {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevasi: {response.Elevation()} m asl")

# 4. Proses Data Hourly (Per Jam)
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
hourly_cloud_cover = hourly.Variables(1).ValuesAsNumpy()
hourly_relative_humidity_2m = hourly.Variables(2).ValuesAsNumpy()
hourly_precipitation = hourly.Variables(3).ValuesAsNumpy()
hourly_shortwave_radiation = hourly.Variables(4).ValuesAsNumpy()
hourly_direct_normal_irradiance = hourly.Variables(5).ValuesAsNumpy()
hourly_diffuse_radiation = hourly.Variables(6).ValuesAsNumpy()

# Buat Dictionary Data
hourly_data = {"date": pd.date_range(
    start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
    end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
    freq = pd.Timedelta(seconds = hourly.Interval()),
    inclusive = "left"
)}

# Masukkan nilai ke kolom
hourly_data["temperature_2m"] = hourly_temperature_2m
hourly_data["cloud_cover"] = hourly_cloud_cover
hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
hourly_data["precipitation"] = hourly_precipitation
hourly_data["shortwave_radiation"] = hourly_shortwave_radiation
hourly_data["direct_normal_irradiance"] = hourly_direct_normal_irradiance
hourly_data["diffuse_radiation"] = hourly_diffuse_radiation

# 5. Buat DataFrame Pandas
df_cuaca = pd.DataFrame(data = hourly_data)

# Konversi timezone UTC ke WIB (Jakarta) agar sinkron dengan data Polusi
df_cuaca['date'] = df_cuaca['date'].dt.tz_convert('Asia/Makassar').dt.tz_localize(None)

# Tampilkan Preview
print("\nPreview Data (5 Baris Teratas):")
print(df_cuaca.head())

# 6. SIMPAN KE CSV
nama_file = "data_solar_bali_openmeteo.csv"
df_cuaca.to_csv(nama_file, index=False)

print("-" * 40)
print(f"SUKSES! Data tersimpan di file: {nama_file}")
print(f"Jumlah Data: {len(df_cuaca)} baris")