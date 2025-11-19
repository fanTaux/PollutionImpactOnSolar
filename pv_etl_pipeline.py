import pandas as pd
import numpy as np
import math
import requests
import time
from sqlalchemy import create_engine
import openmeteo_requests
import requests_cache
from retry_requests import retry

# --- 1. KONFIGURASI GLOBAL ---
# Konfigurasi Database PostgreSQL
DB_NAME = "pv_analysis_db"
DB_USER = "postgres"
DB_PASS = "Sidoarjo11_" 
DB_HOST = "localhost"
DB_PORT = "5432" # <<< SUDAH DIUBAH KE 5432
TABLE_NAME = "hourly_pv_data" 

# Konfigurasi Lokasi & PV System
LAT = -8.53035       # Latitude Ubud, Bali
LON = 115.26933      # Longitude Ubud, Bali
TZ = "Asia/Makassar" # Timezone WIB/Bali
START_DATE = "2025-06-22"
END_DATE = "2025-11-19"
RATED_POWER = 250    # Kapasitas Panel (250 Watt)
PANEL_TILT = 10      # Kemiringan 10 derajat
PANEL_AZIMUTH = 0    # Azimuth 0 (Menghadap Utara)
TEMP_COEFF = -0.0045

# Konfigurasi OpenAQ
OPENAQ_API_KEY = "c6375c650d0bb95242e8c2dd633602b6ff6f52bd9ebe3e226f448a1e8b078358"
OPENAQ_SENSORS = [
    {'id': 13397854, 'param': 'pm1'},
    {'id': 13397855, 'param': 'pm25'},
    {'id': 13397856, 'param': 'relativehumidity'},
    {'id': 13397857, 'param': 'temperature'},
    {'id': 13397858, 'param': 'um003'}
]

# --- 2. FUNGSI EXTRACT (FIXED TIMEZONE) ---

def extract_openaq_data(sensors, api_key):
    """
    Mengambil data polusi dari OpenAQ API v3 dengan indikator progres.
    """
    all_data = []
    headers = {"X-API-Key": api_key, "Accept": "application/json"}
    
    print("\n" + "="*50)
    print("⏳ [E] Memulai pengambilan data Polusi dari OpenAQ (API v3)...")
    print("="*50)

    for sensor in sensors:
        s_id = sensor['id']
        s_param = sensor['param']
        
        print(f"👉 Mengambil data Sensor: {s_id} ({s_param})")
        
        page = 1
        total_sensor_data = 0
        
        while True:
            url = f"https://api.openaq.org/v3/sensors/{s_id}/measurements"
            
            params = {
                "datetime_from": f"{START_DATE}T00:00:00Z",
                "datetime_to": f"{END_DATE}T23:59:59Z",
                "limit": 1000,
                "page": page
            }
            
            try:
                response = requests.get(url, headers=headers, params=params)
                
                if response.status_code != 200:
                    print(f"   ⚠️ Stop di Page {page}. Status Code: {response.status_code}")
                    break

                data = response.json()
                results = data.get('results', [])
                
                if results:
                    batch_data = []
                    for r in results:
                        # Parsing waktu v3
                        local_time = r.get('period', {}).get('datetimeTo', {}).get('local')
                        if not local_time:
                            local_time = r.get('period', {}).get('datetimeFrom', {}).get('local')

                        if local_time:
                            batch_data.append({
                                'datetime_local': local_time,
                                'parameter': s_param,
                                'value': r.get('value')
                            })

                    all_data.extend(batch_data)
                    
                    count_batch = len(batch_data)
                    total_sensor_data += count_batch
                    
                    print(f"   📦 Page {page}: +{count_batch} data. (Total: {total_sensor_data})")
                    
                    if len(results) < 1000:
                        print("   ✅ Ini halaman terakhir untuk sensor ini.")
                        break 
                    
                    page += 1
                    time.sleep(0.2) 
                else:
                    print("   ✅ Tidak ada data lagi.")
                    break
                    
            except Exception as e:
                print(f"   ❌ Error pada Page {page}: {e}")
                break
        
        print("-" * 30)
    
    if not all_data:
        print("❌ [E] Gagal mengambil data polusi / Data Kosong.")
        return pd.DataFrame()
        
    df_openaq = pd.DataFrame(all_data)
    
    # === PERBAIKAN UTAMA DI SINI ===
    # 1. Convert ke datetime
    # 2. .dt.tz_localize(None) membuang info timezone (+08:00) agar jadi "naive"
    df_openaq['datetime_local'] = pd.to_datetime(df_openaq['datetime_local'], utc=True).dt.tz_convert(TZ).dt.tz_localize(None)
    
    # Pivot
    df_openaq = df_openaq.pivot_table(
        index='datetime_local', 
        columns='parameter', 
        values='value', 
        aggfunc='mean'
    ).reset_index()
    
    rename_map = {'temperature': 'temp_openaq', 'relativehumidity': 'rh_openaq'}
    df_openaq = df_openaq.rename(columns=rename_map)
    
    print(f"✅ [E] Selesai Extract OpenAQ. Total baris gabungan: {len(df_openaq)}")
    return df_openaq


def extract_openmeteo_data():
    """Mengambil data cuaca dan radiasi dari Open-Meteo Archive API."""
    print("\n⏳ [E] Memulai pengambilan data Cuaca dari Open-Meteo...")
    
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": ["temperature_2m", "cloud_cover", "relative_humidity_2m", 
                   "precipitation", "shortwave_radiation", "direct_normal_irradiance", 
                   "diffuse_radiation"],
        "timezone": TZ 
    }
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    responses = openmeteo.weather_api(url, params=params)
    
    if not responses:
        print("❌ [E] Gagal mengambil data cuaca.")
        return pd.DataFrame()

    response = responses[0]
    hourly = response.Hourly()
    
    hourly_data = {
        "datetime_local": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        ).tz_convert(TZ).tz_localize(None), # Ini sudah Naive (Polos)
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "cloud_cover": hourly.Variables(1).ValuesAsNumpy(),
        "relative_humidity_2m": hourly.Variables(2).ValuesAsNumpy(),
        "precipitation": hourly.Variables(3).ValuesAsNumpy(),
        "shortwave_radiation": hourly.Variables(4).ValuesAsNumpy(),
        "direct_normal_irradiance": hourly.Variables(5).ValuesAsNumpy(),
        "diffuse_radiation": hourly.Variables(6).ValuesAsNumpy(),
    }
    
    df_cuaca = pd.DataFrame(hourly_data)
    print(f"✅ [E] Data Cuaca berhasil diambil: {len(df_cuaca)} baris.")
    return df_cuaca

# --- 3. FUNGSI TRANSFORM & SIMULATE ---

def calculate_solar_position(dt):
    day_of_year = dt.timetuple().tm_yday
    hour = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
    
    b = 360 * (day_of_year - 81) / 365
    eot = 9.87 * np.sin(np.deg2rad(2 * b)) - 7.53 * np.cos(np.deg2rad(b)) - 1.5 * np.sin(np.deg2rad(b))
    
    lst = hour * 15 
    tc = 4 * (0 - LON) + eot 
    solar_time = lst + tc 
    hra = solar_time - 180 
    declination = 23.45 * np.sin(np.deg2rad(360 * (284 + day_of_year) / 365))
    
    cos_zenith = (np.sin(np.deg2rad(LAT)) * np.sin(np.deg2rad(declination)) +
                  np.cos(np.deg2rad(LAT)) * np.cos(np.deg2rad(declination)) * np.cos(np.deg2rad(hra)))
    zenith_angle = np.rad2deg(np.arccos(np.clip(cos_zenith, -1, 1)))
    
    if zenith_angle > 90: zenith_angle = 90
    return zenith_angle

def transform_and_simulate(df_openaq, df_cuaca):
    print("\n⏳ [T] Memulai Transformasi dan Simulasi PV...")

    # Merge (JOIN)
    # Karena kedua kolom datetime_local sekarang sudah sama-sama "Naive" (tidak ada timezone),
    # Merge akan berhasil.
    df_final = pd.merge(df_cuaca, df_openaq, on='datetime_local', how='inner')
    df_final = df_final.rename(columns={'datetime_local': 'timestamp'})
    
    if 'pm25' not in df_final.columns:
        df_final['pm25'] = 0
    else:
        df_final['pm25'] = df_final['pm25'].fillna(0)

    df_final['direct_normal_irradiance'] = df_final['direct_normal_irradiance'].fillna(0)
    df_final['diffuse_radiation'] = df_final['diffuse_radiation'].fillna(0)
    
    poa_list = []
    power_list = []
    tilt_rad = np.deg2rad(PANEL_TILT)
    
    for index, row in df_final.iterrows():
        dt = row['timestamp']
        dni = row['direct_normal_irradiance']
        dhi = row['diffuse_radiation']
        temp_amb = row['temperature_2m']
        ghi = row['shortwave_radiation']
        
        zenith_angle = calculate_solar_position(dt)
        
        if zenith_angle > 89.9:
            poa_total = 0.0
            dc_output = 0.0
        else:
            zenith_rad = np.deg2rad(zenith_angle)
            cos_aoi = np.cos(zenith_rad) * np.cos(tilt_rad) 
            poa_beam = dni * max(0, cos_aoi)
            poa_diffuse = dhi * ((1 + np.cos(tilt_rad)) / 2)
            poa_ground = ghi * 0.2 * ((1 - np.cos(tilt_rad)) / 2)
            poa_total = poa_beam + poa_diffuse + poa_ground
            
            t_cell = temp_amb + (45 - 20) / 800 * poa_total
            temp_correction = 1 + TEMP_COEFF * (t_cell - 25)
            dc_output = RATED_POWER * (poa_total / 1000) * temp_correction
            dc_output = max(0, dc_output)

        poa_list.append(poa_total)
        power_list.append(dc_output)
        
    df_final['poa_irradiance'] = poa_list
    df_final['simulated_power_watt'] = power_list
    
    print(f"✅ [T] Transformasi selesai. Data siap load: {len(df_final)} baris.")
    
    cols_to_keep = ['timestamp', 'pm25', 'direct_normal_irradiance', 'cloud_cover', 'temperature_2m', 'poa_irradiance', 'simulated_power_watt']
    for col in cols_to_keep:
        if col not in df_final.columns: df_final[col] = 0

    return df_final[cols_to_keep]

# --- 4. FUNGSI LOAD ---

def load_data_to_postgres(df_final: pd.DataFrame):
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print("\n⏳ [L] Memulai Load ke PostgreSQL...")
    
    try:
        engine = create_engine(DATABASE_URL)
        df_final.to_sql(TABLE_NAME, engine, if_exists='replace', index=False, method='multi')
        print(f"✅ [L] Sukses! {len(df_final)} baris data tersimpan di DB.")
        
    except Exception as e:
        print(f"❌ [L] GAGAL Load ke Database: {e}")

# --- 5. ORCHESTRATOR ---

def run_pv_etl_pipeline():
    # 1. EXTRACT
    df_openaq = extract_openaq_data(OPENAQ_SENSORS, OPENAQ_API_KEY)
    df_cuaca = extract_openmeteo_data()
    
    if df_openaq.empty or df_cuaca.empty:
        print("\n❌ Pipeline Berhenti: Data source tidak lengkap.")
        return

    # 2. TRANSFORM
    df_transformed = transform_and_simulate(df_openaq, df_cuaca)
    
    if df_transformed.empty:
        print("\n❌ Pipeline Berhenti: Hasil transformasi kosong.")
        return
        
    # 3. LOAD
    load_data_to_postgres(df_transformed)
    
    print("\n🎉 PIPELINE ETL SELESAI!")

if __name__ == "__main__":
    run_pv_etl_pipeline()