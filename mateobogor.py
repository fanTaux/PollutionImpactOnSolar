import pandas as pd
import requests
from datetime import datetime

# --- KONFIGURASI BARU ---
INPUT_FILE = "data_lengkap_bogor_selatan.csv"
OUTPUT_FILE = "dataset_bogor_solar_polusi_final.csv"

# Koordinat Baru (Bogor Selatan)
LAT = -6.5944
LON = 106.7892

def process_and_merge():
    print("1. Membaca Data Polusi (CSV Local)...")
    try:
        df_aq = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: File {INPUT_FILE} tidak ditemukan. Pastikan nama file benar.")
        return

    # Konversi waktu & Pivot data agar rapi
    df_aq['datetime_local'] = pd.to_datetime(df_aq['datetime_local'])
    df_pivot = df_aq.pivot_table(
        index='datetime_local', 
        columns='parameter', 
        values='value', 
        aggfunc='mean'
    ).reset_index()

    # Hapus info timezone agar bisa di-merge
    df_pivot['datetime_key'] = df_pivot['datetime_local'].dt.tz_localize(None)
    
    # Ambil rentang tanggal dari data CSV Anda
    start_date = df_pivot['datetime_key'].min().strftime('%Y-%m-%d')
    end_date = df_pivot['datetime_key'].max().strftime('%Y-%m-%d')
    
    print(f"   -> Lokasi: {LAT}, {LON}")
    print(f"   -> Periode Data: {start_date} s/d {end_date}")

    print("\n2. Menarik Data Matahari dari Open-Meteo (Archive)...")
    
    # Kita gunakan Archive API karena datanya masa lalu
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date,
        "end_date": end_date,
        # Kita butuh Radiasi (Watt) dan Cloud Cover (%)
        "hourly": ["temperature_2m", "cloud_cover", "shortwave_radiation", "direct_normal_irradiance"],
        "timezone": "Asia/Bangkok"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data_solar = response.json()
        
        # Ambil data hourly
        hourly = data_solar['hourly']
        df_solar = pd.DataFrame({
            'datetime_key': pd.to_datetime(hourly['time']),
            'temperature_weather': hourly['temperature_2m'],   # Suhu dari API Cuaca
            'cloud_cover': hourly['cloud_cover'],              # Tutupan Awan (%)
            'solar_radiation': hourly['shortwave_radiation'],  # Radiasi Global (W/m2)
            'direct_radiation': hourly['direct_normal_irradiance'] # Radiasi Langsung
        })
        
        print(f"   -> Berhasil mengunduh {len(df_solar)} jam data cuaca.")
        
    except Exception as e:
        print(f"   -> Gagal mengambil data API: {e}")
        return

    print("\n3. Menggabungkan Data (Merge)...")
    # Gabungkan data Polusi (df_pivot) dengan Data Cuaca (df_solar)
    df_final = pd.merge(df_pivot, df_solar, on='datetime_key', how='inner')
    
    # --- FITUR TAMBAHAN: Hitung Simulasi Energi ---
    # Rumus: Output (W) = Radiasi (W/m2) * Luas Panel (m2) * Efisiensi Panel
    # Asumsi: Panel rumahan ukuran 1.6m x 1m, efisiensi 18%
    df_final['simulated_solar_output_watt'] = df_final['solar_radiation'] * 1.6 * 0.18
    
    # Simpan ke CSV baru
    df_final.to_csv(OUTPUT_FILE, index=False)
    
    print("-" * 40)
    print(f"SELESAI! Dataset Analisis tersimpan di: {OUTPUT_FILE}")
    print("\nContoh 5 Baris Data Gabungan:")
    # Tampilkan kolom penting saja
    cols = ['datetime_key', 'pm25', 'cloud_cover', 'solar_radiation', 'simulated_solar_output_watt']
    print(df_final[cols].head().to_string())

if __name__ == "__main__":
    process_and_merge()