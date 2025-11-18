import requests
import pandas as pd
import time

def fetch_measurements():
    # 1. Daftar Sensor ID dari hasil scan sebelumnya (Bogor Selatan)
    # Kita ambil dari CSV yang Anda tunjukkan
    sensors = [
        {'id': 13986082, 'param': 'pm1'},
        {'id': 13986083, 'param': 'pm25'},
        {'id': 13986084, 'param': 'humidity'},
        {'id': 13986085, 'param': 'temperature'},
        {'id': 13986086, 'param': 'pm003_count'}
    ]

    api_key = "c6375c650d0bb95242e8c2dd633602b6ff6f52bd9ebe3e226f448a1e8b078358"
    headers = {"X-API-Key": api_key, "Accept": "application/json"}
    
    # List untuk menampung semua data
    all_data = []

    print("Memulai pengambilan data pengukuran (Measurements)...")
    print("-" * 40)

    for sensor in sensors:
        s_id = sensor['id']
        s_param = sensor['param']
        
        # URL untuk mengambil measurements per sensor
        # limit=1000 artinya kita minta 1000 data terakhir
        url = f"https://api.openaq.org/v3/sensors/{s_id}/measurements?limit=1000"
        
        print(f"Mengambil data untuk Sensor {s_id} ({s_param})...")
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            if 'results' in data:
                # Masukkan data ke list
                for item in data['results']:
                    # Kita ratakan strukturnya manual agar rapi
                    record = {
                        'sensor_id': s_id,
                        'parameter': s_param,
                        'value': item.get('value'),
                        'datetime_utc': item.get('period', {}).get('datetimeTo', {}).get('utc'),
                        'datetime_local': item.get('period', {}).get('datetimeTo', {}).get('local'),
                    }
                    all_data.append(record)
                
                print(f"  -> Berhasil dapat {len(data['results'])} baris.")
            else:
                print("  -> Tidak ada data.")

        except Exception as e:
            print(f"  -> Error pada sensor {s_id}: {e}")
        
        # Jeda sedikit agar tidak dianggap spam oleh server
        time.sleep(1)

    print("-" * 40)
    
    # 2. Konversi ke CSV
    if all_data:
        df = pd.DataFrame(all_data)
        
        # Urutkan berdasarkan waktu
        df['datetime_local'] = pd.to_datetime(df['datetime_local'])
        df = df.sort_values(by=['datetime_local'], ascending=False)
        
        filename = "data_lengkap_bogor_selatan.csv"
        df.to_csv(filename, index=False)
        
        print(f"SELESAI! Data tersimpan di: {filename}")
        print(f"Total baris data yang didapat: {len(df)}")
        print("\nPreview 5 data teratas:")
        print(df.head().to_string())
    else:
        print("Gagal mendapatkan data apapun.")

if __name__ == "__main__":
    fetch_measurements()