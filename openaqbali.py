import requests
import pandas as pd
import time

def fetch_measurements_bali_pagination():
    # --- KONFIGURASI SENSOR BALI (UBUD) ---
    sensors = [
        {'id': 13397854, 'param': 'pm1'},
        {'id': 13397855, 'param': 'pm25'},
        {'id': 13397856, 'param': 'relativehumidity'},
        {'id': 13397857, 'param': 'temperature'},
        {'id': 13397858, 'param': 'um003'}
    ]

    api_key = "c6375c650d0bb95242e8c2dd633602b6ff6f52bd9ebe3e226f448a1e8b078358"
    headers = {"X-API-Key": api_key, "Accept": "application/json"}
    
    all_data = []

    print("Memulai pengambilan data Bali (Ubud) dengan Pagination...")
    print("-" * 50)

    for sensor in sensors:
        s_id = sensor['id']
        s_param = sensor['param']
        print(f"Mengambil data Sensor: {s_id} ({s_param})")
        
        page = 1
        total_sensor_data = 0
        
        while True:
            # Limit kita set 1000 (Maksimum aman), tapi kita mainkan 'page'
            url = f"https://api.openaq.org/v3/sensors/{s_id}/measurements?limit=1000&page={page}"
            
            try:
                response = requests.get(url, headers=headers)
                
                # Jika error 422/dll, berhenti loop sensor ini
                if response.status_code != 200:
                    print(f"   -> Stop di Page {page}. Status: {response.status_code}")
                    break

                data = response.json()
                
                if 'results' in data and len(data['results']) > 0:
                    results = data['results']
                    count_batch = len(results)
                    
                    for item in results:
                        record = {
                            'sensor_id': s_id,
                            'parameter': s_param,
                            'value': item.get('value'),
                            'datetime_utc': item.get('period', {}).get('datetimeTo', {}).get('utc'),
                            'datetime_local': item.get('period', {}).get('datetimeTo', {}).get('local'),
                        }
                        all_data.append(record)
                    
                    total_sensor_data += count_batch
                    print(f"   -> Page {page}: Dapat {count_batch} data. (Total sementara: {total_sensor_data})")
                    
                    # Jika data yang didapat kurang dari limit (1000), berarti ini halaman terakhir
                    if count_batch < 1000:
                        print("   -> Ini halaman terakhir.")
                        break
                    
                    # Lanjut ke halaman berikutnya
                    page += 1
                    time.sleep(0.5) # Jeda agar tidak kena rate limit
                    
                else:
                    print("   -> Tidak ada data lagi.")
                    break

            except Exception as e:
                print(f"   -> Error: {e}")
                break
        
        print("-" * 30)

    # 2. Simpan CSV
    if all_data:
        df = pd.DataFrame(all_data)
        
        # Formatting & Sorting
        df['datetime_local'] = pd.to_datetime(df['datetime_local'])
        df = df.sort_values(by=['datetime_local'], ascending=False)
        
        filename = "data_lengkap_bali_ubud.csv"
        df.to_csv(filename, index=False)
        
        print("=" * 50)
        print(f"SELESAI! Data tersimpan di: {filename}")
        print(f"Total seluruh data baris: {len(df)}")
        print("=" * 50)
    else:
        print("Gagal total mendapatkan data.")

if __name__ == "__main__":
    fetch_measurements_bali_pagination()