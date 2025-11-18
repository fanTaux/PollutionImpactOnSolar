import pandas as pd
import numpy as np
import math

# --- 1. KONFIGURASI SISTEM PV ---
LAT = -8.53035
LON = 115.26933
PANEL_TILT = 10         # Kemiringan 10 derajat (Biar debu turun kalau hujan)
PANEL_AZIMUTH = 0       # Menghadap Utara (0 derajat) karena kita di Selatan Khatulistiwa
RATED_POWER = 250       # Kapasitas Panel (250 Watt)
TEMP_COEFF = -0.0045    # Efisiensi turun 0.45% setiap naik 1 derajat celcius

def calculate_solar_position(dt, lat, lon):
    """
    Algoritma sederhana untuk estimasi posisi matahari.
    """
    # Konversi waktu ke Day of Year dan Hour
    day_of_year = dt.timetuple().tm_yday
    hour = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
    
    # Equation of Time
    b = 360 * (day_of_year - 81) / 365
    eot = 9.87 * np.sin(np.deg2rad(2 * b)) - 7.53 * np.cos(np.deg2rad(b)) - 1.5 * np.sin(np.deg2rad(b))
    
    # Solar Time correction
    time_offset = 4 * (lon - 105) # 105 adalah meridian WIB (GMT+7)
    solar_time = hour + (time_offset + eot) / 60
    
    # Declination Angle
    declination = 23.45 * np.sin(np.deg2rad(360 * (284 + day_of_year) / 365))
    
    # Hour Angle
    hour_angle = 15 * (solar_time - 12)
    
    # Solar Elevation & Zenith
    lat_rad = np.deg2rad(lat)
    dec_rad = np.deg2rad(declination)
    ha_rad = np.deg2rad(hour_angle)
    
    elevation = np.arcsin(np.sin(lat_rad) * np.sin(dec_rad) + 
                          np.cos(lat_rad) * np.cos(dec_rad) * np.cos(ha_rad))
    zenith = 90 - np.rad2deg(elevation)
    
    # Solar Azimuth
    azimuth_arg = (np.sin(dec_rad) * np.cos(lat_rad) - np.cos(dec_rad) * np.sin(lat_rad) * np.cos(ha_rad)) / np.cos(elevation)
    # Clamp value for arccos safety
    azimuth_arg = np.clip(azimuth_arg, -1.0, 1.0)
    azimuth = np.rad2deg(np.arccos(azimuth_arg))
    
    if hour_angle > 0:
        azimuth = 360 - azimuth
        
    return zenith, azimuth

def process_simulation():
    print("--- Memulai Simulasi PV Manual ---")
    
    # 1. Load Data
    df_aq = pd.read_csv('data_lengkap_bali_ubud.csv')
    df_solar = pd.read_csv('data_solar_bali_openmeteo.csv')
    
    # Preprocess timestamps
    df_aq['datetime_local'] = pd.to_datetime(df_aq['datetime_local'])
    df_solar['date'] = pd.to_datetime(df_solar['date'])
    
    # Pivot AQ Data
    df_aq_pivot = df_aq.pivot_table(index='datetime_local', columns='parameter', values='value', aggfunc='mean').reset_index()
    df_aq_pivot['timestamp'] = df_aq_pivot['datetime_local'].dt.tz_localize(None)
    
    df_solar['timestamp'] = df_solar['date'].dt.tz_localize(None)
    
    # Merge
    df = pd.merge(df_aq_pivot, df_solar, on='timestamp', how='inner')
    
    # Lists for calculation results
    poa_list = []
    power_list = []
    
    # 2. Loop per jam untuk hitung Fisika Surya
    for index, row in df.iterrows():
        # Ambil data cuaca
        ghi = row['shortwave_radiation'] # Global
        dni = row['direct_normal_irradiance'] # Direct
        dhi = row['diffuse_radiation'] # Diffuse
        temp_amb = row['temperature_2m'] # Suhu Udara
        
        if pd.isna(ghi) or ghi <= 0:
            poa_list.append(0)
            power_list.append(0)
            continue
            
        # Hitung Posisi Matahari
        zenith, azimuth = calculate_solar_position(row['timestamp'], LAT, LON)
        
        # Hitung Angle of Incidence (AOI) - Sudut datang sinar ke panel
        # Rumus geometri panel miring
        tilt_rad = np.deg2rad(PANEL_TILT)
        azimuth_panel_rad = np.deg2rad(PANEL_AZIMUTH)
        zenith_rad = np.deg2rad(zenith)
        azimuth_solar_rad = np.deg2rad(azimuth)
        
        # Cos(AOI)
        cos_aoi = (np.cos(zenith_rad) * np.cos(tilt_rad) + 
                   np.sin(zenith_rad) * np.sin(tilt_rad) * np.cos(azimuth_solar_rad - azimuth_panel_rad))
        
        # Hitung Sinar yang sampai ke Panel (Plane of Array Irradiance)
        # Komponen 1: Sinar Langsung (Beam)
        poa_beam = dni * max(0, cos_aoi)
        
        # Komponen 2: Sinar Sebar (Diffuse) - Model Isotropic Sederhana
        poa_diffuse = dhi * ((1 + np.cos(tilt_rad)) / 2)
        
        # Komponen 3: Pantulan Tanah (Ground Reflected) - Albedo 0.2
        poa_ground = ghi * 0.2 * ((1 - np.cos(tilt_rad)) / 2)
        
        poa_total = poa_beam + poa_diffuse + poa_ground
        poa_list.append(poa_total)
        
        # 3. Hitung Output Daya (Watt)
        # Hitung suhu sel panel (Panel lebih panas dari udara sekitar)
        # Approx: Tcell = Tair + (NOCT-20)/800 * S
        t_cell = temp_amb + (45 - 20) / 800 * poa_total
        
        # Koreksi efisiensi karena panas
        temp_correction = 1 + TEMP_COEFF * (t_cell - 25)
        
        # Rumus Daya DC
        dc_output = RATED_POWER * (poa_total / 1000) * temp_correction
        
        # Efisiensi Inverter (misal 96%)
        ac_output = dc_output * 0.96
        power_list.append(max(0, ac_output))

    df['poa_irradiance'] = poa_list
    df['simulated_power_watt'] = power_list
    
    # Simpan
    output_file = "dataset_final_analisis_pv.csv"
    df.to_csv(output_file, index=False)
    print(f"SELESAI! File analisis tersimpan di: {output_file}")
    print(df[['timestamp', 'pm25', 'simulated_power_watt']].head())

if __name__ == "__main__":
    process_simulation()