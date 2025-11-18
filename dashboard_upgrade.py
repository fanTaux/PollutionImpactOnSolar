import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

# --- CONFIG ---
st.set_page_config(page_title="SolarClear: Pollution Impact", layout="wide", page_icon="☀️")

# --- LOAD DATA ---
@st.cache_data
def load_data():
    try:
        df = pd.read_csv("dataset_final_analisis_pv.csv")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except FileNotFoundError:
        st.error("File 'dataset_final_analisis_pv.csv' tidak ditemukan. Pastikan file ada di folder yang sama.")
        return pd.DataFrame()

df = load_data()

# --- SIDEBAR: CONTROL PANEL ---
st.sidebar.header("🎛️ Kontrol Analisis")

if not df.empty:
    # Filter Tanggal
    min_date = df['timestamp'].min().date()
    max_date = df['timestamp'].max().date()
    start_date, end_date = st.sidebar.date_input("Rentang Tanggal", [min_date, max_date])

    # Filter Cloud Cover
    st.sidebar.subheader("☁️ Filter Cuaca")
    st.sidebar.caption("Geser ke kiri untuk memilih kondisi langit yang lebih cerah (minim awan).")
    cloud_threshold = st.sidebar.slider("Maksimal Tutupan Awan (%)", 0, 100, 30, step=5)
    
    # Filtering Logic
    mask = (
        (df['timestamp'].dt.date >= start_date) & 
        (df['timestamp'].dt.date <= end_date) &
        (df['cloud_cover'] <= cloud_threshold) &
        (df['simulated_power_watt'] > 0) # Hanya siang hari
    )
    df_filtered = df.loc[mask]
else:
    df_filtered = pd.DataFrame()

# --- MAIN PAGE ---
st.title("☀️ Surya Terhalang Polusi")
st.markdown("""
Dashboard ini menganalisis dampak **Polusi Udara** terhadap efisiensi **Panel Surya** di Bogor Selatan.
Tujuannya adalah membuktikan bahwa langit yang terlihat "bersih" dari awan belum tentu menghasilkan listrik maksimal jika terhalang debu polusi.
""")

# --- BAGIAN BARU: KAMUS PARAMETER (INFORMATIF) ---
with st.expander("📖 Kamus Istilah & Parameter (Klik untuk membuka)"):
    col_info1, col_info2 = st.columns(2)
    with col_info1:
        st.markdown("""
        **🌡️ Kualitas Udara & Cuaca:**
        * **PM2.5 (Particulate Matter):** Debu halus berukuran <2.5 mikron. Bisa menghalangi sinar matahari (*Aerosol Optical Depth*). Semakin tinggi, semakin buruk.
        * **Cloud Cover (Tutupan Awan):** Persentase langit yang tertutup awan. Musuh utama panel surya.
        * **Temperature:** Suhu udara. Uniknya, panel surya **tidak suka panas**. Semakin panas suhu, efisiensi panel justru turun.
        """)
    with col_info2:
        st.markdown("""
        **⚡ Energi Surya:**
        * **GHI (Global Horizontal Irradiance):** Total radiasi matahari yang jatuh ke tanah.
        * **DNI (Direct Normal Irradiance):** Sinar matahari **langsung**. Inilah komponen yang paling banyak "dimakan" oleh polusi/jerebu.
        * **Solar Output (Watt):** Simulasi listrik yang dihasilkan panel surya kapasitas 250 Watt dengan kemiringan 10°.
        """)

st.markdown("---")

# 1. KEY METRICS (DENGAN TOOLTIPS)
st.subheader(f"📊 Ringkasan Data (Kondisi Awan ≤ {cloud_threshold}%)")

if not df_filtered.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    avg_power = df_filtered['simulated_power_watt'].mean()
    avg_pm25 = df_filtered['pm25'].mean()
    total_energy = df_filtered['simulated_power_watt'].sum() / 1000 # kWh
    
    # Korelasi
    corr_coeff = df_filtered['pm25'].corr(df_filtered['direct_normal_irradiance'])
    
    col1.metric(
        "Rata-rata Output Listrik", 
        f"{avg_power:.1f} W",
        help="Rata-rata daya listrik yang dihasilkan per jam dalam kondisi filter saat ini."
    )
    col2.metric(
        "Rata-rata Polusi (PM2.5)", 
        f"{avg_pm25:.1f} µg/m³", 
        delta_color="inverse",
        help="Konsentrasi debu halus rata-rata. Di atas 15 µg/m³ dianggap tidak sehat menurut standar WHO baru."
    )
    col3.metric(
        "Total Energi Terkumpul", 
        f"{total_energy:.2f} kWh",
        help="Total akumulasi energi listrik selama periode waktu yang dipilih."
    )
    col4.metric(
        "Korelasi Polusi vs Matahari", 
        f"{corr_coeff:.2f}", 
        help="Angka antara -1 s/d 1. Nilai negatif (misal -0.5) berarti saat Polusi NAIK, Sinar Matahari TURUN. Semakin mendekati -1, semakin kuat efek polusinya."
    )
else:
    st.warning("Tidak ada data yang sesuai filter. Coba naikkan batas 'Tutupan Awan' di sebelah kiri.")

st.markdown("---")

# 2. TIME SERIES CHART
st.subheader("📈 Tren Waktu: Polusi vs Energi")
st.markdown("""
Grafik ini membandingkan **Output Listrik** (Kuning) dengan tingkat **Polusi** (Merah). 
Coba perhatikan pola **puncak (peak)** kuning. Apakah puncaknya menjadi lebih rendah saat garis merah sedang tinggi-tingginya?
""")

if not df_filtered.empty:
    fig_ts = go.Figure()
    
    # Area Listrik
    fig_ts.add_trace(go.Scatter(
        x=df_filtered['timestamp'], 
        y=df_filtered['simulated_power_watt'],
        name="Solar Output (Watt)",
        mode='lines',
        fill='tozeroy',
        line=dict(color='#FFC107', width=1),
        hovertemplate='%{y:.1f} Watt<extra></extra>'
    ))

    # Garis Polusi
    fig_ts.add_trace(go.Scatter(
        x=df_filtered['timestamp'], 
        y=df_filtered['pm25'],
        name="PM2.5 (Polusi)",
        mode='lines',
        line=dict(color='#FF5722', width=2),
        yaxis='y2',
        hovertemplate='%{y:.1f} µg/m³<extra></extra>'
    ))

    fig_ts.update_layout(
        xaxis_title="Waktu",
        yaxis=dict(title=dict(text="Output Daya (Watt)", font=dict(color="#FFC107"))),
        yaxis2=dict(
            title=dict(text="PM2.5 (µg/m³)", font=dict(color="#FF5722")), 
            overlaying='y', 
            side='right'
        ),
        hovermode="x unified",
        height=450,
        legend=dict(orientation="h", y=1.1)
    )
    st.plotly_chart(fig_ts, use_container_width=True)

# 3. SCATTER PLOT & INSIGHT
st.subheader("🔍 Bukti Statistik: Efek 'Redup' Akibat Polusi")

col_left, col_right = st.columns([2, 1])

with col_left:
    if not df_filtered.empty:
        # Cek ketersediaan statsmodels
        try:
            import statsmodels.api as sm
            trendline_mode = "ols"
        except ImportError:
            trendline_mode = None

        fig_scatter = px.scatter(
            df_filtered, 
            x="pm25", 
            y="direct_normal_irradiance", 
            color="temperature_2m",
            trendline=trendline_mode, 
            labels={
                "pm25": "Polusi PM2.5 (µg/m³)", 
                "direct_normal_irradiance": "Kekuatan Sinar Matahari Langsung (W/m²)",
                "temperature_2m": "Suhu Udara (°C)"
            },
            title="Sebaran Data: Polusi vs Sinar Matahari",
            color_continuous_scale="Bluered"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        
        if trendline_mode is None:
            st.caption("Catatan: Garis tren tidak muncul karena library `statsmodels` belum terinstall.")

with col_right:
    st.info("💡 **Cara Membaca Grafik:**")
    st.markdown("""
    1.  **Sumbu X (Mendatar):** Tingkat polusi. Semakin ke kanan, semakin kotor udaranya.
    2.  **Sumbu Y (Tegak):** Kekuatan sinar matahari langsung (DNI).
    3.  **Garis Tren:** Jika garis miring ke **bawah**, berarti polusi terbukti mengurangi intensitas cahaya matahari.
    """)
    
    st.markdown("---")
    
    if not df_filtered.empty:
        # Hitung Regresi Sederhana manual (numpy) untuk insight teks
        try:
            slope, intercept = np.polyfit(df_filtered['pm25'], df_filtered['direct_normal_irradiance'], 1)
            loss_per_10_ug = abs(slope * 10)
            
            st.success(f"📉 **Estimasi Kerugian:**")
            st.markdown(f"""
            Berdasarkan data ini, setiap kenaikan polusi sebesar **10 µg/m³**, 
            intensitas sinar matahari yang hilang adalah sekitar:
            
            ## **{loss_per_10_ug:.1f} W/m²**
            
            *Ini setara dengan kehilangan daya satu buah lampu LED terang per meter persegi panel surya.*
            """)
        except:
            st.write("Data tidak cukup untuk estimasi regresi.")

# 4. FOOTER / RAW DATA
with st.expander("📂 Lihat Data Mentah (Tabel)"):
    st.markdown("Data ini adalah gabungan dari sensor OpenAQ (Polusi) dan Open-Meteo (Cuaca & Solar).")
    st.dataframe(df_filtered)