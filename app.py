import streamlit as st
import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import time

# Configuração da Página
st.set_page_config(page_title="B3 Scanner Server", page_icon="🚀", layout="wide")

# --- INICIALIZAÇÃO FIREBASE ---
def init_firebase():
    if not firebase_admin._apps:
        if "firebase" in st.secrets:
            firebase_creds = dict(st.secrets["firebase"])
            cred = credentials.Certificate(firebase_creds)
            firebase_admin.initialize_app(cred)
        else:
            st.error("Erro: Configure as credenciais do Firebase nas 'Secrets' do Streamlit.")
            return None
    
    # ID do seu banco de dados específico
    database_id = "ai-studio-92eeeca1-1ed5-4536-875f-36a3730ccdfe"
    return firestore.client(database=database_id)

# --- BUSCA DE DADOS VIA REQUESTS ---
def get_yahoo_data(ticker_symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_symbol}.SA?range=1y&interval=1d"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=15)
        data = response.json()
        result = data['chart']['result'][0]
        timestamps = result['timestamp']
        closes = result['indicators']['quote'][0]['close']
        volumes = result['indicators']['quote'][0]['volume']
        lows = result['indicators']['quote'][0]['low']
        highs = result['indicators']['quote'][0]['high']
        
        df = pd.DataFrame({
            'Close': closes, 
            'Volume': volumes,
            'Low': lows,
            'High': highs
        }, index=pd.to_datetime(timestamps, unit='s'))
        return df.dropna()
    except Exception as e:
        return None

# --- LÓGICA DE ANÁLISE ---
def analyze_ticker(ticker_symbol, df):
    if df is None or len(df) < 50:
        return None

    # Médias Móveis
    df['MM9'] = df['Close'].rolling(window=9).mean()
    df['MM20'] = df['Close'].rolling(window=20).mean()
    df['MM50'] = df['Close'].rolling(window=50).mean()
    df['VMed'] = df['Volume'].rolling(window=20).mean()
    
    last = df.iloc[-1]
    price = last['Close']
    mm9 = last['MM9']
    mm20 = last['MM20']
    mm50 = last['MM50']
    v_hoje = last['Volume']
    v_med = last['VMed']
    
    # Sinais
    is_explosive = price > mm20 and price > mm9 and v_hoje > (v_med * 1.2)
    is_compra = price > mm20 and price > mm9
    
    label = "AGUARDAR"
    if is_explosive: label = "EXPLOSIVO"
    elif is_compra: label = "COMPRA"
    elif price > mm9: label = "ATENÇÃO"

    # Variação 3 meses
    change_3m = ((price / df.iloc[-60]['Close']) - 1) * 100 if len(df) > 60 else 0

    return {
        "ticker": ticker_symbol,
        "preco": round(float(price), 2),
        "mms9": round(float(mm9), 2),
        "mms20": round(float(mm20), 2),
        "mms200": round(float(mm50), 2), # Mapeado para MM50 no PWA
        "vHoje": int(v_hoje),
        "vMed": int(v_med),
        "isCompra": bool(is_compra),
        "isExplosive": bool(is_explosive),
        "signalLabel": label,
        "changePerc": round(float(change_3m), 2),
        "updatedAt": datetime.now().isoformat(),
        "score": round(float((price / mm50 - 1) * 100), 2) if mm50 > 0 else 0
    }

# --- UI ---
st.title("🚀 B3 Scanner Server")

# Lista completa de todos os 142 papéis do projeto
ACOES_IBOV = [
    'ALOS3', 'ABEV3', 'ASAI3', 'AURE3', 'AXIA3', 'AXIA6', 'AXIA7', 'AZZA3',
    'B3SA3', 'BBSE3', 'BBDC3', 'BBDC4', 'BRAP4', 'BBAS3', 'BRKM5', 'BRAV3',
    'BPAC11', 'CXSE3', 'CEAB3', 'CMIG4', 'COGN3', 'CSMG3', 'CPLE3', 'CSAN3',
    'CPFE3', 'CMIN3', 'CURY3', 'CYRE3', 'CYRE4', 'DIRR3', 'EMBJ3', 'ENGI11',
    'ENEV3', 'EGIE3', 'EQTL3', 'FLRY3', 'GGBR4', 'GOAU4', 'HAPV3', 'HYPE3',
    'IGTI11', 'IRBR3', 'ISAE4', 'ITSA4', 'ITUB4', 'KLBN11', 'RENT3', 'RENT4',
    'LREN3', 'MGLU3', 'POMO4', 'BEEF3', 'MRVE3', 'MULT3', 'PETR3', 'PETR4', 
    'RECV3', 'PRIO3', 'PSSA3', 'RADL3', 'RAIZ4', 'RDOR3', 'RAIL3', 'SBSP3', 
    'SANB11', 'CSNA3', 'SLCE3', 'SMFT3', 'SUZB3', 'TAEE11', 'VIVT3', 'TIMS3', 
    'TOTS3', 'UGPA3', 'USIM5', 'VALE3', 'VAMO3', 'VBBR3', 'VIVA3', 'WEGE3', 
    'YDUQ3', 'TTEN3', 'ABCB4', 'ALPA4', 'ALUP11', 'ANIM3', 'ARML3', 'AMOB3', 
    'BPAN4', 'BRSR6', 'BMOB3', 'BLAU3', 'SOJA3', 'BRBI11', 'AGRO3', 'CAML3', 
    'BHIA3', 'CBAV3', 'CVCB3', 'DESK3', 'DXCO3', 'PNVL3', 'ECOR3', 'EVEN3', 
    'EZTC3', 'FESA4', 'FRAS3', 'GFSA3', 'GGPS3', 'GRND3', 'GMAT3', 'SBFG3', 
    'GUAR3', 'HBOR3', 'HBSA3', 'INTB3', 'MYPK3', 'RANI3', 'JHSF3', 'JSLG3', 
    'KEPL3', 'LAVV3', 'LOGG3', 'LWSA3', 'MDIA3', 'CASH3', 'LEVE3', 'MILS3', 
    'MOVI3', 'ODPV3', 'ONCO3', 'ORVR3', 'PGMN3', 'PLPL3', 'POSI3', 'PRNR3', 
    'QUAL3', 'LJQQ3', 'RAPT4', 'SAPR11', 'SMTO3', 'SEER3', 'SIMH3', 'SYNE3', 
    'TGMA3', 'TEND3', 'TUPY3', 'UNIP6', 'VLID3', 'VULC3'
]

if st.button("⚡ SINCRONIZAR AGORA"):
    db = init_firebase()
    if db:
        results = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, ticker in enumerate(ACOES_IBOV):
            status_text.text(f"Processando {ticker} ({i+1}/{len(ACOES_IBOV)})...")
            df = get_yahoo_data(ticker)
            data = analyze_ticker(ticker, df)
            if data:
                # Salva no Firestore
                db.collection('market_data').document(ticker).set(data)
                results.append(data)
            
            progress_bar.progress((i + 1) / len(ACOES_IBOV))
            time.sleep(0.05) # Pausa curta para evitar bloqueios
        
        st.success(f"✅ Sincronizado! {len(results)} ativos atualizados no PWA.")
        st.dataframe(pd.DataFrame(results))
