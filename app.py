import streamlit as st
import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import time

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="B3 Scanner Server", page_icon="🚀", layout="wide")

# --- INICIALIZAÇÃO FIREBASE ---
def init_firebase():
    if not firebase_admin._apps:
        if "firebase" in st.secrets:
            # Criamos uma cópia para não alterar o st.secrets original
            creds_dict = dict(st.secrets["firebase"])
            
            # TRATAMENTO CRÍTICO DA CHAVE PRIVADA
            # Remove escapes de string que o TOML ou o JSON podem ter inserido
            if "private_key" in creds_dict:
                creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
            
            try:
                cred = credentials.Certificate(creds_dict)
                firebase_admin.initialize_app(cred)
            except Exception as e:
                st.error(f"❌ Erro na certificação: {e}")
                return None
        else:
            st.error("❌ Erro: Configure [firebase] nas Secrets.")
            return None
    
    # Nome do banco (Named Database)
    db_id = "ai-studio-92eeeca1-1ed5-4536-875f-36a3730ccdfe"
    
    try:
        # Usando database_id conforme a versão nova da biblioteca
        return firestore.client(database_id=db_id)
    except Exception as e:
        st.warning(f"⚠️ Falha no banco {db_id}, tentando (default). Erro: {e}")
        return firestore.client()

# --- BUSCA DE DADOS ---
def get_yahoo_data(ticker_symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_symbol}.SA?range=1y&interval=1d"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        data = response.json()
        result = data['chart']['result'][0]
        df = pd.DataFrame({
            'Close': result['indicators']['quote'][0]['close'], 
            'Volume': result['indicators']['quote'][0]['volume'],
            'Low': result['indicators']['quote'][0]['low'],
            'High': result['indicators']['quote'][0]['high']
        }, index=pd.to_datetime(result['timestamp'], unit='s'))
        return df.dropna()
    except:
        return None

# --- LÓGICA DE ANÁLISE ---
def analyze_ticker(ticker, df):
    if df is None or len(df) < 50: return None
    
    df['MM9'] = df['Close'].rolling(9).mean()
    df['MM20'] = df['Close'].rolling(20).mean()
    df['MM50'] = df['Close'].rolling(50).mean()
    df['VMed'] = df['Volume'].rolling(20).mean()
    
    last = df.iloc[-1]
    is_explosive = last['Close'] > last['MM20'] and last['Volume'] > (last['VMed'] * 1.2)
    
    return {
        "ticker": ticker,
        "preco": round(float(last['Close']), 2),
        "mms9": round(float(last['MM9']), 2),
        "mms20": round(float(last['MM20']), 2),
        "mms200": round(float(last['MM50']), 2),
        "signalLabel": "EXPLOSIVO" if is_explosive else "COMPRA" if last['Close'] > last['MM20'] else "AGUARDAR",
        "updatedAt": datetime.now().isoformat()
    }

# --- UI ---
st.title("🚀 B3 Scanner Server")

ACOES_B3 = ['ALOS3', 'ABEV3', 'ASAI3', 'AURE3', 'B3SA3', 'BBSE3', 'BBDC4', 'BBAS3', 'PETR4', 'VALE3', 'ITUB4'] # Lista reduzida para teste

if st.button("⚡ SINCRONIZAR"):
    db = init_firebase()
    if db:
        progress = st.progress(0)
        batch = db.batch()
        count = 0
        
        for i, ticker in enumerate(ACOES_B3):
            df = get_yahoo_data(ticker)
            data = analyze_ticker(ticker, df)
            if data:
                doc_ref = db.collection('market_data').document(ticker)
                batch.set(doc_ref, data)
                count += 1
            progress.progress((i + 1) / len(ACOES_B3))
            time.sleep(0.1)
        
        if count > 0:
            try:
                batch.commit()
                st.success(f"✅ {count} ativos sincronizados!")
            except Exception as e:
                st.error(f"❌ Erro no Commit: {e}")
                st.info("Dica: Verifique se o seu 'Service Account' tem permissão de 'Cloud Datastore User' no Google Cloud.")
