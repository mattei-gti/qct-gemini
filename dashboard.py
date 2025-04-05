# quantis_crypto_trader_gemini/dashboard.py

import streamlit as st
import pandas as pd
from redis_client import RedisHandler # Para ler estado e cache
import config # Para configurações do Redis
from binance.client import Client # Para constantes de intervalo se necessário
import datetime
from io import StringIO # Para corrigir o FutureWarning do pandas

# --- Configuração da Página ---
st.set_page_config(
    page_title="Quantis Crypto Trader Dashboard",
    page_icon="🤖",
    layout="wide", # Usa a largura total da página
    initial_sidebar_state="expanded" # Mantém a sidebar aberta inicialmente
)

# --- Funções Auxiliares ---

@st.cache_resource # Cacheia a conexão Redis para performance
def get_redis_handler():
    """Cria e retorna uma instância do RedisHandler."""
    try:
        handler = RedisHandler(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB
        )
        return handler
    except Exception as e:
        st.error(f"Erro ao conectar ao Redis: {e}. Verifique se o Redis está rodando e as configs estão corretas.")
        return None

# Função para carregar e preparar dados do cache Redis
# @st.cache_data(ttl=60) # Cacheia os dados por 60s para evitar leituras excessivas
def load_data_from_redis(redis_handler: RedisHandler, symbol: str, interval: str):
    """Carrega estado e klines do Redis."""
    if not redis_handler:
        return None, "N/A" # Retorna None para klines, N/A para estado

    # Carrega Estado da Posição
    position_state_key = f"state:position_asset:{symbol}"
    current_asset = redis_handler.get_state(position_state_key)
    if current_asset is None:
        # Se não encontrar, pode assumir USDT ou indicar 'Indefinido'
        current_asset = "USDT (Inicial/Indefinido)"

    # Carrega Klines do Cache
    kline_cache_key = redis_handler._generate_kline_key(symbol, interval)
    klines_df = redis_handler.get_dataframe(kline_cache_key)

    return klines_df, current_asset

# --- Inicialização ---
redis_h = get_redis_handler()

# --- Sidebar (Configurações/Controles - por enquanto, só informação) ---
st.sidebar.title("Configurações")
st.sidebar.info(f"Monitorando: **BTCUSDT** (Intervalo: 1h)") # Hardcoded por enquanto
# Adicionar mais opções aqui depois (seleção de par, intervalo, etc.)
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Conexão Redis:** {'Conectado' if redis_h else 'Falha'}")
st.sidebar.markdown(f"Host: `{config.REDIS_HOST}` Port: `{config.REDIS_PORT}` DB: `{config.REDIS_DB}`")


# --- Layout Principal ---
st.title("🤖 Quantis Crypto Trader - Dashboard")
st.caption(f"Última atualização: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# --- Carregar Dados ---
# Parâmetros hardcoded por enquanto, idealmente viriam de config ou UI
symbol_display = "BTCUSDT"
interval_display = Client.KLINE_INTERVAL_1HOUR # ou '1h'

klines_data, position_status = load_data_from_redis(redis_h, symbol_display, interval_display)

# Botão para forçar atualização (opcional)
if st.button("Atualizar Dados"):
    st.cache_data.clear() # Limpa o cache de dados do Streamlit para forçar releitura
    st.rerun() # Recarrega a página

# --- Exibição do Status ---
col1, col2 = st.columns(2)
with col1:
    st.metric(label="Status da Posição", value=position_status)
with col2:
    # Placeholder para outras métricas (ex: último sinal, P&L)
    st.metric(label="Último Sinal (Placeholder)", value="N/A")


# --- Exibição dos Dados de Kline ---
st.markdown("---")
st.subheader(f"Dados de Mercado Recentes (Cache Redis) - {symbol_display} ({interval_display})")

if klines_data is not None and not klines_data.empty:
    # Gráfico de Preços de Fechamento
    st.line_chart(klines_data, x='Open time', y='Close')

    # Tabela com os dados recentes
    st.dataframe(klines_data.sort_values(by='Open time', ascending=False), use_container_width=True)

    st.success("Dados de Klines carregados do cache Redis.")
else:
    st.warning(f"Nenhum dado de Kline encontrado no cache Redis para a chave: klines:{symbol_display}:{interval_display}")
    st.info("Execute o robô (`main.py`) para que ele busque os dados e os coloque no cache.")


# --- Placeholder para Histórico de Trades/Logs ---
st.markdown("---")
st.subheader("Histórico de Trades / Logs (Placeholder)")
st.text("Área para exibir trades simulados/reais e logs importantes.")