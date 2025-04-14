# quantis_crypto_trader_gemini/find_patterns.py

import logging
import sys
import pandas as pd
import pandas_ta as ta
import numpy as np
import datetime
import time
import config
from redis_client import RedisHandler
from binance.client import Client
import os # Para salvar CSV

# --- Configuração do Logging ---
LOG_FILE_PATTERNS = "find_patterns_run.log"
def setup_find_patterns_logging(level=logging.INFO):
    # ... (código setup_find_patterns_logging como antes) ...
    bt_logger = logging.getLogger('find_patterns'); # ... (limpeza e handlers) ...
    for handler in bt_logger.handlers[:]: bt_logger.removeHandler(handler); handler.close()
    bt_logger.setLevel(level); formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    try: fh = logging.FileHandler(LOG_FILE_PATTERNS, mode='w', encoding='utf-8'); fh.setFormatter(formatter); bt_logger.addHandler(fh)
    except Exception as e: print(f"Erro config FileHandler find_patterns: {e}")
    ch = logging.StreamHandler(sys.stdout); ch.setFormatter(formatter); bt_logger.addHandler(ch); bt_logger.propagate = False
    bt_logger.info("--- Logging do Find Patterns configurado ---"); return bt_logger

logger = setup_find_patterns_logging(level=logging.INFO)

# --- Parâmetros da Análise ---
SYMBOL = "BTCUSDT"; INTERVAL_CODE = Client.KLINE_INTERVAL_15MINUTE; INTERVAL_LABEL = "15m"
START_DATE_STR = "1 Jan, 2023"; END_DATE_STR = None
PROFIT_TARGET = 1.02 # 2%
LOOKAHEAD_CANDLES = 24 # 6 horas (15m * 24)

# Parâmetros dos indicadores (CORRIGIDOS)
sma_params = {'fast': 30, 'slow': 60}
ichi_params = {'t': 21, 'k': 34, 's': 52}
rsi_params = {'length': 14}
macd_params = {'fast': 12, 'slow': 26, 'signal': 9}
bbands_params = {'length': 20, 'std': 2.0}
atr_params = {'length': 14}
min_klines_needed_hist = max(sma_params['slow'], ichi_params['s'], 26, 20, 14) + 1

# --- Função Principal ---
def find_profitable_entries():
    logger.info("==== INICIANDO ANÁLISE DE PADRÕES HISTÓRICOS (PRIMEIRO DO GRUPO) ====")
    # ... (Logs iniciais como antes) ...
    logger.info(f"Par: {SYMBOL}, Intervalo: {INTERVAL_LABEL}, Período: '{START_DATE_STR}'->'{END_DATE_STR if END_DATE_STR else 'Fim Redis'}'")
    logger.info(f"Alvo: >= { (PROFIT_TARGET - 1) * 100:.1f}%, Lookahead: {LOOKAHEAD_CANDLES} velas")

    # 1. Inicializar Redis Handler
    try: redis_handler = RedisHandler(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
    except Exception as e: logger.critical("Falha inicializar RedisHandler.", exc_info=True); return

    # 2. Calcular Timestamps
    try: # ... (cálculo start_ts_ms e end_ts_ms como antes) ...
        start_dt = pd.to_datetime(START_DATE_STR, utc=True); start_ts_ms = int(start_dt.timestamp() * 1000)
        end_ts_ms = int(time.time() * 1000) if END_DATE_STR is None else int(pd.to_datetime(END_DATE_STR, utc=True).timestamp() * 1000)
        logger.info(f"Range Timestamps: {start_ts_ms} a {end_ts_ms}")
    except Exception as e: logger.critical("Erro converter datas.", exc_info=True); return

    # 3. Buscar Dados Históricos do Redis
    logger.info(f"Buscando dados {INTERVAL_LABEL} do Redis...")
    data = redis_handler.get_hist_klines_range(symbol=SYMBOL, interval=INTERVAL_CODE, start_ts_ms=start_ts_ms, end_ts_ms=end_ts_ms)
    if data is None or data.empty: logger.critical(f"Dados {INTERVAL_LABEL} não encontrados no Redis p/ período."); return
    logger.info(f"Total de {len(data)} velas {INTERVAL_LABEL} obtidas do Redis ({data.index.min()} a {data.index.max()}).")

    # 4. Calcular TODOS os Indicadores (CORRIGIDO para ATR absoluto)
    logger.info("Calculando indicadores técnicos para todo o período...")
    try:
        data.ta.sma(length=sma_params['fast'], append=True)
        data.ta.sma(length=sma_params['slow'], append=True)
        data.ta.rsi(length=rsi_params['length'], append=True)
        data.ta.macd(fast=macd_params['fast'], slow=macd_params['slow'], signal=macd_params['signal'], append=True)
        data.ta.obv(append=True)
        data.ta.ichimoku(tenkan=ichi_params['t'], kijun=ichi_params['k'], senkou=ichi_params['s'], append=True)
        data.ta.bbands(length=bbands_params['length'], std=bbands_params['std'], append=True)
        data.ta.atr(length=atr_params['length'], append=True) # <<< Gera ATR_14 (absoluto) por padrão
        data.ta.vwap(append=True)
        data.dropna(inplace=True)
        if data.empty: logger.critical("Sem dados após cálculo inds."); return
        logger.info(f"Indicadores calculados. {len(data)} velas restantes para análise.")
    except Exception as e: logger.critical("Erro ao calcular indicadores.", exc_info=True); return

    # 5. Iterar e Encontrar APENAS a PRIMEIRA Entrada Lucrativa de uma Sequência
    logger.info("Procurando por pontos de entrada (PRIMEIRO DO GRUPO) que atingiram >= 2% de lucro...")
    successful_entries_indicators = []
    processed_count = 0; log_interval = 10000
    # <<< NOVO: Flag para rastrear se a vela anterior já foi uma entrada lucrativa >>>
    is_prev_candle_profitable = False

    # Itera do início dos dados válidos até (Total - Janela Futura)
    for i in range(len(data) - LOOKAHEAD_CANDLES):
        processed_count += 1
        if processed_count % log_interval == 0: logger.info(f"Processando vela {processed_count}/{len(data) - LOOKAHEAD_CANDLES}...")

        entry_candle_time = data.index[i]
        entry_price = data['Close'].iloc[i]
        target_price = entry_price * PROFIT_TARGET

        lookahead_window = data['High'].iloc[i + 1 : i + 1 + LOOKAHEAD_CANDLES]

        # Verifica se a vela ATUAL levaria ao lucro
        is_current_candle_profitable = (lookahead_window >= target_price).any()

        # <<< NOVA LÓGICA: Só registra se a ATUAL é lucrativa E a ANTERIOR NÃO foi >>>
        if is_current_candle_profitable and not is_prev_candle_profitable:
            logger.debug(f"PRIMEIRA Entrada lucrativa em sequencia encontrada em {entry_candle_time} (Preço: {entry_price:.2f}, Alvo: {target_price:.2f})")
            indicators_snapshot = data.iloc[i].to_dict()
            indicators_snapshot['entry_timestamp'] = entry_candle_time
            successful_entries_indicators.append(indicators_snapshot)

        # Atualiza o estado da vela anterior para a próxima iteração
        is_prev_candle_profitable = is_current_candle_profitable
        # <<< FIM DA NOVA LÓGICA >>>

    logger.info(f"Análise concluída. {len(successful_entries_indicators)} PONTOS DE ENTRADA *INICIAIS* LUCRATIVOS encontrados.")

    # 6. Salvar Resultados
    if successful_entries_indicators:
        try:
            results_df = pd.DataFrame(successful_entries_indicators)
            output_filename = f"FIRST_successful_entries_{SYMBOL}_{INTERVAL_LABEL}_profit{int((PROFIT_TARGET-1)*100)}pct_lookahead{LOOKAHEAD_CANDLES}.csv"
            results_df.to_csv(output_filename, index=False)
            logger.info(f"Resultados (primeira entrada do grupo) salvos em: {output_filename}")
        except Exception as e: logger.error("Erro ao salvar resultados em CSV.", exc_info=True)
    else: logger.info("Nenhuma entrada lucrativa inicial encontrada com os critérios definidos.")

    logger.info("==== ANÁLISE DE PADRÕES (PRIMEIRO DO GRUPO) CONCLUÍDA ====")

# --- Execução ---
if __name__ == "__main__":
    try: import pandas_ta as ta
    except ImportError: print("Erro: pandas-ta não encontrado. Instale: pip install pandas-ta"); sys.exit(1)
    find_profitable_entries()