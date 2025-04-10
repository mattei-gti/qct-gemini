# quantis_crypto_trader_gemini/main.py

# --- Imports Principais ---
import config
from database import init_db
from binance_client import BinanceHandler
from binance.client import Client
from redis_client import RedisHandler
from gemini_analyzer import GeminiAnalyzer
from telegram_interface import send_telegram_message # Texto Simples
from strategy import StrategyManager
import pandas as pd
import pandas_ta as ta
import datetime
import schedule
import time
import logging
import sys

# --- Configuração do Logging ---
LOG_FILE = "quantis_trader.log"
def setup_logging(level=logging.INFO):
    # ... (código setup_logging como antes) ...
    root_logger = logging.getLogger(); # ... (limpeza e handlers como antes) ...
    if root_logger.hasHandlers():
        for handler in root_logger.handlers[:]: root_logger.removeHandler(handler); handler.close()
    logging.basicConfig(level=level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S', handlers=[logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'), logging.StreamHandler(sys.stdout)])
    logger_cfg = logging.getLogger(); logger_cfg.info("--- Logging reconfigurado ---")
    logging.getLogger('urllib3').setLevel(logging.WARNING); logging.getLogger('requests').setLevel(logging.WARNING); logging.getLogger('schedule').setLevel(logging.WARNING)

# --- Handlers Globais ---
binance_handler: BinanceHandler | None = None; redis_handler: RedisHandler | None = None; gemini_analyzer: GeminiAnalyzer | None = None; strategy_manager: StrategyManager | None = None
logger = logging.getLogger(__name__)

# --- Função Auxiliar de Duração de Intervalo ---
def get_interval_ms(interval: str) -> int | None:
    # ... (código get_interval_ms como antes) ...
    multipliers = {'m': 60, 'h': 3600, 'd': 86400, 'w': 604800, 'M': 2592000}; 
    try:
        unit = interval[-1].lower(); value = int(interval[:-1])
        if unit in multipliers:
            if unit == 'M': return value * 30 * 24 * 60 * 60 * 1000
            elif unit == 'w': return value * 7 * 24 * 60 * 60 * 1000
            else: return value * multipliers[unit] * 1000
    except Exception:
        if interval == Client.KLINE_INTERVAL_1MINUTE: return 60*1000;
        elif interval == Client.KLINE_INTERVAL_15MINUTE: return 15*60*1000;
        elif interval == Client.KLINE_INTERVAL_1HOUR: return 60*60*1000;
        elif interval == Client.KLINE_INTERVAL_4HOUR: return 4*60*60*1000;
        elif interval == Client.KLINE_INTERVAL_1DAY: return 24*60*60*1000;
        elif interval == Client.KLINE_INTERVAL_1WEEK: return 7*24*60*60*1000;
        elif interval == Client.KLINE_INTERVAL_1MONTH: return 30*24*60*60*1000
    logger.warning(f"Duração ms desconhecida: {interval}"); return None

# --- Funções de Inicialização e Ciclo de Trade ---
def initialize_services():
    # ... (código initialize_services como antes) ...
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager
    logger.info("Inicializando serviços..."); 
    try:
        init_db(); config.load_or_set_initial_db_settings()
        redis_handler = RedisHandler(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
        binance_handler = BinanceHandler(api_key=config.BINANCE_API_KEY, api_secret=config.BINANCE_SECRET_KEY)
        gemini_analyzer = GeminiAnalyzer(api_key=config.GEMINI_API_KEY)
        strategy_manager = StrategyManager(redis_handler=redis_handler, binance_handler=binance_handler)
        logger.info("Todos serviços inicializados."); return True
    except Exception as e: logger.critical("Erro CRÍTICO inicialização.", exc_info=True); 
    try: send_telegram_message(f"ERRO INICIALIZACAO:\n{str(e)}"[:4000]) 
    except Exception: pass; return False

def calculate_indicators(df: pd.DataFrame, sma_p: dict, ichi_p: dict) -> dict:
    # ... (código calculate_indicators como antes, usando 'ta' importado no topo) ...
    indicators = {}; # ... (resto como antes) ...
    if df is None or df.empty: return indicators
    required_len = max(sma_p['slow'], ichi_p['s'], 26, 20, 14) + 1
    if len(df) < required_len: logger.warning(f"Dados insuficientes ({len(df)}) p/ inds (~{required_len})."); return {}
    logger.debug(f"Calculando inds DF {len(df)}L..."); 
    try:
        df.ta.sma(length=sma_p['fast'], append=True); df.ta.sma(length=sma_p['slow'], append=True); df.ta.rsi(append=True); df.ta.macd(append=True); df.ta.obv(append=True); df.ta.ichimoku(tenkan=ichi_p['t'], kijun=ichi_p['k'], senkou=ichi_p['s'], append=True); df.ta.bbands(append=True); df.ta.atr(append=True); df.ta.vwap(append=True)
        last = df.iloc[-1]
        def get_ind(row, key, decimals=2): return round(row[key], decimals) if pd.notna(row.get(key)) else None
        indicators['sma_fast'] = get_ind(last, f"SMA_{sma_p['fast']}"); indicators['sma_slow'] = get_ind(last, f"SMA_{sma_p['slow']}"); indicators['rsi'] = get_ind(last, "RSI_14")
        macd_fast, macd_slow, macd_sig = 12, 26, 9; indicators['macd_line'] = get_ind(last, f'MACD_{macd_fast}_{macd_slow}_{macd_sig}'); indicators['macd_hist'] = get_ind(last, f'MACDh_{macd_fast}_{macd_slow}_{macd_sig}'); indicators['macd_signal'] = get_ind(last, f'MACDs_{macd_fast}_{macd_slow}_{macd_sig}')
        indicators['obv'] = get_ind(last, "OBV", 0)
        ichi_t, ichi_k, ichi_s = ichi_p['t'], ichi_p['k'], ichi_p['s']; indicators['ichi_tenkan'] = get_ind(last, f'ITS_{ichi_t}'); indicators['ichi_kijun'] = get_ind(last, f'IKS_{ichi_k}'); indicators['ichi_senkou_a'] = get_ind(last, f'ISA_{ichi_t}'); indicators['ichi_senkou_b'] = get_ind(last, f'ISB_{ichi_s}')
        bb_len, bb_std = 20, 2.0; indicators['bb_lower'] = get_ind(last, f'BBL_{bb_len}_{bb_std}'); indicators['bb_middle'] = get_ind(last, f'BBM_{bb_len}_{bb_std}'); indicators['bb_upper'] = get_ind(last, f'BBU_{bb_len}_{bb_std}')
        atr_len = 14; indicators['atr'] = get_ind(last, f'ATR_{atr_len}', 4)
        indicators['vwap'] = get_ind(last, 'VWAP_D')
    except Exception as e: logger.error("Erro calcular inds TA.", exc_info=True); return {}
    final_indicators = {k: v for k, v in indicators.items() if v is not None}; logger.debug(f"Inds calculados (nao nulos): {list(final_indicators.keys())}"); return final_indicators


def trade_cycle():
    """Executa um ciclo completo: Atualiza Histórico -> Busca Recente -> Calcula TAs -> Analisa -> Decide."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager
    if not all([binance_handler, redis_handler, gemini_analyzer, strategy_manager]): logger.error("Serviços não inicializados. Abortando ciclo."); return

    start_cycle_time = datetime.datetime.now()
    logger.info(f"--- Iniciando Ciclo de Trade (MTA + Indicadores) em {start_cycle_time.strftime('%Y-%m-%d %H:%M:%S')} ---")

    symbol = strategy_manager.symbol; analysis_interval = Client.KLINE_INTERVAL_1HOUR # Intervalo de referência
    mta_intervals = {"1M": Client.KLINE_INTERVAL_1MONTH, "1d": Client.KLINE_INTERVAL_1DAY, "1h": Client.KLINE_INTERVAL_1HOUR, "15m": Client.KLINE_INTERVAL_15MINUTE, "1m": Client.KLINE_INTERVAL_1MINUTE}
    sma_params = {'fast': 30, 'slow': 60}; ichi_params = {'t': 21, 'k': 34, 's': 52}
    min_klines_needed = max(sma_params['slow'], ichi_params['s'], 26, 20, 14) + 50
    mta_data_for_gemini = {}; all_data_available = True

    try:
        # --- PASSO 1: Atualização Incremental ---
        logger.info("--- Iniciando Atualização Histórico Redis ---")
        # ... (código da atualização incremental CORRIGIDO como na última versão) ...
        for tf_label, tf_interval in mta_intervals.items():
             logger.debug(f"Atualizando {symbol}/{tf_label}..."); last_ts_ms = redis_handler.get_last_hist_timestamp(symbol, tf_interval); start_fetch_str = None
             if last_ts_ms: interval_ms = get_interval_ms(tf_interval);
             else: logger.error(f"HISTÓRICO BASE {tf_label} NÃO ENCONTRADO!"); all_data_available = False; continue
             if interval_ms: start_fetch_ts_ms = last_ts_ms + interval_ms; now_ms = int(time.time() * 1000);
             else: logger.warning(f"Duração {tf_label} desconhecida."); continue
             if start_fetch_ts_ms < now_ms - 10000: start_fetch_str = str(start_fetch_ts_ms); logger.info(f"Verificando velas {tf_label} desde {pd.to_datetime(start_fetch_ts_ms, unit='ms')}...")
             else: logger.info(f"Histórico {tf_label} atualizado."); continue
             if start_fetch_str:
                 try:
                     new_klines_df = binance_handler.get_klines(symbol=symbol, interval=tf_interval, start_str=start_fetch_str, limit=1000)
                     if new_klines_df is not None and not new_klines_df.empty:
                         logger.info(f"{len(new_klines_df)} novas velas {tf_label} encontradas.")
                         try: # Garante índice
                             if not isinstance(new_klines_df.index, pd.DatetimeIndex):
                                 if 'Open time' in new_klines_df.columns: new_klines_df['Open time'] = pd.to_datetime(new_klines_df['Open time'], unit='ms'); new_klines_df.set_index('Open time', inplace=True)
                                 else: logger.error(f"Coluna 'Open time' nao encontrada {tf_label}."); continue
                             redis_handler.add_klines_to_hist(symbol, tf_interval, new_klines_df)
                         except Exception as idx_err: logger.error(f"Erro processar/add klines {tf_label}.", exc_info=True)
                     elif new_klines_df is not None: logger.info(f"Nenhuma vela nova para {tf_label}.")
                 except Exception as fetch_err: logger.error(f"Erro busca/add incremental {tf_label}.", exc_info=True)
             time.sleep(0.2)
        logger.info("--- Concluída Atualização Histórico Redis ---")

        # --- PASSO 2: Busca Dados Recentes e Calcula Indicadores ---
        logger.info(f"--- Buscando Dados Recentes e Calculando Indicadores ({min_klines_needed} velas) ---")
        for tf_label, tf_interval in mta_intervals.items():
            # ... (código busca dados e chama calculate_indicators como antes) ...
            indicators = {}; df = None; 
            try:
                df = redis_handler.get_last_n_hist_klines(symbol, tf_interval, min_klines_needed)
                if df is not None and not df.empty:
                    indicators = calculate_indicators(df.copy(), sma_params, ichi_params)
                    if not indicators and len(df) >= min_klines_needed: logger.warning(f"Falha calc inds {tf_label} c/ {len(df)} velas."); all_data_available = False
                else: logger.warning(f"Falha carregar dados {tf_label} Redis."); all_data_available = False
            except Exception as e: logger.error(f"Erro buscar/calc inds {tf_label}.", exc_info=True); all_data_available = False
            mta_data_for_gemini[tf_label] = indicators # Guarda só inds
            if not indicators: logger.warning(f"Dict inds vazio para {tf_label}.")

        # --- PASSO 3: Análise Gemini ---
        # *** CORREÇÃO: Recebe tupla (sinal, justificativa) ***
        trade_signal: str | None = None
        justification: str | None = None

        if all_data_available:
            logger.info(f"Enviando dados MTA+Indicadores para análise Gemini ({gemini_analyzer.model_name})...")
            # Recebe a tupla
            signal_tuple = gemini_analyzer.get_trade_signal_mta_indicators(mta_data_for_gemini, symbol)
            if signal_tuple:
                trade_signal, justification = signal_tuple # Desempacota
            logger.info(f"Sinal Obtido Gemini (MTA+Ind): {trade_signal if trade_signal else 'Nenhum/Erro'}")
            if justification:
                 logger.info(f"Justificativa Gemini: {justification}")

            # Envia mensagem Telegram com sinal e justificativa (se houver)
            if trade_signal:
                message = f"Sinal MTA+Ind ({symbol} - {analysis_interval}): {trade_signal}"
                if justification:
                    message += f"\nJustif: {justification}" # Adiciona justificativa
                send_telegram_message(message)
        else:
            logger.warning("Análise Gemini ignorada: faltaram dados ou indicadores.")
            send_telegram_message(f"Alerta ({symbol}): Falha carregar/calcular TFs p/ analise MTA.", disable_notification=True)

        # --- PASSO 4: Decisão Estratégia ---
        # Passa apenas o sinal para a estratégia por enquanto
        logger.info(f"Executando estratégia para {symbol} com sinal '{trade_signal}'...")
        strategy_manager.decide_action(trade_signal)

    # --- Tratamento de Erro do Ciclo e Finalização ---
    except Exception as e:
        logger.critical("Erro CRÍTICO inesperado durante ciclo de trade.", exc_info=True)
        try: critical_message = f"ERRO CRITICO no Ciclo ({symbol}):\nVerifique {LOG_FILE}.\nErro: {str(e)}"; send_telegram_message(critical_message[:4000])
        except Exception as telegram_err: logger.error("Falha enviar notificação erro ciclo.", exc_info=True)
    finally:
        end_cycle_time = datetime.datetime.now(); cycle_duration = end_cycle_time - start_cycle_time
        logger.info(f"--- Ciclo concluído em {cycle_duration}. ({end_cycle_time.strftime('%Y-%m-%d %H:%M:%S')}) ---")

# --- Função Principal ---
def main():
    # ... (código main como antes, agendamento, loop while) ...
    setup_logging(level=logging.INFO); main_logger = logging.getLogger('main_runner')
    main_logger.info("--- Iniciando Quantis Crypto Trader - Gemini Version (MTA + Indicadores) ---"); send_telegram_message("Quantis Crypto Trader (MTA+Ind) iniciando...")
    if not initialize_services(): main_logger.critical("Falha inicialização. Encerrando."); return
    main_logger.info("Inicialização concluída. Configurando agendamento..."); main_cycle_interval_minutes = 15; schedule.every(main_cycle_interval_minutes).minutes.do(trade_cycle)
    try: job = schedule.get_jobs()[0]; main_logger.info(f"Ciclo agendado a cada {job.interval} {job.unit if hasattr(job, 'unit') else 'minutes'}.")
    except IndexError: main_logger.error("Nenhum job agendado!")
    main_logger.info("Executando primeiro ciclo imediatamente..."); trade_cycle()
    main_logger.info("Agendamento configurado. Entrando no loop principal..."); send_telegram_message("Robo MTA+Ind online e operando (modo simulado).")
    try: # Loop principal
        while True: schedule.run_pending(); time.sleep(1)
    except KeyboardInterrupt: main_logger.info("Interrupção manual. Encerrando..."); send_telegram_message("Quantis Crypto Trader (MTA+Ind) encerrado manualmente.")
    except Exception as e: main_logger.critical("Erro CRÍTICO loop principal.", exc_info=True); send_telegram_message(f"ERRO CRITICO LOOP PRINCIPAL (MTA+Ind)! Encerrando.\nErro: {str(e)[:500]}")
    finally: main_logger.info("--- Quantis Crypto Trader (MTA+Ind) Finalizado ---")

if __name__ == "__main__":
    # ... (verificação pandas_ta como antes) ...
    try: import pandas_ta as ta
    except ImportError: print("\n!!! ERRO FATAL: Biblioteca pandas-ta não encontrada. !!!\nInstale com: pip install pandas-ta"); sys.exit(1)
    main()