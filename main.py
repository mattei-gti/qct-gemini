# quantis_crypto_trader_gemini/main.py

# --- Imports Principais ---
import config
from database import init_db
from binance_client import BinanceHandler
from binance.client import Client
from redis_client import RedisHandler
from gemini_analyzer import GeminiAnalyzer
from telegram_interface import send_telegram_message
from strategy import StrategyManager
import pandas as pd
import pandas_ta as ta # Import aqui no topo
import datetime
import schedule
import time
import logging
import sys

# --- Configuração do Logging ---
LOG_FILE = "quantis_trader.log"
def setup_logging(level=logging.INFO):
    """Configura o logging para console e arquivo."""
    root_logger = logging.getLogger()
    # Limpa handlers existentes para evitar duplicação
    if root_logger.hasHandlers():
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()
    # Configura basicConfig
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger_cfg = logging.getLogger()
    logger_cfg.info("--- Logging reconfigurado ---")
    # Reduz verbosidade de bibliotecas externas
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('schedule').setLevel(logging.WARNING)

# --- Handlers Globais ---
binance_handler: BinanceHandler | None = None
redis_handler: RedisHandler | None = None
gemini_analyzer: GeminiAnalyzer | None = None
strategy_manager: StrategyManager | None = None
logger = logging.getLogger(__name__) # Logger específico para este módulo

# --- Função Auxiliar de Duração de Intervalo ---
def get_interval_ms(interval: str) -> int | None:
    """Retorna a duração aproximada do intervalo em milissegundos."""
    multipliers = {'m': 60, 'h': 3600, 'd': 86400, 'w': 604800, 'M': 2592000} # Segundos
    try:
        # Tenta extrair valor e unidade da string (ex: '15m', '1h', '1M')
        unit = interval[-1].lower()
        value = int(interval[:-1])
        if unit in multipliers:
            if unit == 'M': # Mês ~30d
                 return value * 30 * 24 * 60 * 60 * 1000
            elif unit == 'w': # Semana
                 return value * 7 * 24 * 60 * 60 * 1000
            else: # m, h, d
                 return value * multipliers[unit] * 1000
    except Exception:
        # Se falhar (ou se for uma constante Client), tenta o fallback
        if interval == Client.KLINE_INTERVAL_1MINUTE:
            return 60 * 1000
        elif interval == Client.KLINE_INTERVAL_15MINUTE:
            return 15 * 60 * 1000
        elif interval == Client.KLINE_INTERVAL_1HOUR:
            return 60 * 60 * 1000
        elif interval == Client.KLINE_INTERVAL_4HOUR:
             return 4 * 60 * 60 * 1000
        elif interval == Client.KLINE_INTERVAL_1DAY:
            return 24 * 60 * 60 * 1000
        elif interval == Client.KLINE_INTERVAL_1WEEK:
            return 7 * 24 * 60 * 60 * 1000
        elif interval == Client.KLINE_INTERVAL_1MONTH:
            return 30 * 24 * 60 * 60 * 1000 # Aproximação Mês

    # Se chegou aqui, não conseguiu determinar
    logger.warning(f"Duração em ms desconhecida para intervalo: {interval}")
    return None

# --- Funções de Inicialização e Ciclo de Trade ---
def initialize_services():
    """Inicializa todos os serviços necessários."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager
    logger.info("Inicializando serviços...")
    # Bloco try/except CORRIGIDO
    try:
        init_db()
        config.load_or_set_initial_db_settings() # Usa logger interno
        redis_handler = RedisHandler(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB) # Usa logger interno
        binance_handler = BinanceHandler(api_key=config.BINANCE_API_KEY, api_secret=config.BINANCE_SECRET_KEY) # Usa logger interno
        gemini_analyzer = GeminiAnalyzer(api_key=config.GEMINI_API_KEY) # Usa logger interno
        strategy_manager = StrategyManager(redis_handler=redis_handler, binance_handler=binance_handler) # Usa logger interno
        logger.info("Todos os serviços inicializados com sucesso.")
        return True
    except Exception as e:
        logger.critical("Erro CRÍTICO durante a inicialização dos serviços.", exc_info=True)
        # Bloco try/except aninhado CORRIGIDO
        try:
            critical_message = f"ERRO INICIALIZACAO:\n{str(e)}"
            send_telegram_message(critical_message[:4000]) # Envia erro truncado
        except Exception as telegram_err:
            logger.error("Falha ao enviar notificação de erro de inicialização.", exc_info=True)
        return False

def calculate_indicators(df: pd.DataFrame, sma_p: dict, ichi_p: dict) -> dict:
    """Calcula e retorna os últimos valores dos indicadores para um DataFrame."""
    indicators = {}
    if df is None or df.empty: return indicators
    required_len = max(sma_p['slow'], ichi_p['s'], 26, 20, 14) + 1 # Garante dados para todos TAs padrão
    if len(df) < required_len: logger.warning(f"Dados insuficientes ({len(df)}) para calcular inds (necessário ~{required_len})."); return {}

    logger.debug(f"Calculando indicadores para DF com {len(df)} linhas...")
    try:
        # Usa a instância 'ta' importada no topo do arquivo
        df.ta.sma(length=sma_p['fast'], append=True); df.ta.sma(length=sma_p['slow'], append=True)
        df.ta.rsi(append=True); df.ta.macd(append=True); df.ta.obv(append=True)
        df.ta.ichimoku(tenkan=ichi_p['t'], kijun=ichi_p['k'], senkou=ichi_p['s'], append=True)
        df.ta.bbands(append=True); df.ta.atr(append=True); df.ta.vwap(append=True)

        last = df.iloc[-1]
        def get_ind(row, key, decimals=2): return round(row[key], decimals) if pd.notna(row.get(key)) else None

        # SMAs
        indicators['sma_fast'] = get_ind(last, f"SMA_{sma_p['fast']}"); indicators['sma_slow'] = get_ind(last, f"SMA_{sma_p['slow']}")
        # RSI
        indicators['rsi'] = get_ind(last, "RSI_14")
        # MACD
        macd_fast, macd_slow, macd_sig = 12, 26, 9; indicators['macd_line'] = get_ind(last, f'MACD_{macd_fast}_{macd_slow}_{macd_sig}'); indicators['macd_hist'] = get_ind(last, f'MACDh_{macd_fast}_{macd_slow}_{macd_sig}'); indicators['macd_signal'] = get_ind(last, f'MACDs_{macd_fast}_{macd_slow}_{macd_sig}')
        # OBV
        indicators['obv'] = get_ind(last, "OBV", 0)
        # Ichimoku
        ichi_t, ichi_k, ichi_s = ichi_p['t'], ichi_p['k'], ichi_p['s']; indicators['ichi_tenkan'] = get_ind(last, f'ITS_{ichi_t}'); indicators['ichi_kijun'] = get_ind(last, f'IKS_{ichi_k}'); indicators['ichi_senkou_a'] = get_ind(last, f'ISA_{ichi_t}'); indicators['ichi_senkou_b'] = get_ind(last, f'ISB_{ichi_s}')
        # Bollinger Bands
        bb_len, bb_std = 20, 2.0; indicators['bb_lower'] = get_ind(last, f'BBL_{bb_len}_{bb_std}'); indicators['bb_middle'] = get_ind(last, f'BBM_{bb_len}_{bb_std}'); indicators['bb_upper'] = get_ind(last, f'BBU_{bb_len}_{bb_std}')
        # ATR
        atr_len = 14; indicators['atr'] = get_ind(last, f'ATR_{atr_len}', 4)
        # VWAP
        indicators['vwap'] = get_ind(last, 'VWAP_D') # Nome padrão da coluna VWAP do pandas-ta

    except Exception as e: logger.error("Erro calcular indicadores TA.", exc_info=True); return {}
    final_indicators = {k: v for k, v in indicators.items() if v is not None} # Remove None
    logger.debug(f"Indicadores calculados (não nulos): {list(final_indicators.keys())}"); return final_indicators


def trade_cycle():
    """Executa um ciclo completo: Atualiza Histórico -> Busca Recente -> Calcula TAs -> Analisa -> Decide."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager
    if not all([binance_handler, redis_handler, gemini_analyzer, strategy_manager]): logger.error("Serviços não inicializados. Abortando ciclo."); return

    start_cycle_time = datetime.datetime.now()
    logger.info(f"--- Iniciando Ciclo de Trade (Intraday + Indicadores) em {start_cycle_time.strftime('%Y-%m-%d %H:%M:%S')} ---")

    symbol = strategy_manager.symbol; analysis_interval = Client.KLINE_INTERVAL_1HOUR

    # Todos os timeframes a serem MANTIDOS ATUALIZADOS no Redis
    mta_intervals = {"1M": Client.KLINE_INTERVAL_1MONTH, "1d": Client.KLINE_INTERVAL_1DAY, "1h": Client.KLINE_INTERVAL_1HOUR, "15m": Client.KLINE_INTERVAL_15MINUTE, "1m": Client.KLINE_INTERVAL_1MINUTE}
    # Timeframes a serem USADOS NA ANÁLISE GEMINI (Intraday)
    tfs_for_gemini_analysis = {"1h": Client.KLINE_INTERVAL_1HOUR, "15m": Client.KLINE_INTERVAL_15MINUTE, "1m": Client.KLINE_INTERVAL_1MINUTE}

    sma_params = {'fast': 30, 'slow': 60}; ichi_params = {'t': 21, 'k': 34, 's': 52}
    min_klines_needed = max(sma_params['slow'], ichi_params['s'], 26, 20, 14) + 50

    mta_data_for_gemini = {} # Guarda indicadores SÓ dos TFs de análise
    all_data_available = True # Flag para análise Gemini

    try:
        # --- PASSO 1: Atualização Incremental (PARA TODOS os TFs) ---
        logger.info("--- Iniciando Atualização Histórico Redis (Todos TFs) ---")
        # Itera sobre todos os TFs que queremos manter atualizados
        for tf_label, tf_interval in mta_intervals.items():
             logger.debug(f"Atualizando {symbol}/{tf_label}...")
             last_ts_ms = redis_handler.get_last_hist_timestamp(symbol, tf_interval)
             start_fetch_str = None
             if last_ts_ms:
                 interval_ms = get_interval_ms(tf_interval)
                 if interval_ms:
                     start_fetch_ts_ms = last_ts_ms + interval_ms
                     now_ms = int(time.time() * 1000)
                     if start_fetch_ts_ms < now_ms - 10000: # Buffer de 10s
                         start_fetch_str = str(start_fetch_ts_ms)
                         logger.info(f"Verificando velas {tf_label} desde {pd.to_datetime(start_fetch_ts_ms, unit='ms')}...")
                     else: logger.info(f"Histórico {tf_label} atualizado."); continue
                 else: logger.warning(f"Duração {tf_label} desconhecida."); continue
             else: logger.error(f"HISTÓRICO BASE {tf_label} NÃO ENCONTRADO!"); all_data_available = False; continue

             if start_fetch_str:
                 try: # Bloco try/except para busca E adição
                     new_klines_df = binance_handler.get_klines(symbol=symbol, interval=tf_interval, start_str=start_fetch_str, limit=1000)
                     if new_klines_df is not None and not new_klines_df.empty:
                         logger.info(f"{len(new_klines_df)} novas velas {tf_label} encontradas.")
                         try: # Garante índice correto
                             if not isinstance(new_klines_df.index, pd.DatetimeIndex):
                                 if 'Open time' in new_klines_df.columns:
                                     new_klines_df['Open time'] = pd.to_datetime(new_klines_df['Open time'], unit='ms')
                                     new_klines_df.set_index('Open time', inplace=True)
                                 else: logger.error(f"Coluna 'Open time' nao encontrada {tf_label}."); continue
                             redis_handler.add_klines_to_hist(symbol, tf_interval, new_klines_df) # Usa logger interno
                         except Exception as idx_err: logger.error(f"Erro processar/add klines {tf_label}.", exc_info=True)
                     elif new_klines_df is not None: logger.info(f"Nenhuma vela nova para {tf_label}.")
                     # else: get_klines já loga erro se retornar None
                 except Exception as fetch_err: logger.error(f"Erro busca/add incremental {tf_label}.", exc_info=True)
             time.sleep(0.2) # Pausa menor entre TFs
        logger.info("--- Concluída Atualização Histórico Redis ---")


        # --- PASSO 2: Busca Dados Recentes e Calcula Indicadores (SÓ para TFs de análise) ---
        logger.info(f"--- Buscando Dados Recentes e Calculando Indicadores ({min_klines_needed} velas) para TFs {list(tfs_for_gemini_analysis.keys())} ---")
        # Itera APENAS nos timeframes definidos para análise Gemini
        for tf_label, tf_interval in tfs_for_gemini_analysis.items():
            indicators = {}; df = None # Reinicia
            try:
                df = redis_handler.get_last_n_hist_klines(symbol, tf_interval, min_klines_needed) # Usa logger interno
                if df is not None and not df.empty:
                    indicators = calculate_indicators(df.copy(), sma_params, ichi_params) # Usa logger interno
                    if not indicators and len(df) >= min_klines_needed:
                         logger.warning(f"Falha calc inds {tf_label} c/ {len(df)} velas.")
                         # Marcamos que faltou dado se o cálculo falhou mesmo tendo velas suficientes
                         all_data_available = False
                else:
                    logger.warning(f"Falha carregar dados {tf_label} Redis.")
                    all_data_available = False # Dado ausente
            except Exception as e:
                logger.error(f"Erro buscar/calc inds {tf_label}.", exc_info=True)
                all_data_available = False
            mta_data_for_gemini[tf_label] = indicators # Guarda o dict (pode ser vazio)
            if not indicators: logger.warning(f"Dict inds vazio para {tf_label} (será enviado assim para Gemini).")

        # --- PASSO 3: Análise Gemini ---
        trade_signal: str | None = None; justification: str | None = None
        # Só executa se conseguiu dados E indicadores para TODOS os TFs de análise
        if all_data_available and all(mta_data_for_gemini.get(tf) for tf in tfs_for_gemini_analysis):
            logger.info(f"Enviando dados Intraday+Indicadores para análise Gemini ({gemini_analyzer.model_name})...")
            signal_tuple = gemini_analyzer.get_trade_signal_mta_indicators(mta_data_for_gemini, symbol) # Usa logger interno
            if signal_tuple: trade_signal, justification = signal_tuple
            logger.info(f"Sinal Obtido Gemini (Intraday+Ind): {trade_signal if trade_signal else 'Nenhum/Erro'}")
            if justification: logger.info(f"Justificativa Gemini: {justification}")
            if trade_signal: message = f"Sinal Intraday+Ind ({symbol} - {analysis_interval}): {trade_signal}"; # Usa analysis_interval ref
            if justification: message += f"\nJustif: {justification}"; send_telegram_message(message) # Usa logger interno
        else:
            logger.warning("Análise Gemini ignorada: faltaram dados ou indicadores de TFs Intraday.")
            send_telegram_message(f"Alerta ({symbol}): Falha carregar/calcular TFs Intraday p/ analise.", disable_notification=True) # Usa logger interno

        # --- PASSO 4: Decisão Estratégia ---
        logger.info(f"Executando estratégia para {symbol} com sinal '{trade_signal}'...")
        strategy_manager.decide_action(trade_signal) # Usa logger interno

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
    setup_logging(level=logging.INFO)
    main_logger = logging.getLogger('main_runner')
    main_logger.info("--- Iniciando Quantis Crypto Trader - Gemini Version (Intraday + Indicadores) ---")
    send_telegram_message("Quantis Crypto Trader (Intraday+Ind) iniciando...")
    if not initialize_services(): main_logger.critical("Falha inicialização. Encerrando."); return
    main_logger.info("Inicialização concluída. Configurando agendamento...")
    main_cycle_interval_minutes = 15; schedule.every(main_cycle_interval_minutes).minutes.do(trade_cycle)
    try: job = schedule.get_jobs()[0]; main_logger.info(f"Ciclo agendado a cada {job.interval} {job.unit if hasattr(job, 'unit') else 'minutes'}.")
    except IndexError: main_logger.error("Nenhum job agendado!")
    main_logger.info("Executando primeiro ciclo imediatamente..."); trade_cycle()
    main_logger.info("Agendamento configurado. Entrando no loop principal..."); send_telegram_message("Robo Intraday+Ind online e operando (modo simulado).")
    try: # Loop principal
        while True: schedule.run_pending(); time.sleep(1)
    except KeyboardInterrupt: main_logger.info("Interrupção manual. Encerrando..."); send_telegram_message("Quantis Crypto Trader (Intraday+Ind) encerrado.")
    except Exception as e: main_logger.critical("Erro CRÍTICO loop principal.", exc_info=True); send_telegram_message(f"ERRO CRITICO LOOP PRINCIPAL (Intraday+Ind)! Encerrando.\nErro: {str(e)[:500]}")
    finally: main_logger.info("--- Quantis Crypto Trader (MTA+Ind) Finalizado ---")

if __name__ == "__main__":
    # Garante que pandas_ta está disponível
    try: import pandas_ta as ta
    except ImportError: print("\n!!! ERRO FATAL: pandas-ta nao encontrado. Instale: pip install pandas-ta"); sys.exit(1)
    main()