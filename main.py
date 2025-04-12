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
            try:
                handler.close()
                root_logger.removeHandler(handler)
            except Exception as e:
                 print(f"Erro ao remover handler de log existente: {e}")
    # Configura basicConfig
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True # Garante que a reconfiguração funcione
    )
    logger_cfg = logging.getLogger()
    logger_cfg.info("--- Logging configurado/reconfigurado ---")
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
        else: # Unidade desconhecida na string
             logger.warning(f"Unidade de intervalo desconhecida em '{interval}'")
             return None
    except Exception:
        # Se falhar (ou se for uma constante Client), tenta o fallback
        # Estrutura if/elif CORRIGIDA e indentada
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
        else:
            # Se não corresponder a nenhuma constante conhecida
            logger.warning(f"Duração em ms desconhecida para intervalo constante: {interval}")
            return None

# --- Funções de Inicialização e Ciclo de Trade ---
def initialize_services():
    """Inicializa todos os serviços necessários."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager
    logger.info("Inicializando serviços...")
    # Bloco try/except CORRIGIDO
    try:
        init_db()
        config.load_or_set_initial_db_settings()
        redis_handler = RedisHandler(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
        binance_handler = BinanceHandler(api_key=config.BINANCE_API_KEY, api_secret=config.BINANCE_SECRET_KEY)
        gemini_analyzer = GeminiAnalyzer(api_key=config.GEMINI_API_KEY)
        strategy_manager = StrategyManager(redis_handler=redis_handler, binance_handler=binance_handler)
        logger.info("Todos os serviços inicializados com sucesso.")
        return True
    except Exception as e:
        logger.critical("Erro CRÍTICO durante a inicialização dos serviços.", exc_info=True)
        # Bloco try/except aninhado CORRIGIDO
        try:
            critical_message = f"ERRO INICIALIZACAO:\n{str(e)}"
            send_telegram_message(critical_message[:4000])
        except Exception as telegram_err:
            logger.error("Falha ao enviar notificação de erro de inicialização.", exc_info=True)
        return False

def calculate_indicators(df: pd.DataFrame, sma_p: dict, ichi_p: dict) -> dict:
    """Calcula e retorna os últimos valores dos indicadores para um DataFrame."""
    indicators = {}
    if df is None or df.empty:
        return indicators
    required_len = max(sma_p['slow'], ichi_p['s'], 26, 20, 14) + 1
    if len(df) < required_len:
        logger.warning(f"Dados insuficientes ({len(df)}) para calcular inds (necessário ~{required_len}).")
        return {}

    logger.debug(f"Calculando indicadores para DF com {len(df)} linhas...")
    try:
        # Usa a instância 'ta' importada no topo
        df.ta.sma(length=sma_p['fast'], append=True)
        df.ta.sma(length=sma_p['slow'], append=True)
        df.ta.rsi(append=True)
        df.ta.macd(append=True)
        df.ta.obv(append=True)
        df.ta.ichimoku(tenkan=ichi_p['t'], kijun=ichi_p['k'], senkou=ichi_p['s'], append=True)
        df.ta.bbands(append=True)
        df.ta.atr(append=True)
        df.ta.vwap(append=True)

        last = df.iloc[-1] # Pega a última linha completa
        # Helper para extrair e arredondar com segurança
        def get_ind(row, key, decimals=2):
            return round(row[key], decimals) if pd.notna(row.get(key)) else None

        # Extrai indicadores
        indicators['sma_fast'] = get_ind(last, f"SMA_{sma_p['fast']}")
        indicators['sma_slow'] = get_ind(last, f"SMA_{sma_p['slow']}")
        indicators['rsi'] = get_ind(last, "RSI_14")
        macd_fast, macd_slow, macd_sig = 12, 26, 9 # Padrões
        indicators['macd_line'] = get_ind(last, f'MACD_{macd_fast}_{macd_slow}_{macd_sig}')
        indicators['macd_hist'] = get_ind(last, f'MACDh_{macd_fast}_{macd_slow}_{macd_sig}')
        indicators['macd_signal'] = get_ind(last, f'MACDs_{macd_fast}_{macd_slow}_{macd_sig}')
        indicators['obv'] = get_ind(last, "OBV", 0)
        ichi_t, ichi_k, ichi_s = ichi_p['t'], ichi_p['k'], ichi_p['s']
        indicators['ichi_tenkan'] = get_ind(last, f'ITS_{ichi_t}')
        indicators['ichi_kijun'] = get_ind(last, f'IKS_{ichi_k}')
        indicators['ichi_senkou_a'] = get_ind(last, f'ISA_{ichi_t}')
        indicators['ichi_senkou_b'] = get_ind(last, f'ISB_{ichi_s}')
        bb_len, bb_std = 20, 2.0 # Padrões
        indicators['bb_lower'] = get_ind(last, f'BBL_{bb_len}_{bb_std}')
        indicators['bb_middle'] = get_ind(last, f'BBM_{bb_len}_{bb_std}')
        indicators['bb_upper'] = get_ind(last, f'BBU_{bb_len}_{bb_std}')
        atr_len = 14 # Padrão
        indicators['atr'] = get_ind(last, f'ATR_{atr_len}', 4)
        indicators['vwap'] = get_ind(last, 'VWAP_D') # Confere se nome da coluna está correto

    except Exception as e:
        logger.error("Erro calcular indicadores TA.", exc_info=True)
        return {} # Retorna vazio em caso de erro

    final_indicators = {k: v for k, v in indicators.items() if v is not None} # Remove None
    logger.debug(f"Inds calculados (nao nulos): {list(final_indicators.keys())}")
    return final_indicators


def trade_cycle():
    """Executa um ciclo completo: Atualiza Histórico -> Busca Recente -> Calcula TAs -> Analisa -> Decide."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager
    # Verificação inicial CORRIGIDA
    if not all([binance_handler, redis_handler, gemini_analyzer, strategy_manager]):
        logger.error("Serviços não inicializados corretamente. Abortando ciclo.")
        return

    start_cycle_time = datetime.datetime.now()
    logger.info(f"--- Iniciando Ciclo de Trade (Híbrido AI+TA) em {start_cycle_time.strftime('%Y-%m-%d %H:%M:%S')} ---")

    symbol = strategy_manager.symbol
    analysis_interval = Client.KLINE_INTERVAL_1HOUR # Intervalo de referência

    mta_intervals_to_update = {"1M": Client.KLINE_INTERVAL_1MONTH, "1d": Client.KLINE_INTERVAL_1DAY, "1h": Client.KLINE_INTERVAL_1HOUR, "15m": Client.KLINE_INTERVAL_15MINUTE, "1m": Client.KLINE_INTERVAL_1MINUTE}
    tfs_for_gemini_analysis = {"1h": Client.KLINE_INTERVAL_1HOUR, "15m": Client.KLINE_INTERVAL_15MINUTE, "1m": Client.KLINE_INTERVAL_1MINUTE}
    sma_params = {'fast': 30, 'slow': 60}
    ichi_params = {'t': 21, 'k': 34, 's': 52}
    min_klines_needed = max(sma_params['slow'], ichi_params['s'], 26, 20, 14) + 50
    mta_data_for_gemini = {}
    all_data_available = True
    latest_price = None # Inicializa latest_price

    # Bloco try principal CORRIGIDO
    try:
        # --- PASSO 1: Atualização Incremental (PARA TODOS os TFs) ---
        logger.info("--- Iniciando Fase de Atualização do Histórico Redis (Todos TFs) ---")
        # Itera sobre todos os TFs
        for tf_label, tf_interval in mta_intervals_to_update.items():
             logger.debug(f"Atualizando {symbol}/{tf_label}...")
             last_ts_ms = redis_handler.get_last_hist_timestamp(symbol, tf_interval)
             start_fetch_str = None
             if last_ts_ms:
                 interval_ms = get_interval_ms(tf_interval)
                 if interval_ms:
                     start_fetch_ts_ms = last_ts_ms + interval_ms
                     now_ms = int(time.time() * 1000)
                     if start_fetch_ts_ms < now_ms - 10000: # Buffer
                         start_fetch_str = str(start_fetch_ts_ms)
                         logger.info(f"Verificando velas {tf_label} desde {pd.to_datetime(start_fetch_ts_ms, unit='ms')}...")
                     else:
                         logger.info(f"Histórico {tf_label} já está atualizado.")
                         continue
                 else:
                     logger.warning(f"Duração {tf_label} desconhecida.")
                     continue
             else:
                 logger.error(f"HISTÓRICO BASE {tf_label} NÃO ENCONTRADO!")
                 all_data_available = False
                 continue

             if start_fetch_str:
                 # Bloco try/except para busca e adição CORRIGIDO
                 try:
                     new_klines_df = binance_handler.get_klines(symbol=symbol, interval=tf_interval, start_str=start_fetch_str, limit=1000)
                     if new_klines_df is not None and not new_klines_df.empty:
                         logger.info(f"{len(new_klines_df)} novas velas {tf_label} encontradas.")
                         # Bloco try/except para processar e adicionar CORRIGIDO
                         try:
                             if not isinstance(new_klines_df.index, pd.DatetimeIndex):
                                 if 'Open time' in new_klines_df.columns:
                                     new_klines_df['Open time'] = pd.to_datetime(new_klines_df['Open time'], unit='ms')
                                     new_klines_df.set_index('Open time', inplace=True)
                                 else:
                                     logger.error(f"Coluna 'Open time' nao encontrada {tf_label}.")
                                     continue # Pula
                             redis_handler.add_klines_to_hist(symbol, tf_interval, new_klines_df)
                         except Exception as idx_err:
                             logger.error(f"Erro ao processar/adicionar novas klines para {tf_label}.", exc_info=True)
                     elif new_klines_df is not None:
                         logger.info(f"Nenhuma vela nova para {tf_label}.")
                     # else: erro já logado em get_klines
                 except Exception as fetch_err:
                     logger.error(f"Erro na busca/adição incremental para {tf_label}.", exc_info=True)

             # Pausa curta (indentada corretamente)
             time.sleep(0.2)
        logger.info("--- Concluída Fase de Atualização do Histórico Redis ---")


        # --- PASSO 2: Busca Dados Recentes e Calcula Indicadores (SÓ para TFs de análise) ---
        logger.info(f"--- Buscando Dados Recentes e Calculando Indicadores ({min_klines_needed} velas) para TFs {list(tfs_for_gemini_analysis.keys())} ---")
        # Itera APENAS nos TFs de análise
        for tf_label, tf_interval in tfs_for_gemini_analysis.items():
            indicators = {}
            df = None # Reinicia
            # Bloco try/except CORRIGIDO
            try:
                df = redis_handler.get_last_n_hist_klines(symbol, tf_interval, min_klines_needed)
                if df is not None and not df.empty:
                    indicators = calculate_indicators(df.copy(), sma_params, ichi_params) # Passa cópia
                    if not indicators and len(df) >= min_klines_needed:
                         logger.warning(f"Falha calc inds {tf_label} c/ {len(df)} velas.")
                         all_data_available = False # Se falhou em calcular mesmo com dados
                else:
                    logger.warning(f"Falha carregar dados {tf_label} Redis.")
                    all_data_available = False # Dado ausente
            except Exception as e:
                 logger.error(f"Erro buscar/calc inds {tf_label}.", exc_info=True)
                 all_data_available = False
            # Guarda dict de indicadores (pode ser vazio)
            mta_data_for_gemini[tf_label] = indicators
            if not indicators:
                logger.warning(f"Dict inds vazio para {tf_label}.")


        # --- PASSO 2.5: Busca Preço Atual do Ticker ---
        if binance_handler:
            logger.debug(f"Buscando preço atual ticker para {symbol}...")
            latest_price = binance_handler.get_ticker_price(symbol)
            if latest_price is None:
                 logger.warning(f"Não foi possível obter o preço atual do ticker para {symbol}.")
        else:
             logger.error("Binance handler não disponível para buscar ticker price.")


        # --- PASSO 3: Análise Gemini com Indicadores MTA Intraday ---
        trade_signal: str | None = None
        justification: str | None = None

        # Só executa se conseguiu dados E indicadores para TODOS os TFs de análise
        if all_data_available and all(mta_data_for_gemini.get(tf) for tf in tfs_for_gemini_analysis):
            logger.info(f"Enviando dados Intraday+Indicadores e Ticker={latest_price} para análise Gemini...")
            signal_tuple = gemini_analyzer.get_trade_signal_mta_indicators(
                mta_indicators_data=mta_data_for_gemini,
                symbol=symbol,
                current_ticker_price=latest_price # Passa o preço do ticker
            )
            if signal_tuple:
                trade_signal, justification = signal_tuple # Desempacota
            logger.info(f"Sinal Obtido Gemini (Intraday+Ind): {trade_signal if trade_signal else 'Nenhum/Erro'}")
            if justification:
                 logger.info(f"Justificativa Gemini: {justification}")

            # Envia sinal e justificativa para Telegram se obtidos
            if trade_signal:
                price_str = f"{latest_price:.2f}" if latest_price is not None else "N/A"
                message = f"Sinal Intraday+Ind ({symbol} - {analysis_interval}): {trade_signal}\n"
                message += f"Preço Atual: {price_str}\n"
                if justification:
                    message += f"Justif: {justification}"
                send_telegram_message(message)
        else:
            logger.warning("Análise Gemini ignorada: faltaram dados ou indicadores dos TFs Intraday.")
            send_telegram_message(f"Alerta ({symbol}): Falha carregar/calcular TFs Intraday p/ analise.", disable_notification=True)


        # --- PASSO 4: Decisão e Ação (Simulada) da Estratégia ---
        logger.info(f"Executando estratégia HÍBRIDA para {symbol} com sinal '{trade_signal}'...")
        indicators_1h = mta_data_for_gemini.get('1h', {})
        sma_fast_1h = indicators_1h.get('sma_fast')
        sma_slow_1h = indicators_1h.get('sma_slow')
        strategy_manager.decide_action(
            signal=trade_signal,
            sma_fast_1h=sma_fast_1h,
            sma_slow_1h=sma_slow_1h
        )

    # --- Tratamento de Erro do Ciclo e Finalização ---
    # Bloco try/except/finally CORRIGIDO
    except Exception as e:
        logger.critical("Erro CRÍTICO inesperado durante ciclo de trade.", exc_info=True)
        try:
            critical_message = f"ERRO CRITICO no Ciclo ({symbol}):\nVerifique {LOG_FILE}.\nErro: {str(e)}"
            send_telegram_message(critical_message[:4000])
        except Exception as telegram_err:
            logger.error("Falha enviar notificação erro ciclo.", exc_info=True)
    finally:
        end_cycle_time = datetime.datetime.now()
        cycle_duration = end_cycle_time - start_cycle_time
        logger.info(f"--- Ciclo concluído em {cycle_duration}. ({end_cycle_time.strftime('%Y-%m-%d %H:%M:%S')}) ---")


# --- Função Principal ---
def main():
    setup_logging(level=logging.INFO)
    main_logger = logging.getLogger('main_runner')
    main_logger.info("--- Iniciando Quantis Crypto Trader - Gemini Version (Híbrido AI+TA) ---")
    send_telegram_message("Quantis Crypto Trader (Híbrido AI+TA) iniciando...")

    if not initialize_services():
        main_logger.critical("Falha inicialização. Encerrando.")
        return

    main_logger.info("Inicialização concluída.")
    # Bloco try/except CORRIGIDO
    try:
        main_logger.info("Verificando saldos iniciais...")
        base_asset = strategy_manager.base_asset
        quote_asset = strategy_manager.quote_asset
        base_balance = binance_handler.get_asset_balance(base_asset)
        quote_balance = binance_handler.get_asset_balance(quote_asset)
        balance_message = (f"--- Saldo Inicial Binance ---\n"
                           f"{base_asset}: {base_balance:.8f}\n"
                           f"{quote_asset}: {quote_balance:.2f}")
        logger.info(balance_message)
        send_telegram_message(balance_message)
    except Exception as e:
        logger.error("Erro ao verificar ou enviar saldo inicial.", exc_info=True)
        send_telegram_message("Erro ao verificar saldo inicial.")

    main_logger.info("Configurando agendamento...")
    # *** Schedule de 5 minutos ***
    main_cycle_interval_minutes = 5
    schedule.every(main_cycle_interval_minutes).minutes.do(trade_cycle)
    # Bloco try/except CORRIGIDO
    try:
        job = schedule.get_jobs()[0]
        main_logger.info(f"Ciclo agendado a cada {job.interval} {job.unit if hasattr(job, 'unit') else 'minutes'}.")
    except IndexError:
        main_logger.error("Nenhum job agendado!")

    main_logger.info("Executando primeiro ciclo imediatamente...")
    trade_cycle()
    main_logger.info("Agendamento configurado. Entrando no loop principal...")
    send_telegram_message("Robo Híbrido AI+TA online e operando (modo simulado).")

    # Loop principal try/except/finally CORRIGIDO
    try:
        while True:
            schedule.run_pending()
            time.sleep(1) # Pausa pequena
    except KeyboardInterrupt:
        main_logger.info("Interrupção manual. Encerrando...")
        send_telegram_message("Quantis Crypto Trader (Híbrido AI+TA) encerrado.")
    except Exception as e:
        main_logger.critical("Erro CRÍTICO loop principal.", exc_info=True)
        send_telegram_message(f"ERRO CRITICO LOOP PRINCIPAL (Híbrido AI+TA)! Encerrando.\nErro: {str(e)[:500]}")
    finally:
         main_logger.info("--- Quantis Crypto Trader (Híbrido AI+TA) Finalizado ---")


if __name__ == "__main__":
    # Bloco try/except CORRIGIDO
    try:
        # Garante que pandas_ta está disponível e importável
        import pandas_ta as ta
        logger.debug("Biblioteca pandas-ta importada com sucesso.")
    except ImportError:
        print("\n!!! ERRO FATAL: Biblioteca pandas-ta não encontrada. !!!")
        print("Por favor, instale usando o comando abaixo no terminal com seu ambiente virtual ativo:")
        print("pip install pandas-ta")
        sys.exit(1) # Impede a execução sem a biblioteca
    # Chamada main fora do try/except
    main()