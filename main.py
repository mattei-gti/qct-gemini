# quantis_crypto_trader_gemini/main.py

import config
from database import init_db
from binance_client import BinanceHandler
from binance.client import Client # Importa Client para constantes de intervalo
from redis_client import RedisHandler
from gemini_analyzer import GeminiAnalyzer
from telegram_interface import send_telegram_message # Importa função de envio (texto simples)
from strategy import StrategyManager
import pandas as pd
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
    # Limpa handlers existentes para evitar duplicação se função for chamada de novo
    if root_logger.hasHandlers():
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close() # Fecha handlers antigos
    # Configura basicConfig
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'), # Salva em arquivo
            logging.StreamHandler(sys.stdout) # Mostra no console
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
        unit = interval[-1].lower()
        value = int(interval[:-1])
        if unit in multipliers:
            if unit == 'M': return value * 30 * 24 * 60 * 60 * 1000 # Mês ~30d
            elif unit == 'w': return value * 7 * 24 * 60 * 60 * 1000 # Semana
            else: return value * multipliers[unit] * 1000 # m, h, d
    except Exception: # Fallback para constantes Client
        if interval == Client.KLINE_INTERVAL_1MINUTE: return 60 * 1000
        if interval == Client.KLINE_INTERVAL_15MINUTE: return 15 * 60 * 1000
        if interval == Client.KLINE_INTERVAL_1HOUR: return 60 * 60 * 1000
        if interval == Client.KLINE_INTERVAL_4HOUR: return 4 * 60 * 60 * 1000
        if interval == Client.KLINE_INTERVAL_1DAY: return 24 * 60 * 60 * 1000
        if interval == Client.KLINE_INTERVAL_1WEEK: return 7 * 24 * 60 * 60 * 1000
        if interval == Client.KLINE_INTERVAL_1MONTH: return 30 * 24 * 60 * 60 * 1000
    logger.warning(f"Duração em ms desconhecida para intervalo: {interval}")
    return None

# --- Funções de Inicialização e Ciclo de Trade ---

def initialize_services():
    """Inicializa todos os serviços necessários."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager
    logger.info("Inicializando serviços...")
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
        try:
            critical_message = f"ERRO CRITICO NA INICIALIZACAO:\nNao foi possivel iniciar os servicos.\nErro: {str(e)}"
            send_telegram_message(critical_message[:4000])
        except Exception as telegram_err:
            logger.error("Falha ao enviar notificação de erro de inicialização.", exc_info=True)
        return False

def trade_cycle():
    """Executa um ciclo completo: Atualiza Histórico -> Busca Recente -> Analisa -> Decide."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager
    if not all([binance_handler, redis_handler, gemini_analyzer, strategy_manager]):
        logger.error("Serviços não inicializados corretamente. Abortando ciclo.")
        return

    start_cycle_time = datetime.datetime.now()
    logger.info(f"--- Iniciando Ciclo de Trade (Histórico Redis) em {start_cycle_time.strftime('%Y-%m-%d %H:%M:%S')} ---")

    symbol = strategy_manager.symbol
    analysis_interval = Client.KLINE_INTERVAL_1HOUR # Intervalo de referência para mensagens

    # Timeframes a atualizar e usar na análise MTA
    mta_intervals = {
        "1M": Client.KLINE_INTERVAL_1MONTH, "1d": Client.KLINE_INTERVAL_1DAY,
        "1h": Client.KLINE_INTERVAL_1HOUR, "15m": Client.KLINE_INTERVAL_15MINUTE,
        "1m": Client.KLINE_INTERVAL_1MINUTE,
    }
    sma_periods = {'fast': 30, 'slow': 60} # Períodos das SMAs
    # Quantidade de velas recentes a buscar do histórico para análise/cálculo de SMA
    klines_needed_for_analysis = max(sma_periods.values()) + 50

    mta_data = {} # Dicionário para guardar dados recentes de cada TF
    all_data_available_for_analysis = True # Flag de controle

    try:
        # --- PASSO 1: Atualização Incremental do Histórico Redis ---
        logger.info("--- Iniciando Fase de Atualização do Histórico Redis ---")
        for tf_label, tf_interval in mta_intervals.items():
            logger.debug(f"Atualizando histórico para {symbol}/{tf_label}...")
            last_ts_ms = redis_handler.get_last_hist_timestamp(symbol, tf_interval)
            start_fetch_str = None

            if last_ts_ms:
                interval_ms = get_interval_ms(tf_interval)
                if interval_ms:
                    start_fetch_ts_ms = last_ts_ms + interval_ms
                    now_ms = int(time.time() * 1000)
                    # Define um pequeno buffer (ex: 10 segundos) para garantir que a vela fechou
                    buffer_ms = 10000
                    if start_fetch_ts_ms < now_ms - buffer_ms:
                         start_fetch_str = str(start_fetch_ts_ms)
                         logger.info(f"Verificando novas velas para {tf_label} desde {pd.to_datetime(start_fetch_ts_ms, unit='ms')}...")
                    else:
                        logger.info(f"Histórico {tf_label} já está atualizado (próxima vela começa em {pd.to_datetime(start_fetch_ts_ms, unit='ms')}).")
                        continue
                else:
                    logger.warning(f"Duração {tf_label} desconhecida, atualização incremental pulada.")
                    continue
            else:
                # Se não há histórico, populate_history deveria ter rodado. Não busca aqui.
                logger.error(f"HISTÓRICO BASE NÃO ENCONTRADO para {symbol}/{tf_label}! Execute populate_history.py.")
                all_data_available_for_analysis = False # Impede análise se falta base
                continue # Pula para próximo timeframe

            # Se start_fetch_str foi definido, busca velas novas
            if start_fetch_str:
                try:
                    # Usa get_klines para buscar atualizações pequenas
                    new_klines_df = binance_handler.get_klines(
                        symbol=symbol,
                        interval=tf_interval,
                        start_str=start_fetch_str, # Passa como string (timestamp ms ou formato de data)
                        limit=1000 # Limite da API para get_klines
                    )

                    if new_klines_df is not None and not new_klines_df.empty:
                        logger.info(f"{len(new_klines_df)} novas velas encontradas para {tf_label}.")
                        try:
                            # *** GARANTIA DE ÍNDICE DATETIME ANTES DE ADICIONAR ***
                            # get_klines retorna DF com índice numérico, precisa ajustar
                            if not isinstance(new_klines_df.index, pd.DatetimeIndex):
                                if 'Open time' in new_klines_df.columns:
                                    logger.debug(f"Convertendo e definindo índice 'Open time' para novas velas {tf_label}...")
                                    new_klines_df['Open time'] = pd.to_datetime(new_klines_df['Open time'], unit='ms')
                                    new_klines_df.set_index('Open time', inplace=True)
                                else:
                                     logger.error(f"Coluna 'Open time' não encontrada nas novas klines de {tf_label}. Impossível adicionar ao histórico.")
                                     continue # Pula adição se não conseguir indexar
                            # Adiciona ao histórico Redis (Sorted Set)
                            redis_handler.add_klines_to_hist(symbol, tf_interval, new_klines_df)
                        except Exception as process_err:
                             logger.error(f"Erro ao processar/indexar/adicionar novas klines para {tf_label}.", exc_info=True)

                    elif new_klines_df is not None: # DataFrame vazio retornado = sem novas velas
                        logger.info(f"Nenhuma vela nova para {tf_label} desde {pd.to_datetime(int(start_fetch_str), unit='ms')}.")
                    # else: get_klines retornou None, erro já logado internamente

                except Exception as fetch_err:
                    logger.error(f"Erro na busca/adição incremental para {tf_label}.", exc_info=True)

            # Pausa curta para não sobrecarregar a API da Binance
            time.sleep(0.3)
        logger.info("--- Concluída Fase de Atualização do Histórico Redis ---")


        # --- PASSO 2: Busca de Dados Recentes do Histórico Redis para Análise ---
        logger.info(f"--- Buscando Dados Recentes do Histórico Redis para Análise ({klines_needed_for_analysis} velas) ---")
        for tf_label, tf_interval in mta_intervals.items():
            df = None; sma_f = None; sma_s = None
            try:
                # Busca as N últimas velas do histórico ZSET
                df = redis_handler.get_last_n_hist_klines(symbol, tf_interval, klines_needed_for_analysis)

                # Calcula SMAs se tiver dados suficientes
                if df is not None and len(df) >= sma_periods['slow']:
                     sma_f_col = f"SMA_{sma_periods['fast']}"; sma_s_col = f"SMA_{sma_periods['slow']}"
                     # Calcula SMAs (pode ser feito no DF retornado)
                     df[sma_f_col] = df['Close'].rolling(window=sma_periods['fast']).mean()
                     df[sma_s_col] = df['Close'].rolling(window=sma_periods['slow']).mean()
                     # Pega os últimos valores não nulos das SMAs
                     sma_f_val = df[sma_f_col].dropna().iloc[-1] if not df[sma_f_col].dropna().empty else None
                     sma_s_val = df[sma_s_col].dropna().iloc[-1] if not df[sma_s_col].dropna().empty else None
                     sma_f = round(sma_f_val, 2) if pd.notna(sma_f_val) else None
                     sma_s = round(sma_s_val, 2) if pd.notna(sma_s_val) else None
                     logger.debug(f"Dados/SMAs {tf_label} carregados. Últimas SMAs: F={sma_f}, S={sma_s}")
                elif df is not None:
                     logger.warning(f"Dados insuficientes ({len(df)} velas) para calcular SMAs {sma_periods['slow']} em {tf_label}.")
                else:
                     logger.warning(f"Falha ao carregar dados recentes para {tf_label} do histórico Redis.")
                     all_data_available_for_analysis = False # Marca que faltou dado essencial

            except Exception as e:
                 logger.error(f"Erro ao buscar/processar dados recentes para {tf_label}.", exc_info=True)
                 all_data_available_for_analysis = False
                 df = None; sma_f = None; sma_s = None # Garante que são None

            # Guarda no dicionário mta_data (apenas últimas velas para o prompt)
            mta_data[tf_label] = {'df': df.tail(15) if df is not None else None, 'sma_fast': sma_f, 'sma_slow': sma_s}


        # --- PASSO 3: Análise Gemini com Dados MTA ---
        trade_signal: str | None = None
        if all_data_available_for_analysis:
            logger.info(f"Enviando dados MTA para análise Gemini ({gemini_analyzer.model_name})...")
            # Chama a função MTA corrigida
            trade_signal = gemini_analyzer.get_trade_signal_mta(mta_data, symbol)
            logger.info(f"Sinal Obtido Gemini (MTA): {trade_signal if trade_signal else 'Nenhum/Erro'}")
            if trade_signal:
                # Mensagem Telegram sem formatação
                message = f"Sinal MTA Quantis ({symbol} - {analysis_interval}): {trade_signal}"
                send_telegram_message(message)
        else:
            logger.warning("Análise Gemini ignorada pois faltaram dados recentes de algum timeframe.")
            send_telegram_message(f"Alerta ({symbol}): Falha ao carregar dados recentes de TFs para analise MTA.", disable_notification=True)


        # --- PASSO 4: Decisão e Ação (Simulada) da Estratégia ---
        logger.info(f"Executando estratégia para {symbol} com sinal MTA '{trade_signal}'...")
        strategy_manager.decide_action(trade_signal) # Usa logger interno e envia msg Telegram s/ formatação

    # --- Tratamento de Erro do Ciclo ---
    except Exception as e:
        logger.critical("Erro CRÍTICO inesperado durante ciclo de trade.", exc_info=True)
        try:
            # Mensagem Telegram sem formatação
            critical_message = f"ERRO CRITICO no Ciclo ({symbol}):\nVerifique {LOG_FILE}.\nErro: {str(e)}"
            send_telegram_message(critical_message[:4000])
        except Exception as telegram_err:
            logger.error("Falha enviar notificação erro ciclo.", exc_info=True)
    # --- Finalização do Ciclo ---
    finally:
        end_cycle_time = datetime.datetime.now()
        cycle_duration = end_cycle_time - start_cycle_time
        logger.info(f"--- Ciclo concluído em {cycle_duration}. ({end_cycle_time.strftime('%Y-%m-%d %H:%M:%S')}) ---")

# --- Função Principal ---
def main():
    setup_logging(level=logging.INFO) # Configura logging INFO (ou DEBUG)
    main_logger = logging.getLogger('main_runner')

    main_logger.info("--- Iniciando Quantis Crypto Trader - Gemini Version (Histórico Redis) ---")
    send_telegram_message("Quantis Crypto Trader (Hist Redis) iniciando...")

    if not initialize_services():
        main_logger.critical("Falha na inicialização. Encerrando.")
        return

    main_logger.info("Inicialização concluída. Configurando agendamento...")

    # Define o intervalo do ciclo principal (ex: 15 minutos)
    main_cycle_interval_minutes = 15
    schedule.every(main_cycle_interval_minutes).minutes.do(trade_cycle)
    # Mantém 1 minuto para teste rápido se descomentar:
    # schedule.every(1).minutes.do(trade_cycle)
    # main_cycle_interval_minutes = 1

    job = schedule.get_jobs()[0]
    main_logger.info(f"Ciclo de trade agendado para rodar a cada {job.interval} {job.unit if hasattr(job, 'unit') else 'minutes'}.")

    main_logger.info("Executando o primeiro ciclo imediatamente...")
    trade_cycle()

    main_logger.info("Agendamento configurado. Entrando no loop principal de espera...")
    send_telegram_message("Robo MTA (Hist Redis) online e operando (modo simulado).")

    # Loop principal do schedule
    try:
        while True:
            schedule.run_pending()
            time.sleep(1) # Verifica a cada segundo se há jobs agendados
    except KeyboardInterrupt:
        main_logger.info("Interrupção manual (Ctrl+C). Encerrando...")
        send_telegram_message("Quantis Crypto Trader (Hist Redis) encerrado manualmente.")
    except Exception as e:
        main_logger.critical("Erro CRÍTICO inesperado no loop principal.", exc_info=True)
        send_telegram_message(f"ERRO CRITICO LOOP PRINCIPAL (Hist Redis)! Encerrando. Verifique logs.\nErro: {str(e)[:500]}")
    finally:
         main_logger.info("--- Quantis Crypto Trader (Hist Redis) Finalizado ---")

if __name__ == "__main__":
    main()