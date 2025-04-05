# quantis_crypto_trader_gemini/main.py

import config
from database import init_db
from binance_client import BinanceHandler
from binance.client import Client
from redis_client import RedisHandler
from gemini_analyzer import GeminiAnalyzer
from telegram_interface import send_telegram_message, escape_markdown_v2
from strategy import StrategyManager
import pandas as pd
import datetime
import schedule
import time
import logging # Importa o módulo de logging
import sys # Para direcionar para stdout

# --- Configuração do Logging ---
LOG_FILE = "quantis_trader.log" # Nome do arquivo de log

def setup_logging(level=logging.INFO):
    """Configura o logging para console e arquivo."""
    # Verifica se handlers já foram adicionados ao root logger para evitar duplicação
    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        # Logger já configurado (útil se esta função for chamada acidentalmente mais de uma vez)
        # root_logger.info("Logger já configurado anteriormente.")
        return

    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger() # Pega o root logger configurado
    logger.info("--- Logging configurado ---")
    # Opcional: Reduzir o nível de log de bibliotecas muito "falantes"
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('schedule').setLevel(logging.WARNING) # Silencia logs de 'schedule' a cada execução

# --- Handlers Globais ---
binance_handler: BinanceHandler | None = None
redis_handler: RedisHandler | None = None
gemini_analyzer: GeminiAnalyzer | None = None
strategy_manager: StrategyManager | None = None
# Obtém o logger para este módulo específico
logger = logging.getLogger(__name__)

# --- Funções de Inicialização e Ciclo de Trade ---

def initialize_services():
    """Inicializa todos os serviços necessários."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager
    logger.info("Inicializando serviços...")
    try:
        init_db() # Internamente usa logger
        config.load_or_set_initial_db_settings() # Internamente usa logger
        redis_handler = RedisHandler(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB) # Usa logger no init
        binance_handler = BinanceHandler(api_key=config.BINANCE_API_KEY, api_secret=config.BINANCE_SECRET_KEY) # Usa logger no init
        gemini_analyzer = GeminiAnalyzer(api_key=config.GEMINI_API_KEY) # Usa logger no init
        strategy_manager = StrategyManager(redis_handler=redis_handler, binance_handler=binance_handler) # Usa logger no init
        logger.info("Todos os serviços inicializados com sucesso.")
        return True
    except Exception as e:
        # Logger já captura o traceback com exc_info=True
        logger.critical(f"Erro CRÍTICO durante a inicialização dos serviços: {e}", exc_info=True)
        try:
            critical_message = f"🆘 *ERRO CRÍTICO NA INICIALIZAÇÃO*:\nNão foi possível iniciar os serviços do robô\\.\nErro: {escape_markdown_v2(str(e))}"
            if len(critical_message) > 4000: critical_message = critical_message[:4000] + "\n\\.\\.\\."
            send_telegram_message(critical_message)
        except Exception as telegram_err:
            logger.error(f"Falha ao enviar notificação de erro de inicialização para o Telegram.", exc_info=True)
        return False

def trade_cycle():
    """Executa um ciclo completo de coleta, análise e decisão."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager

    if not all([binance_handler, redis_handler, gemini_analyzer, strategy_manager]):
         logger.error("Serviços não inicializados corretamente. Abortando ciclo.")
         # Considerar parar o schedule se for um erro persistente
         # schedule.clear()
         return

    start_cycle_time = datetime.datetime.now()
    logger.info(f"--- Iniciando Ciclo de Trade em {start_cycle_time.strftime('%Y-%m-%d %H:%M:%S')} ---")

    # Definições do ciclo (poderiam vir de config/DB)
    symbol = strategy_manager.symbol
    interval = Client.KLINE_INTERVAL_1HOUR
    # Buscar mais klines pode dar mais contexto para a análise (ex: 200)
    # Mas o prompt precisa ser ajustado para lidar com mais dados ou resumir
    limit = 100 # Mantendo 100 por enquanto

    klines_df: pd.DataFrame | None = None
    trade_signal: str | None = None

    try:
        # 1. Coleta de Dados (com Cache)
        cache_key = redis_handler._generate_kline_key(symbol, interval) # Internamente usa logger.debug
        klines_df = redis_handler.get_dataframe(cache_key) # Internamente usa logger

        if klines_df is None:
            logger.info(f"--- Cache MISS para {cache_key} ---")
            logger.info(f"Buscando {limit} klines para {symbol} na API da Binance...")
            klines_df = binance_handler.get_klines(symbol=symbol, interval=interval, limit=limit) # Usa logger interno
            if klines_df is not None:
                # TTL um pouco menor que o intervalo da vela (55 min para 1h)
                ttl = 55 * 60
                redis_handler.cache_dataframe(cache_key, klines_df, ttl_seconds=ttl) # Usa logger interno
                # logger.info(f"Klines para {symbol} buscados e armazenados no cache.") # Log já está em cache_dataframe
            else:
                 logger.warning(f"Não foi possível buscar klines para {symbol} na Binance neste ciclo.")
        # else: # O log de Cache HIT já está em get_dataframe
             # logger.info(f"--- Cache HIT para {cache_key} ---")

        # 2. Análise Gemini
        if klines_df is not None:
             logger.info(f"Enviando {len(klines_df)} klines para análise do Gemini ({gemini_analyzer.model_name})...")
             trade_signal = gemini_analyzer.get_trade_signal(klines_df, symbol) # Usa logger interno
             logger.info(f"Sinal de Trade Obtido do Gemini: {trade_signal if trade_signal else 'Nenhum/Erro'}")
             # Envia sinal para Telegram se obtido
             if trade_signal:
                 message = f"📊 *Sinal Quantis Trader* \\({escape_markdown_v2(symbol)} \\- {escape_markdown_v2(interval)}\\)\\:\n`{escape_markdown_v2(trade_signal)}`"
                 send_telegram_message(message) # Usa logger interno
             # O cenário de Nenhum/Erro já é logado dentro de get_trade_signal
        else:
             logger.warning(f"Sem dados de Klines para {symbol}. Análise Gemini ignorada.")
             # Notificar Telegram sobre falta de dados?
             # send_telegram_message(f"⚠️ *Alerta* ({escape_markdown_v2(symbol)}): Sem dados de Klines para análise neste ciclo.", disable_notification=True)

        # 3. Decisão e Ação (Simulada) da Estratégia
        logger.info(f"Executando estratégia para {symbol} com sinal '{trade_signal}'...")
        strategy_manager.decide_action(trade_signal) # Usa logger interno

    except Exception as e:
        # Captura qualquer exceção não tratada durante o ciclo
        logger.critical(f"Erro CRÍTICO inesperado durante o ciclo de trade.", exc_info=True)
        try:
            error_text_escaped = escape_markdown_v2(str(e))
            critical_message = f"🆘 *ERRO CRÍTICO no Ciclo* ({escape_markdown_v2(symbol)}):\nOcorreu uma exceção grave\\. Verifique o arquivo de log `{LOG_FILE}`\\.\nErro: ```\n{error_text_escaped}\n```"
            if len(critical_message) > 4000: critical_message = critical_message[:4000] + "\n\\.\\.\\. (erro truncado)"
            send_telegram_message(critical_message)
        except Exception as telegram_err:
            logger.error(f"Falha ao enviar notificação de erro de ciclo para o Telegram.", exc_info=True)

    finally:
        end_cycle_time = datetime.datetime.now()
        cycle_duration = end_cycle_time - start_cycle_time
        logger.info(f"--- Ciclo concluído em {cycle_duration}. ({end_cycle_time.strftime('%Y-%m-%d %H:%M:%S')}) ---")


# --- Função Principal ---
def main():
    # Configura o logging ANTES de qualquer outra coisa
    setup_logging(level=logging.INFO) # Configura para INFO (ou DEBUG para mais detalhes)

    # Obtém um logger específico para a função main, se desejado
    main_logger = logging.getLogger('main_runner') # Nome diferente do módulo

    main_logger.info(f"--- Iniciando Quantis Crypto Trader - Gemini Version ---")
    send_telegram_message("🚀 Quantis Crypto Trader iniciando\\.\\.\\.")

    if not initialize_services():
        main_logger.critical("Falha na inicialização dos serviços. Encerrando.")
        # Mensagem de erro já foi enviada ao Telegram dentro de initialize_services
        return # Sai se não conseguir inicializar

    main_logger.info("Inicialização concluída. Configurando agendamento...")

    # --- Agendamento ---
    # Executa o ciclo a cada hora, no minuto :01 (exemplo)
    # schedule.every().hour.at(":01").do(trade_cycle)
    # Para testar (a cada 1 minuto):
    schedule.every(1).minutes.do(trade_cycle)
    main_logger.info("Ciclo de trade agendado para rodar a cada 1 minuto(s).")

    # Executa o ciclo uma vez imediatamente ao iniciar
    main_logger.info("Executando o primeiro ciclo imediatamente...")
    trade_cycle()

    main_logger.info("Agendamento configurado. Entrando no loop principal de espera...")
    send_telegram_message("✅ Robô online e operando \\(modo simulado\\)\\.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1) # Pausa pequena para não consumir 100% CPU
    except KeyboardInterrupt:
        main_logger.info("Interrupção manual recebida (Ctrl+C). Encerrando...")
        send_telegram_message("🛑 Quantis Crypto Trader encerrado manualmente\\.")
    except Exception as e:
        main_logger.critical("Erro CRÍTICO inesperado no loop principal.", exc_info=True)
        send_telegram_message("🆘 *ERRO CRÍTICO NO LOOP PRINCIPAL*\\! Encerrando\\. Verifique os logs\\.")
    finally:
         main_logger.info("--- Quantis Crypto Trader Finalizado ---")


if __name__ == "__main__":
    main()