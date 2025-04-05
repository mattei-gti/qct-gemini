# quantis_crypto_trader_gemini/main.py

import config
from database import init_db
from binance_client import BinanceHandler
from binance.client import Client
from redis_client import RedisHandler
from gemini_analyzer import GeminiAnalyzer
from telegram_interface import send_telegram_message, escape_markdown_v2
from strategy import StrategyManager # Importa o gerenciador de estratégia
import pandas as pd
import datetime
import schedule # Para agendamento
import time # Para o sleep do schedule

# --- Handlers Globais ---
# Definimos fora da função main para serem acessíveis pelo job agendado
binance_handler: BinanceHandler | None = None
redis_handler: RedisHandler | None = None
gemini_analyzer: GeminiAnalyzer | None = None
strategy_manager: StrategyManager | None = None

def initialize_services():
    """Inicializa todos os serviços necessários."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager
    print("Inicializando serviços...")
    try:
        init_db()
        config.load_or_set_initial_db_settings()
        redis_handler = RedisHandler(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
        binance_handler = BinanceHandler(api_key=config.BINANCE_API_KEY, api_secret=config.BINANCE_SECRET_KEY)
        gemini_analyzer = GeminiAnalyzer(api_key=config.GEMINI_API_KEY)
        strategy_manager = StrategyManager(redis_handler=redis_handler, binance_handler=binance_handler)
        print("Todos os serviços inicializados com sucesso.")
        return True
    except Exception as e:
        print(f"Erro CRÍTICO durante a inicialização dos serviços: {e}")
        try:
            critical_message = f"🆘 *ERRO CRÍTICO NA INICIALIZAÇÃO*:\nNão foi possível iniciar os serviços do robô\\.\nErro: {escape_markdown_v2(str(e))}"
            send_telegram_message(critical_message)
        except Exception as telegram_err:
            print(f"ERRO ADICIONAL: Falha ao enviar notificação de erro de inicialização para o Telegram: {telegram_err}")
        return False

def trade_cycle():
    """Executa um ciclo completo de coleta, análise e decisão."""
    global binance_handler, redis_handler, gemini_analyzer, strategy_manager

    # Verifica se os handlers foram inicializados corretamente
    if not all([binance_handler, redis_handler, gemini_analyzer, strategy_manager]):
         print("Erro: Serviços não inicializados corretamente. Abortando ciclo.")
         # Poderíamos tentar re-inicializar ou apenas parar
         # schedule.clear() # Para o agendamento se falhar
         return

    start_cycle_time = datetime.datetime.now()
    print(f"\n--- Iniciando Ciclo de Trade em {start_cycle_time.strftime('%Y-%m-%d %H:%M:%S')} ---")

    # Definições do ciclo (poderiam vir de config/DB)
    symbol = strategy_manager.symbol # Pega o símbolo do strategy manager
    interval = Client.KLINE_INTERVAL_1HOUR
    limit = 100 # Buscamos mais velas para análise potencialmente melhor

    klines_df: pd.DataFrame | None = None
    trade_signal: str | None = None

    try:
        # 1. Coleta de Dados (com Cache)
        cache_key = redis_handler._generate_kline_key(symbol, interval)
        klines_df = redis_handler.get_dataframe(cache_key)

        if klines_df is not None:
            print(f"--- Cache HIT para {cache_key} ---")
            # Verificar se o cache é recente o suficiente? Talvez comparar o último timestamp?
            # Por enquanto, usamos como está.
        else:
            print(f"--- Cache MISS para {cache_key} ---")
            print("Buscando dados na API da Binance...")
            # Pega um número maior de velas para a análise do Gemini
            klines_df = binance_handler.get_klines(symbol=symbol, interval=interval, limit=limit)
            if klines_df is not None:
                # TTL um pouco menor que o intervalo da vela
                ttl = 55 * 60 # 55 minutos
                redis_handler.cache_dataframe(cache_key, klines_df, ttl_seconds=ttl)
            else:
                print(f"Não foi possível buscar klines para {symbol} na Binance.")

        # 2. Análise Gemini
        if klines_df is not None:
             print(f"Enviando {len(klines_df)} klines para análise do Gemini...")
             trade_signal = gemini_analyzer.get_trade_signal(klines_df, symbol)
             print(f"Sinal de Trade Obtido do Gemini: {trade_signal if trade_signal else 'Nenhum/Erro'}")
             # Envia sinal para Telegram
             if trade_signal:
                 message = f"📊 *Sinal Quantis Trader* \\({escape_markdown_v2(symbol)} \\- {escape_markdown_v2(interval)}\\)\\:\n`{escape_markdown_v2(trade_signal)}`"
                 send_telegram_message(message)
             # Não enviamos mais a msg de erro daqui, strategy decide se notifica

        else:
             print("\nSem dados de Klines para enviar para análise do Gemini.")
             send_telegram_message(f"⚠️ *Alerta* ({escape_markdown_v2(symbol)}): Sem dados de Klines para análise neste ciclo.", disable_notification=True)


        # 3. Decisão e Ação (Simulada) da Estratégia
        print("\nExecutando estratégia...")
        strategy_manager.decide_action(trade_signal)

    except Exception as e:
        print(f"Erro durante o ciclo de trade: {e}")
        try:
            error_text_escaped = escape_markdown_v2(str(e))
            critical_message = f"🆘 *ERRO CRÍTICO no Ciclo* ({escape_markdown_v2(symbol)}):\n```\n{error_text_escaped}\n```"
            if len(critical_message) > 4000: critical_message = critical_message[:4000] + "\n\\.\\.\\. (erro truncado)"
            send_telegram_message(critical_message)
        except Exception as telegram_err:
            print(f"ERRO ADICIONAL: Falha ao enviar notificação de erro de ciclo para o Telegram: {telegram_err}")

    finally:
        end_cycle_time = datetime.datetime.now()
        cycle_duration = end_cycle_time - start_cycle_time
        print(f"--- Ciclo concluído em {cycle_duration}. ({end_cycle_time.strftime('%Y-%m-%d %H:%M:%S')}) ---")


# --- Função Principal ---
def main():
    print("Iniciando o Quantis Crypto Trader - Gemini Version...")
    send_telegram_message("🚀 Quantis Crypto Trader iniciando\\.\\.\\.")

    if not initialize_services():
        print("Falha na inicialização dos serviços. Encerrando.")
        return # Sai se não conseguir inicializar

    print("\nInicialização concluída. Configurando agendamento...")

    # --- Agendamento ---
    # Executa o ciclo de trade a cada hora, no minuto :01 (para dar tempo da vela fechar)
    # schedule.every().hour.at(":01").do(trade_cycle)
    # Para testar, vamos rodar a cada 1 minuto:
    schedule.every(1).minutes.do(trade_cycle)

    # Executa o ciclo uma vez imediatamente ao iniciar
    print("Executando o primeiro ciclo imediatamente...")
    trade_cycle()

    print("\nAgendamento configurado. Entrando no loop principal...")
    send_telegram_message("✅ Robô online e aguardando agendamento\\.")

    while True:
        # Verifica se há tarefas agendadas para rodar
        schedule.run_pending()
        # Espera um pouco para não consumir CPU excessivamente
        time.sleep(1)


if __name__ == "__main__":
    main()