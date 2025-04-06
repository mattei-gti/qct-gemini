# quantis_crypto_trader_gemini/populate_history.py

import logging
import sys
import time
import datetime
import pandas as pd
from binance.client import Client # Importa Client para constantes de intervalo
import config
from binance_client import BinanceHandler
from redis_client import RedisHandler

# --- Configuração do Logging ---
LOG_FILE_POPULATE = "populate_history.log"

def setup_populate_logging(level=logging.INFO):
    """Configura um logger para o script de população."""
    pop_logger = logging.getLogger('populate_history')
    # Limpa handlers antigos para evitar duplicação em re-execuções no mesmo processo (improvável aqui)
    for handler in pop_logger.handlers[:]: pop_logger.removeHandler(handler); handler.close()

    pop_logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    try: # Configura file handler
        fh = logging.FileHandler(LOG_FILE_POPULATE, mode='a', encoding='utf-8') # Append mode
        fh.setFormatter(formatter)
        pop_logger.addHandler(fh)
    except Exception as e: print(f"Erro config FileHandler populate: {e}")

    ch = logging.StreamHandler(sys.stdout) # Configura console handler
    ch.setFormatter(formatter)
    pop_logger.addHandler(ch)
    pop_logger.propagate = False # Evita duplicar logs se root logger estiver config.
    pop_logger.info("--- Logging do Populate History configurado ---")
    return pop_logger

logger = setup_populate_logging(level=logging.INFO)

# --- Parâmetros de População ---
OVERALL_START_DATE_STR = "1 Jan, 2017" # Data inicial para buscar se não houver histórico
SYMBOLS = ["BTCUSDT"] # Lista de símbolos a popular
# Mapeia nome amigável para constante da Binance
INTERVALS_TO_POPULATE = {
    "1M": Client.KLINE_INTERVAL_1MONTH,
    "1w": Client.KLINE_INTERVAL_1WEEK,
    "1d": Client.KLINE_INTERVAL_1DAY,
    "4h": Client.KLINE_INTERVAL_4HOUR,
    "1h": Client.KLINE_INTERVAL_1HOUR,
    "15m": Client.KLINE_INTERVAL_15MINUTE,
    "1m": Client.KLINE_INTERVAL_1MINUTE,
}
# Delay entre o processamento de cada par/intervalo para evitar rate limit
SLEEP_BETWEEN_TASKS = 2 # Segundos

# Helper para obter a duração do intervalo em milissegundos (aproximado para meses)
def get_interval_ms(interval: str) -> int | None:
    """Retorna a duração aproximada do intervalo em milissegundos."""
    multipliers = {'m': 60, 'h': 3600, 'd': 86400, 'w': 604800, 'M': 2592000} # Segundos por unidade
    try:
        unit = interval[-1].lower()
        value = int(interval[:-1])
        if unit in multipliers:
            # Caso especial para meses (aproximação 30 dias)
            if unit == 'M':
                 return value * 30 * 24 * 60 * 60 * 1000
            # Caso especial para semanas
            elif unit == 'w':
                 return value * 7 * 24 * 60 * 60 * 1000
            # Outros casos (m, h, d)
            else:
                 seconds = value * multipliers[unit]
                 return seconds * 1000
    except Exception:
        # Fallback para constantes da binance (se usadas diretamente)
        if interval == Client.KLINE_INTERVAL_1MINUTE: return 60 * 1000
        if interval == Client.KLINE_INTERVAL_15MINUTE: return 15 * 60 * 1000
        if interval == Client.KLINE_INTERVAL_1HOUR: return 60 * 60 * 1000
        if interval == Client.KLINE_INTERVAL_4HOUR: return 4 * 60 * 60 * 1000 # <-- Adicionado
        if interval == Client.KLINE_INTERVAL_1DAY: return 24 * 60 * 60 * 1000
        if interval == Client.KLINE_INTERVAL_1WEEK: return 7 * 24 * 60 * 60 * 1000 # <-- Adicionado
        if interval == Client.KLINE_INTERVAL_1MONTH: return 30 * 24 * 60 * 60 * 1000 # Aproximação

    logger.warning(f"Duração em ms desconhecida para intervalo: {interval}")
    return None

# --- Função Principal de População ---
def populate_all_history():
    logger.info("==== INICIANDO POPULAÇÃO DO HISTÓRICO NO REDIS ====")
    logger.info(f"Símbolos: {SYMBOLS}")
    logger.info(f"Intervalos: {list(INTERVALS_TO_POPULATE.keys())}")
    logger.info(f"Data inicial padrão: {OVERALL_START_DATE_STR}")

    # 1. Inicializar Clientes
    try:
        logger.info("Inicializando Redis Handler...")
        redis_h = RedisHandler(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
        logger.info("Inicializando Binance Handler...")
        binance_h = BinanceHandler(config.BINANCE_API_KEY, config.BINANCE_SECRET_KEY)
    except Exception as e:
        logger.critical("Falha ao inicializar handlers. Encerrando.", exc_info=True)
        return

    # 2. Loop principal por Símbolo e Intervalo
    total_candles_added = 0
    start_time_total = time.time()

    for symbol in SYMBOLS:
        for interval_label, interval_code in INTERVALS_TO_POPULATE.items():
            logger.info(f"\n--- Processando: {symbol} / {interval_label} ({interval_code}) ---")
            task_start_time = time.time()

            # 2a. Verifica último timestamp no Redis para este par/intervalo
            last_ts_ms = redis_h.get_last_hist_timestamp(symbol, interval_code)

            # 2b. Determina a data de início da busca na Binance
            start_fetch_str = OVERALL_START_DATE_STR # Padrão se não houver nada no Redis
            if last_ts_ms:
                # Calcula o próximo timestamp esperado (último + 1 intervalo)
                interval_ms = get_interval_ms(interval_code)
                if interval_ms:
                    next_expected_ts_ms = last_ts_ms + interval_ms
                    # Converte para string (timestamp ms como string é aceito pela API)
                    start_fetch_str = str(next_expected_ts_ms)
                    start_dt = pd.to_datetime(next_expected_ts_ms, unit='ms')
                    logger.info(f"Último timestamp no Redis: {pd.to_datetime(last_ts_ms, unit='ms')}. Buscando a partir de {start_dt} ({start_fetch_str} ms)...")
                else:
                    logger.warning(f"Não foi possível determinar a duração do intervalo {interval_code}, buscando desde {OVERALL_START_DATE_STR}.")
            else:
                 logger.info(f"Nenhum histórico encontrado no Redis. Buscando desde {OVERALL_START_DATE_STR}...")

            # 2c. Busca dados históricos da Binance (a biblioteca lida com paginação)
            logger.info(f"Chamando get_historical_klines para {symbol}/{interval_code} desde '{start_fetch_str}'...")
            try:
                # Busca desde start_fetch_str até o momento atual (end_str=None)
                klines_df = binance_h.get_historical_klines(
                    symbol=symbol,
                    interval=interval_code,
                    start_str=start_fetch_str,
                    end_str=None # Busca até o fim
                )

                # 2d. Adiciona os dados ao Redis se algo foi retornado
                if klines_df is not None and not klines_df.empty:
                    logger.info(f"{len(klines_df)} novas velas recebidas da Binance.")
                    # Adiciona ao Sorted Set no Redis
                    added_count = redis_h.add_klines_to_hist(symbol, interval_code, klines_df)
                    logger.info(f"{added_count} velas efetivamente adicionadas/atualizadas no histórico Redis.")
                    total_candles_added += added_count if added_count else 0 # Soma ao total geral
                elif klines_df is not None and klines_df.empty:
                     logger.info("Nenhuma vela nova encontrada na Binance desde o último timestamp.")
                else:
                    # get_historical_klines retornou None (erro já logado dentro da função)
                    logger.warning(f"Falha ao buscar dados históricos para {symbol}/{interval_code} neste ciclo.")

            except Exception as e:
                 logger.error(f"Erro durante busca ou adição para {symbol}/{interval_code}.", exc_info=True)

            task_duration = time.time() - task_start_time
            logger.info(f"Processamento de {symbol}/{interval_label} concluído em {task_duration:.2f} segundos.")

            # Pausa para evitar Rate Limit antes de processar o próximo item
            logger.debug(f"Aguardando {SLEEP_BETWEEN_TASKS} segundos antes da próxima tarefa...")
            time.sleep(SLEEP_BETWEEN_TASKS)


    # 3. Conclusão
    total_duration = time.time() - start_time_total
    logger.info("\n==== POPULAÇÃO DO HISTÓRICO CONCLUÍDA ====")
    logger.info(f"Total de velas adicionadas/atualizadas nesta execução: {total_candles_added}")
    logger.info(f"Tempo total de execução: {str(datetime.timedelta(seconds=total_duration))}")

# --- Execução ---
if __name__ == "__main__":
    populate_all_history()