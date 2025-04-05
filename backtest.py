# quantis_crypto_trader_gemini/backtest.py

import logging
import sys
import pandas as pd
import numpy as np # Usaremos numpy para gerar sinais
import datetime
import config
from binance_client import BinanceHandler
# import talib # Descomente se quiser usar TA-Lib em vez de cálculo manual

# --- Configuração do Logging (mantém igual) ---
LOG_FILE_BACKTEST = "backtest_run.log"
def setup_backtest_logging(level=logging.INFO):
    bt_logger = logging.getLogger('backtester')
    for handler in bt_logger.handlers[:]: bt_logger.removeHandler(handler)
    bt_logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    fh = logging.FileHandler(LOG_FILE_BACKTEST, mode='w', encoding='utf-8')
    fh.setFormatter(formatter)
    bt_logger.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    bt_logger.addHandler(ch)
    bt_logger.info("--- Logging do Backtester configurado ---")
    return bt_logger
logger = setup_backtest_logging(level=logging.INFO) # Use INFO ou DEBUG

# --- Parâmetros do Backtest (mantém igual) ---
SYMBOL = "BTCUSDT"
INTERVAL = "1h"
START_DATE = "6 months ago UTC"
END_DATE = None

# Parâmetros da Estratégia (SMA Crossover)
SMA_FAST_PERIOD = 10
SMA_SLOW_PERIOD = 30

# Parâmetros da Simulação
INITIAL_CASH = 1000.0 # Saldo inicial em USDT
COMMISSION_RATE = 0.001 # Taxa de comissão por trade (0.1% - exemplo, ajuste conforme a Binance)

# --- Função Principal do Backtest ---

def run_backtest():
    logger.info("==== INICIANDO BACKTEST ====")
    logger.info(f"Par: {SYMBOL}, Intervalo: {INTERVAL}")
    logger.info(f"Período: Desde '{START_DATE}' até '{END_DATE if END_DATE else 'Agora'}'")
    logger.info(f"Estratégia: SMA Crossover ({SMA_FAST_PERIOD} / {SMA_SLOW_PERIOD})")
    logger.info(f"Capital Inicial: {INITIAL_CASH} USDT, Comissão: {COMMISSION_RATE*100:.2f}%")

    # 1. Inicializar Cliente Binance
    try:
        binance_handler = BinanceHandler(config.BINANCE_API_KEY, config.BINANCE_SECRET_KEY)
    except Exception as e:
        logger.critical("Falha ao inicializar BinanceHandler.", exc_info=True)
        return

    # 2. Buscar Dados Históricos
    logger.info("Buscando dados históricos...")
    data = binance_handler.get_historical_klines(
        symbol=SYMBOL, interval=INTERVAL, start_str=START_DATE, end_str=END_DATE
    )
    if data is None or data.empty:
        logger.critical("Não foi possível obter dados históricos. Encerrando backtest.")
        return
    logger.info(f"Total de {len(data)} velas históricas obtidas ({data.index.min()} a {data.index.max()}).")

    # 3. Calcular Indicadores (SMAs)
    logger.info(f"Calculando SMA {SMA_FAST_PERIOD} e SMA {SMA_SLOW_PERIOD}...")
    data['SMA_Fast'] = data['Close'].rolling(window=SMA_FAST_PERIOD).mean()
    data['SMA_Slow'] = data['Close'].rolling(window=SMA_SLOW_PERIOD).mean()
    # Remove as linhas iniciais onde as SMAs são NaN (não podemos operar sem elas)
    data.dropna(inplace=True)
    if data.empty:
        logger.critical("Não há dados suficientes para calcular as SMAs. Aumente o período histórico.")
        return
    logger.info("Indicadores calculados.")
    logger.debug(f"Últimas linhas com SMAs:\n{data.tail(3)}")

    # 4. Gerar Sinais com base nos Indicadores
    logger.info("Gerando sinais de Crossover...")
    # 'Position': 1 se SMA Rápida > SMA Lenta, -1 caso contrário
    data['Position'] = np.where(data['SMA_Fast'] > data['SMA_Slow'], 1, -1)
    # 'Signal': 1 para BUY (cruzou para cima), -1 para SELL (cruzou para baixo), 0 para HOLD
    # O sinal ocorre quando a 'Position' muda em relação ao período anterior (shift(1))
    data['Signal'] = np.where(data['Position'] > data['Position'].shift(1), 1,
                              np.where(data['Position'] < data['Position'].shift(1), -1, 0))
    # Remove a primeira linha onde shift(1) é NaN
    data = data.iloc[1:]
    if data.empty:
        logger.critical("Não há dados suficientes após cálculo do sinal. Aumente o período histórico.")
        return

    buy_signals = data[data['Signal'] == 1]
    sell_signals = data[data['Signal'] == -1]
    logger.info(f"Sinais gerados: {len(buy_signals)} BUYs, {len(sell_signals)} SELLs.")
    logger.debug(f"Exemplo de Sinais BUY:\n{buy_signals.head(2)}")
    logger.debug(f"Exemplo de Sinais SELL:\n{sell_signals.head(2)}")

    # 5. Loop de Simulação
    logger.info("Iniciando simulação de trades...")
    cash = INITIAL_CASH
    holding = 0.0 # Quantidade do ativo BASE (BTC) que possuímos
    in_position = False # Flag para indicar se estamos comprados
    trades_log = [] # Lista para registrar os trades

    # Itera sobre cada vela (período) nos dados históricos
    for index, row in data.iterrows():
        signal = row['Signal']
        close_price = row['Close'] # Usamos o preço de fechamento para simular a execução

        # --- Lógica de Compra ---
        if signal == 1 and not in_position: # Se é sinal de BUY e não estamos comprados
            # Calcula quanto do ativo BASE podemos comprar com nosso caixa
            amount_to_buy = (cash / close_price) * (1 - COMMISSION_RATE) # Aplica comissão
            holding = amount_to_buy
            cash = 0.0 # Gastamos todo o caixa
            in_position = True # Marcamos que estamos comprados
            trade_info = {
                'Timestamp': index, 'Type': 'BUY', 'Price': close_price,
                'Amount': holding, 'Cash_After': cash
            }
            trades_log.append(trade_info)
            logger.info(f"Executado BUY @ {close_price:.2f} | Holding: {holding:.6f} BTC | Cash: {cash:.2f} | Time: {index}")

        # --- Lógica de Venda ---
        elif signal == -1 and in_position: # Se é sinal de SELL e estamos comprados
            # Calcula quanto caixa recebemos pela venda do ativo BASE
            cash_received = (holding * close_price) * (1 - COMMISSION_RATE) # Aplica comissão
            cash = cash_received
            holding = 0.0 # Vendemos tudo
            in_position = False # Marcamos que não estamos mais comprados
            trade_info = {
                'Timestamp': index, 'Type': 'SELL', 'Price': close_price,
                'Amount': cash_received / close_price / (1 - COMMISSION_RATE), # Amount vendido antes da comissão
                'Cash_After': cash
            }
            trades_log.append(trade_info)
            logger.info(f"Executado SELL @ {close_price:.2f} | Holding: {holding:.6f} BTC | Cash: {cash:.2f} | Time: {index}")

        # --- Lógica HOLD ---
        # Nenhuma ação necessária se signal == 0 ou se o sinal for incoerente com a posição

    logger.info("Simulação de trades concluída.")

    # 6. Calcular e Exibir Resultados
    logger.info("Calculando resultados finais...")
    final_portfolio_value = cash # Valor final é o caixa se não estivermos posicionados
    if in_position:
        # Se terminou comprado, calcula o valor da posição com o último preço
        last_price = data['Close'].iloc[-1]
        final_portfolio_value = (holding * last_price) * (1 - COMMISSION_RATE) # Simula venda final com comissão
        logger.info(f"Simulação terminou em posição. Valor final calculado com último preço {last_price:.2f} (inclui comissão de saída).")

    total_pnl = final_portfolio_value - INITIAL_CASH
    total_return_pct = (total_pnl / INITIAL_CASH) * 100

    logger.info("==== RESULTADOS DO BACKTEST ====")
    logger.info(f"Período Analisado: {data.index.min()} a {data.index.max()}")
    logger.info(f"Capital Inicial: {INITIAL_CASH:.2f} USDT")
    logger.info(f"Valor Final do Portfólio: {final_portfolio_value:.2f} USDT")
    logger.info(f"Lucro/Prejuízo Total (P&L): {total_pnl:.2f} USDT")
    logger.info(f"Retorno Total: {total_return_pct:.2f}%")
    logger.info(f"Total de Trades Executados: {len(trades_log)}")

    # Opcional: Salvar ou exibir log de trades
    if trades_log:
        trades_df = pd.DataFrame(trades_log)
        logger.debug(f"Log de Trades:\n{trades_df.to_string()}")
        # trades_df.to_csv("backtest_trades.csv") # Salva em CSV

    logger.info("==== BACKTEST CONCLUÍDO ====")


# --- Execução ---
if __name__ == "__main__":
    run_backtest()