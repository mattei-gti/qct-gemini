# quantis_crypto_trader_gemini/backtest.py

import logging
import sys
import pandas as pd
import numpy as np
import datetime
import time # Para converter datas
import config
# Removido import do BinanceHandler, não precisamos mais dele aqui
from redis_client import RedisHandler # Importa RedisHandler
import quantstats as qs
import matplotlib.pyplot as plt
import itertools

# --- Configuração do Logging (mantém igual) ---
LOG_FILE_BACKTEST = "backtest_run.log"
def setup_backtest_logging(level=logging.INFO):
    # ... (código setup_backtest_logging como antes) ...
    bt_logger = logging.getLogger('backtester'); # ... (limpeza e handlers como antes) ...
    for handler in bt_logger.handlers[:]: bt_logger.removeHandler(handler); handler.close()
    bt_logger.setLevel(level); formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    try: fh = logging.FileHandler(LOG_FILE_BACKTEST, mode='w', encoding='utf-8'); fh.setFormatter(formatter); bt_logger.addHandler(fh)
    except Exception as e: print(f"Erro config FileHandler backtest: {e}")
    ch = logging.StreamHandler(sys.stdout); ch.setFormatter(formatter); bt_logger.addHandler(ch); bt_logger.propagate = False
    bt_logger.info("--- Logging do Backtester configurado ---"); return bt_logger

logger = setup_backtest_logging(level=logging.INFO)

# --- Parâmetros do Backtest ---
SYMBOL = "BTCUSDT"
INTERVAL = "1h"
# Define o período do backtest com datas específicas ou relativas
# IMPORTANTE: Certifique-se que populate_history.py cobriu este range!
START_DATE_STR = "1 Nov, 2024" # Exemplo: Início fixo
END_DATE_STR = "1 Apr, 2025"   # Exemplo: Fim fixo (None usaria até o fim dos dados no Redis)

# Parâmetros da Estratégia (SMA Crossover) a Otimizar
SMA_FAST_PERIODS = [10, 20, 30]
SMA_SLOW_PERIODS = [30, 50, 60, 100]
param_combinations = [(fast, slow) for fast, slow in itertools.product(SMA_FAST_PERIODS, SMA_SLOW_PERIODS) if slow > fast]

# Parâmetros da Simulação
INITIAL_CASH = 1000.0
COMMISSION_RATE = 0.001

# --- Função de Simulação (mantém igual) ---
def simulate_strategy(data_with_signals: pd.DataFrame, initial_cash: float, commission_rate: float) -> tuple[float, float, list, pd.DataFrame]:
    # ... (código da função simulate_strategy como antes) ...
    logger.debug("Iniciando simulação..."); cash = initial_cash; holding = 0.0; in_position = False; trades_log = []; portfolio_values = []
    for index, row in data_with_signals.iterrows():
        signal = row['Signal']; close_price = row['Close']
        current_portfolio_value = cash if not in_position else holding * close_price
        portfolio_values.append({'Timestamp': index, 'Value': current_portfolio_value})
        if signal == 1 and not in_position:
            amount_to_buy_gross = cash / close_price; commission = amount_to_buy_gross * commission_rate
            amount_to_buy_net = amount_to_buy_gross - commission; holding = amount_to_buy_net; cash = 0.0; in_position = True
            trade_info = {'Timestamp': index, 'Type': 'BUY', 'Price': close_price,'Amount_BTC': holding, 'Cost_USDT': initial_cash if not trades_log else trades_log[-1]['Cash_After'],'Commission_BTC': commission, 'Cash_After': cash, 'Portfolio_Value_After': holding * close_price}
            trades_log.append(trade_info); logger.debug(f"SIM: BUY @ {close_price:.2f} | Hold: {holding:.8f} | Cash: {cash:.2f} | Time: {index}")
        elif signal == -1 and in_position:
            cash_received_gross = holding * close_price; commission = cash_received_gross * commission_rate
            cash_received_net = cash_received_gross - commission
            trade_info = {'Timestamp': index, 'Type': 'SELL', 'Price': close_price, 'Amount_BTC': holding, 'Received_USDT': cash_received_net, 'Commission_USDT': commission, 'Cash_After': cash_received_net, 'Portfolio_Value_After': cash_received_net}
            trades_log.append(trade_info); cash = cash_received_net; holding = 0.0; in_position = False
            logger.debug(f"SIM: SELL @ {close_price:.2f} | Hold: {holding:.8f} | Cash: {cash:.2f} | Time: {index}")
    portfolio_df = pd.DataFrame(portfolio_values).set_index(pd.DatetimeIndex(pd.to_datetime([item['Timestamp'] for item in portfolio_values]))); final_portfolio_value = portfolio_df['Value'].iloc[-1]
    total_pnl = final_portfolio_value - initial_cash; total_return_pct = (total_pnl / initial_cash) * 100; num_trades = len(trades_log)
    logger.debug("Simulação concluída."); return final_portfolio_value, total_pnl, total_return_pct, num_trades, trades_log, portfolio_df


# --- Função Principal do Backtest com Otimização (Lendo do Redis) ---

def run_backtest_optimization_redis():
    logger.info("==== INICIANDO OTIMIZAÇÃO BACKTEST (Leitura Redis) ====")
    logger.info(f"Par: {SYMBOL}, Intervalo: {INTERVAL}")
    logger.info(f"Período: Desde '{START_DATE_STR}' até '{END_DATE_STR if END_DATE_STR else 'Fim dos dados Redis'}'")
    logger.info(f"Capital Inicial: {INITIAL_CASH:.2f} USDT, Comissão: {COMMISSION_RATE*100:.2f}%")
    logger.info(f"Testando Combinações SMA Fast: {SMA_FAST_PERIODS}, SMA Slow: {SMA_SLOW_PERIODS}")

    # 1. Inicializar Cliente Redis
    try:
        redis_handler = RedisHandler(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
    except Exception as e:
        logger.critical("Falha ao inicializar RedisHandler.", exc_info=True)
        return

    # 2. Calcular Timestamps de Início/Fim para Query Redis
    try:
        # Converte start_date para timestamp ms (precisa de UTC se a string tiver)
        start_dt = pd.to_datetime(START_DATE_STR, utc=True) # Assume UTC
        start_ts_ms = int(start_dt.timestamp() * 1000)
        logger.info(f"Data início convertida: {start_dt} -> {start_ts_ms} ms")

        end_ts_ms = int(time.time() * 1000) # Padrão é agora
        if END_DATE_STR:
            end_dt = pd.to_datetime(END_DATE_STR, utc=True)
            end_ts_ms = int(end_dt.timestamp() * 1000)
            logger.info(f"Data fim convertida: {end_dt} -> {end_ts_ms} ms")
        else:
            logger.info("Data fim não especificada, buscando até o fim dos dados no Redis.")
            # Para ZRANGEBYSCORE, podemos usar +inf, mas para garantir pegamos até 'agora'
            # A função get_hist_klines_range pode precisar de ajuste se end_ts for muito no futuro
            # Vamos passar o end_ts_ms calculado (agora)

    except Exception as e:
        logger.critical(f"Erro ao converter datas de início/fim. Use formatos como '1 Jan, 2024' ou 'YYYY-MM-DD'.", exc_info=True)
        return

    # 3. Buscar Dados Históricos do Redis
    logger.info(f"Buscando dados históricos do Redis (Range: {start_ts_ms} a {end_ts_ms})...")
    base_data = redis_handler.get_hist_klines_range(
        symbol=SYMBOL, interval=INTERVAL, start_ts_ms=start_ts_ms, end_ts_ms=end_ts_ms
    )
    if base_data is None or base_data.empty:
        logger.critical(f"Não foi possível obter dados históricos do Redis para o período {START_DATE_STR} - {END_DATE_STR}. Verifique se populate_history cobriu este range. Encerrando.")
        return
    logger.info(f"Total de {len(base_data)} velas históricas obtidas do Redis ({base_data.index.min()} a {base_data.index.max()}).")


    # Lista para armazenar os resultados de cada combinação
    results = []

    # --- Loop de Otimização ---
    for sma_fast, sma_slow in param_combinations:
        logger.info(f"\n--- Testando Parâmetros: SMA Fast={sma_fast}, SMA Slow={sma_slow} ---")
        data = base_data.copy() # Trabalha com cópia dos dados base

        # 3a. Calcular Indicadores
        logger.info(f"Calculando SMA {sma_fast} e SMA {sma_slow}...")
        sma_f_col = f'SMA_{sma_fast}'
        sma_s_col = f'SMA_{sma_slow}'
        data[sma_f_col] = data['Close'].rolling(window=sma_fast).mean()
        data[sma_s_col] = data['Close'].rolling(window=sma_slow).mean()
        data.dropna(inplace=True)
        if data.empty: logger.warning(f"Sem dados após dropna para SMAs {sma_fast}/{sma_slow}. Pulando."); continue
        logger.info("Indicadores calculados.")

        # 4a. Gerar Sinais
        logger.info("Gerando sinais de Crossover...")
        data['Position'] = np.where(data[sma_f_col] > data[sma_s_col], 1, -1)
        data['Signal'] = np.where(data['Position'] > data['Position'].shift(1), 1, np.where(data['Position'] < data['Position'].shift(1), -1, 0))
        data = data.iloc[1:]
        if data.empty: logger.warning(f"Sem dados após shift(1) para SMAs {sma_fast}/{sma_slow}. Pulando."); continue
        buy_signals_count = (data['Signal'] == 1).sum(); sell_signals_count = (data['Signal'] == -1).sum()
        logger.info(f"Sinais gerados: {buy_signals_count} BUYs, {sell_signals_count} SELLs.")

        # 5a. Executar Simulação
        final_value, pnl, return_pct, num_trades, trades, portfolio = simulate_strategy(
            data_with_signals=data, initial_cash=INITIAL_CASH, commission_rate=COMMISSION_RATE
        )

        results.append({'SMA_Fast': sma_fast, 'SMA_Slow': sma_slow, 'Final_Value': final_value, 'Pnl': pnl, 'Return_Pct': return_pct, 'Num_Trades': num_trades})

        # --- Geração de Relatórios e Gráficos (Opcional por combinação) ---
        report_suffix = f"SMA_{sma_fast}_{sma_slow}"
        # Relatório QuantStats (Comentado)
        logger.warning(f"Geração do relatório QuantStats HTML desabilitada.")
        # Gráfico da Curva de Capital
        # ... (código para gerar e salvar gráfico PNG como antes, usando 'portfolio' e 'data') ...
        if not portfolio.empty:
             try:
                logger.info(f"Gerando gráfico da Curva de Capital para {report_suffix}...")
                # ... (código matplotlib como antes) ...
                plt.style.use('seaborn-v0_8-darkgrid'); fig, ax = plt.subplots(figsize=(14, 7))
                ax.plot(portfolio.index, portfolio['Value'], label=f'Estratégia SMA ({sma_fast}/{sma_slow})', color='blue', linewidth=1.5)
                buy_hold_start_price = data['Close'].loc[portfolio.index[0]]
                buy_hold_value = (data['Close'].loc[portfolio.index] / buy_hold_start_price) * INITIAL_CASH
                ax.plot(buy_hold_value.index, buy_hold_value, label=f'Buy & Hold {SYMBOL}', color='orange', linestyle='--', alpha=0.8)
                ax.set_title(f'Curva de Capital - {report_suffix} vs Buy & Hold', fontsize=14)
                # ... (resto do código de plotagem) ...
                ax.legend(fontsize=10); ax.grid(True, linestyle=':', linewidth=0.5); ax.set_xlabel('Data', fontsize=12); ax.set_ylabel('Valor do Portfólio (USDT)', fontsize=12)
                try: import matplotlib.ticker as mtick; fmt = '${x:,.0f}'; tick = mtick.StrMethodFormatter(fmt); ax.yaxis.set_major_formatter(tick)
                except ImportError: logger.warning("Ticker não encontrado.")
                plt.tight_layout(); chart_filename = f'equity_curve_{report_suffix}.png'; plt.savefig(chart_filename); logger.info(f"Gráfico salvo em: {chart_filename}"); plt.close(fig)
             except Exception as e: logger.error(f"Falha ao gerar gráfico para {report_suffix}.", exc_info=True)
        else: logger.warning(f"DF Portfólio vazio para {report_suffix}. Gráfico não gerado.")

        # Salvar Trades (Opcional por combinação)
        # ... (código para salvar trades CSV como antes) ...
        if trades:
            try:
                trades_df = pd.DataFrame(trades); trades_filename = f'backtest_trades_{report_suffix}.csv'; trades_df.to_csv(trades_filename, index=False); logger.info(f"Log de trades para {report_suffix} salvo em: {trades_filename}")
            except Exception as e: logger.error(f"Falha ao salvar log de trades para {report_suffix}.", exc_info=True)

        logger.info(f"--- Concluído Teste Parâmetros: SMA Fast={sma_fast}, SMA Slow={sma_slow} ---")
        logger.info(f"Resultado: P&L={pnl:.2f} USDT, Retorno={return_pct:.2f}%")


    # --- Análise Final da Otimização ---
    logger.info("\n==== RESULTADOS DA OTIMIZAÇÃO DE PARÂMETROS ====")
    # ... (código para exibir tabela de resultados e melhor combinação, como antes) ...
    if results:
        results_df = pd.DataFrame(results)
        results_df.sort_values(by='Return_Pct', ascending=False, inplace=True)
        logger.info(f"Resultados por combinação:\n{results_df.to_string()}")
        best_result = results_df.iloc[0]
        logger.info("\n--- Melhor Combinação Encontrada ---"); logger.info(f"SMA Fast: {best_result['SMA_Fast']:.0f}"); logger.info(f"SMA Slow: {best_result['SMA_Slow']:.0f}"); logger.info(f"Valor Final: {best_result['Final_Value']:.2f} USDT")
        logger.info(f"P&L Total: {best_result['Pnl']:.2f} USDT"); logger.info(f"Retorno Total: {best_result['Return_Pct']:.2f}%"); logger.info(f"Número de Trades: {best_result['Num_Trades']:.0f}")
    else: logger.warning("Nenhuma combinação produziu resultados.")

    logger.info("==== OTIMIZAÇÃO CONCLUÍDA ====")


# --- Execução ---
if __name__ == "__main__":
    # Chama a função de otimização que agora lê do Redis
    run_backtest_optimization_redis()