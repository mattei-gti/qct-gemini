# quantis_crypto_trader_gemini/strategy.py

from redis_client import RedisHandler
from binance_client import BinanceHandler
from telegram_interface import send_telegram_message
import logging

logger = logging.getLogger(__name__)

class StrategyManager:
    def __init__(self, redis_handler: RedisHandler, binance_handler: BinanceHandler):
        """Inicializa o gerenciador de estratégia."""
        self.redis_handler = redis_handler
        self.binance_handler = binance_handler
        self.base_asset = "BTC"
        self.quote_asset = "USDT"
        self.symbol = f"{self.base_asset}{self.quote_asset}"
        self.position_state_key = f"position_asset:{self.symbol}"
        self.risk_percentage = 0.95
        self.min_quote_balance_to_buy = 10.0
        self.min_base_balance_to_sell = 0.0001

        # Parâmetros do filtro técnico combinado (15m)
        self.filter_rsi_buy_threshold = 45.0  # Comprar se RSI < 45
        self.filter_rsi_sell_threshold = 55.0 # Vender se RSI > 55
        self.filter_bbp_buy_threshold = 0.2  # Comprar se BBP < 0.2
        self.filter_bbp_sell_threshold = 0.8 # Vender se BBP > 0.8

        logger.info(f"StrategyManager inicializado para {self.symbol}.")
        logger.info(f"  - Filtro Híbrido Ativo: AI + SMA_15m(30/60) + RSI_15m({self.filter_rsi_buy_threshold}/{self.filter_rsi_sell_threshold}) + BBP_15m({self.filter_bbp_buy_threshold}/{self.filter_bbp_sell_threshold})")


    # *** FUNÇÃO DECIDE_ACTION MODIFICADA PARA MULTI-FILTRO 15m ***
    def decide_action(self,
                      signal: str | None,
                      sma_fast_15m: float | None = None, # Recebe SMA rápida de 15m
                      sma_slow_15m: float | None = None, # Recebe SMA lenta de 15m
                      rsi_15m: float | None = None,      # Recebe RSI de 15m
                      bbp_15m: float | None = None):     # Recebe BBP de 15m
        """
        Decide qual ação tomar com base no sinal da IA e múltiplos filtros técnicos de 15m.
        Simula ordens.
        """
        logger.info(f"--- Iniciando decisão de estratégia HÍBRIDA MULTI-FILTRO para {self.symbol} ---")
        logger.info(f"Sinal AI recebido: {signal}")

        # Verifica se temos todos os dados do filtro
        filter_data_ok = all(v is not None for v in [sma_fast_15m, sma_slow_15m, rsi_15m, bbp_15m])
        if filter_data_ok:
            logger.info(f"Valores Filtro 15m: SMA={sma_fast_15m:.2f}/{sma_slow_15m:.2f}, RSI={rsi_15m:.2f}, BBP={bbp_15m:.4f}")
        else:
            logger.warning("Valores de filtro técnico (15m) ausentes! Filtro será ignorado!")
            signal = None # Ignora sinal da IA se filtro falhou
            logger.warning("Sinal da IA ignorado devido à falta de dados para o filtro técnico.")

        # Obtém o estado atual da posição
        current_asset_held = self.redis_handler.get_state(self.position_state_key)
        if current_asset_held is None:
            logger.info(f"Nenhum estado de posição encontrado. Assumindo {self.quote_asset}.")
            current_asset_held = self.quote_asset
            self.redis_handler.set_state(self.position_state_key, self.quote_asset)
            logger.info(f"Estado inicial ({self.quote_asset}) salvo no Redis.")
        logger.info(f"Estado atual da posição: Possui {current_asset_held}")

        # --- Lógica de Decisão Híbrida Multi-Filtro ---
        final_decision = "HOLD" # Padrão

        if signal == "BUY" and current_asset_held == self.quote_asset:
            logger.info("Sinal AI é BUY. Verificando filtros técnicos 15m...")
            if filter_data_ok:
                # CONDIÇÃO DE COMPRA: SMA Bullish E RSI não sobrecomprado E BBP baixo
                sma_confirm = sma_fast_15m > sma_slow_15m
                rsi_confirm = rsi_15m < self.filter_rsi_buy_threshold
                bbp_confirm = bbp_15m < self.filter_bbp_buy_threshold
                logger.info(f"Filtro BUY: SMA OK? {sma_confirm}, RSI OK? {rsi_confirm}, BBP OK? {bbp_confirm}")

                if sma_confirm and rsi_confirm and bbp_confirm:
                    logger.info("Filtros Técnicos (SMA, RSI, BBP 15m) CONFIRMARAM o BUY.")
                    final_decision = "BUY"
                else:
                    logger.info("Filtros Técnicos (SMA, RSI, BBP 15m) REJEITARAM o BUY.")
            # Se filter_data_ok for False, final_decision continua HOLD

        elif signal == "SELL" and current_asset_held == self.base_asset:
            logger.info(f"Sinal AI é SELL. Verificando filtros técnicos 15m...")
            if filter_data_ok:
                 # CONDIÇÃO DE VENDA: SMA Bearish E RSI não sobrevendido E BBP alto
                sma_confirm = sma_fast_15m < sma_slow_15m
                rsi_confirm = rsi_15m > self.filter_rsi_sell_threshold
                bbp_confirm = bbp_15m > self.filter_bbp_sell_threshold
                logger.info(f"Filtro SELL: SMA OK? {sma_confirm}, RSI OK? {rsi_confirm}, BBP OK? {bbp_confirm}")

                if sma_confirm and rsi_confirm and bbp_confirm:
                    logger.info("Filtros Técnicos (SMA, RSI, BBP 15m) CONFIRMARAM o SELL.")
                    final_decision = "SELL"
                else:
                    logger.info("Filtros Técnicos (SMA, RSI, BBP 15m) REJEITARAM o SELL.")
            # Se filter_data_ok for False, final_decision continua HOLD

        elif signal == "HOLD":
            logger.info("Sinal AI é HOLD. Nenhuma ação será considerada.")
            final_decision = "HOLD"

        else: # Casos incoerentes
             logger.info(f"Nenhuma ação necessária (Sinal AI: {signal}, Posição: {current_asset_held}).")
             final_decision = "HOLD"

        # --- Execução da Ação (Simulada) ---
        logger.info(f"Decisão Final da Estratégia Híbrida (AI+MultiFiltro): {final_decision}")
        try:
            if final_decision == "BUY":
                logger.info(f"Ação: Executando COMPRA simulada de {self.base_asset}...")
                quote_balance = self.binance_handler.get_asset_balance(self.quote_asset)
                if quote_balance is not None and quote_balance >= self.min_quote_balance_to_buy:
                    order_size_quote = quote_balance * self.risk_percentage
                    logger.info(f"SIMULANDO ORDEM COMPRA mercado {self.symbol} (aprox {order_size_quote:.2f} {self.quote_asset}).")
                    self.redis_handler.set_state(self.position_state_key, self.base_asset)
                    message = f"✅ Ação Simulada ({self.symbol}):\nCOMPRA (AI+Filtros) (usando {order_size_quote:.2f} {self.quote_asset}).\nPosição: {self.base_asset}"
                    send_telegram_message(message)
                else:
                    logger.warning(f"Saldo {self.quote_asset} ({quote_balance}) insuficiente. Compra cancelada.")
                    send_telegram_message(f"⚠️ Alerta ({self.symbol}): Sinal COMPRA confirmado, mas saldo {self.quote_asset} baixo ({quote_balance}).", disable_notification=True)

            elif final_decision == "SELL":
                logger.info(f"Ação: Executando VENDA simulada de {self.base_asset}...")
                base_balance = self.binance_handler.get_asset_balance(self.base_asset)
                if base_balance is not None and base_balance >= self.min_base_balance_to_sell:
                    order_size_base = base_balance
                    logger.info(f"SIMULANDO ORDEM VENDA mercado {self.symbol} de {order_size_base:.8f} {self.base_asset}.")
                    self.redis_handler.set_state(self.position_state_key, self.quote_asset)
                    message = f"💰 Ação Simulada ({self.symbol}):\nVENDA (AI+Filtros) ({order_size_base:.8f} {self.base_asset}).\nPosição: {self.quote_asset}"
                    send_telegram_message(message)
                else:
                    logger.warning(f"Saldo {self.base_asset} ({base_balance}) insuficiente. Venda cancelada.")
                    send_telegram_message(f"⚠️ Alerta ({self.symbol}): Sinal VENDA confirmado, mas saldo {self.base_asset} baixo ({base_balance}).", disable_notification=True)

            elif final_decision == "HOLD":
                logger.info("Ação: Manter posição atual.")

        except Exception as e:
            logger.error("Erro inesperado durante execução da ação da estratégia.", exc_info=True)
            send_telegram_message(f"ERRO ESTRATEGIA ({self.symbol}): Falha executar ação {final_decision}.\nErro: {str(e)[:100]}", disable_notification=False)

        logger.info(f"--- Decisão de estratégia HÍBRIDA MULTI-FILTRO concluída ---")