# quantis_crypto_trader_gemini/strategy.py

from redis_client import RedisHandler
from binance_client import BinanceHandler
# Removido 'escape_markdown_v2' da importação abaixo
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
        self.risk_percentage = 0.95 # Ex: usar 95% do saldo quote para comprar
        self.min_quote_balance_to_buy = 10.0 # Mínimo USDT para tentar comprar
        self.min_base_balance_to_sell = 0.0001 # Mínimo BTC para tentar vender

        logger.info(f"StrategyManager inicializado para {self.symbol}.")
        logger.info(f"  - Base Asset: {self.base_asset}")
        logger.info(f"  - Quote Asset: {self.quote_asset}")
        logger.info(f"  - Risco por Trade (Quote %): {self.risk_percentage * 100}%")


    def decide_action(self, signal: str | None):
        """Decide qual ação tomar com base no sinal e no estado atual (simulado)."""
        logger.info(f"--- Iniciando decisão de estratégia para {self.symbol} ---")
        if not signal:
            logger.warning("Nenhum sinal de trade recebido. Nenhuma ação será tomada.")
            return

        current_asset_held = self.redis_handler.get_state(self.position_state_key)

        if current_asset_held is None:
            logger.info(f"Nenhum estado de posição encontrado. Assumindo {self.quote_asset} como inicial.")
            current_asset_held = self.quote_asset
            self.redis_handler.set_state(self.position_state_key, self.quote_asset)
            logger.info(f"Estado inicial ({self.quote_asset}) salvo no Redis para {self.position_state_key}.")

        logger.info(f"Estado atual da posição: Possui {current_asset_held}")
        logger.info(f"Sinal recebido: {signal}")

        try:
            if signal == "BUY" and current_asset_held == self.quote_asset:
                logger.info(f"Ação: Avaliando COMPRA de {self.base_asset}...")
                quote_balance = self.binance_handler.get_asset_balance(self.quote_asset)

                if quote_balance is not None and quote_balance >= self.min_quote_balance_to_buy:
                    order_size_quote = quote_balance * self.risk_percentage
                    logger.info(f"SIMULANDO ORDEM DE COMPRA a mercado para {self.symbol} usando aprox {order_size_quote:.2f} {self.quote_asset}.")
                    # SIMULAÇÃO: Atualiza estado e notifica
                    self.redis_handler.set_state(self.position_state_key, self.base_asset)
                    # Mensagem Telegram sem escape
                    message = f"✅ Ação Simulada ({self.symbol}):\nCOMPRA a mercado executada (usando {order_size_quote:.2f} {self.quote_asset}).\nPosição atual: {self.base_asset}"
                    send_telegram_message(message)
                else:
                    logger.warning(f"Saldo de {self.quote_asset} ({quote_balance}) insuficiente para comprar (mínimo: {self.min_quote_balance_to_buy}).")
                    # Mensagem Telegram sem escape
                    send_telegram_message(f"⚠️ Alerta ({self.symbol}): Sinal de COMPRA, mas saldo {self.quote_asset} baixo ({quote_balance}).", disable_notification=True)

            elif signal == "SELL" and current_asset_held == self.base_asset:
                logger.info(f"Ação: Avaliando VENDA de {self.base_asset}...")
                base_balance = self.binance_handler.get_asset_balance(self.base_asset)

                if base_balance is not None and base_balance >= self.min_base_balance_to_sell:
                    order_size_base = base_balance
                    logger.info(f"SIMULANDO ORDEM DE VENDA a mercado para {self.symbol} de {order_size_base} {self.base_asset}.")
                    # SIMULAÇÃO: Atualiza estado e notifica
                    self.redis_handler.set_state(self.position_state_key, self.quote_asset)
                    # Mensagem Telegram sem escape
                    message = f"💰 Ação Simulada ({self.symbol}):\nVENDA a mercado executada ({order_size_base:.8f} {self.base_asset}).\nPosição atual: {self.quote_asset}"
                    send_telegram_message(message)
                else:
                    logger.warning(f"Saldo de {self.base_asset} ({base_balance}) insuficiente para vender (mínimo: {self.min_base_balance_to_sell}).")
                    # Mensagem Telegram sem escape
                    send_telegram_message(f"⚠️ Alerta ({self.symbol}): Sinal de VENDA, mas saldo {self.base_asset} baixo ({base_balance}).", disable_notification=True)

            elif signal == "HOLD":
                logger.info(f"Ação: Manter posição atual ({current_asset_held}).")

            else:
                logger.info(f"Nenhuma ação necessária (Sinal: {signal}, Posição atual: {current_asset_held}).")

        except Exception as e:
            logger.error("Erro inesperado durante a execução da estratégia.", exc_info=True)
            # Mensagem Telegram sem escape
            send_telegram_message(f"ERRO ESTRATEGIA ({self.symbol}): Falha ao decidir acao. Verifique os logs.\nErro: {str(e)[:100]}", disable_notification=False) # Envia parte do erro

        logger.info(f"--- Decisão de estratégia concluída para {self.symbol} ---")