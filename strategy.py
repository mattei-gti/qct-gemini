# quantis_crypto_trader_gemini/strategy.py

from redis_client import RedisHandler
from binance_client import BinanceHandler
from telegram_interface import send_telegram_message # Envio texto simples
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
        self.risk_percentage = 0.95 # % do saldo quote a usar
        self.min_quote_balance_to_buy = 10.0
        self.min_base_balance_to_sell = 0.0001

        # Parâmetros do filtro técnico (ex: SMAs 1h)
        self.filter_sma_fast_period = 30
        self.filter_sma_slow_period = 60

        logger.info(f"StrategyManager inicializado para {self.symbol}.")
        logger.info(f"  - Filtro Técnico Ativo: SMA {self.filter_sma_fast_period}/{self.filter_sma_slow_period} (1h)")


    # *** FUNÇÃO DECIDE_ACTION MODIFICADA PARA LÓGICA HÍBRIDA ***
    def decide_action(self,
                      signal: str | None,
                      sma_fast_1h: float | None = None, # Recebe SMA rápida de 1h
                      sma_slow_1h: float | None = None): # Recebe SMA lenta de 1h
        """
        Decide qual ação tomar com base no sinal da IA e filtro técnico (SMA 1h).
        Simula ordens.
        """
        logger.info(f"--- Iniciando decisão de estratégia HÍBRIDA para {self.symbol} ---")
        logger.info(f"Sinal AI recebido: {signal}")

        # Verifica se temos os dados do filtro SMA 1h
        filter_active = sma_fast_1h is not None and sma_slow_1h is not None
        if filter_active:
            logger.info(f"Valores para Filtro SMA 1h: Fast({self.filter_sma_fast_period})={sma_fast_1h}, Slow({self.filter_sma_slow_period})={sma_slow_1h}")
        else:
            logger.warning("Valores SMA 1h não disponíveis. Filtro técnico será ignorado!")
            # O que fazer se o filtro não puder ser aplicado?
            # Opção 1: Ignorar o sinal da IA (mais seguro)
            # Opção 2: Prosseguir apenas com o sinal da IA (menos seguro)
            # Vamos escolher a Opção 1 por segurança:
            signal = None # Trata como se não houvesse sinal se o filtro falhou
            logger.warning("Sinal da IA ignorado devido à falta de dados para o filtro técnico.")

        # Obtém o estado atual da posição
        current_asset_held = self.redis_handler.get_state(self.position_state_key)
        if current_asset_held is None: # Define estado inicial se não existir
            logger.info(f"Nenhum estado de posição encontrado. Assumindo {self.quote_asset}.")
            current_asset_held = self.quote_asset
            self.redis_handler.set_state(self.position_state_key, self.quote_asset)
            logger.info(f"Estado inicial ({self.quote_asset}) salvo no Redis.")
        logger.info(f"Estado atual da posição: Possui {current_asset_held}")


        # --- Lógica de Decisão Híbrida ---
        final_decision = "HOLD" # Decisão padrão é não fazer nada

        if signal == "BUY" and current_asset_held == self.quote_asset:
            logger.info("Sinal AI é BUY e não estamos posicionados. Verificando filtro SMA 1h...")
            if filter_active and sma_fast_1h > sma_slow_1h:
                logger.info(f"Filtro SMA 1h CONFIRMOU o BUY (SMA{self.filter_sma_fast_period}={sma_fast_1h} > SMA{self.filter_sma_slow_period}={sma_slow_1h}).")
                final_decision = "BUY" # Decisão final é Comprar
            elif filter_active:
                logger.info(f"Filtro SMA 1h REJEITOU o BUY (SMA{self.filter_sma_fast_period}={sma_fast_1h} <= SMA{self.filter_sma_slow_period}={sma_slow_1h}).")
                # Mensagem Telegram opcional indicando sinal filtrado
                # send_telegram_message(f"Filtro ({self.symbol}): Sinal AI 'BUY' ignorado (SMA 1h não confirma).", disable_notification=True)
            # Se filter_active for False, final_decision continua HOLD (conforme decidido acima)

        elif signal == "SELL" and current_asset_held == self.base_asset:
            logger.info(f"Sinal AI é SELL e estamos posicionados em {self.base_asset}. Verificando filtro SMA 1h...")
            if filter_active and sma_fast_1h < sma_slow_1h:
                logger.info(f"Filtro SMA 1h CONFIRMOU o SELL (SMA{self.filter_sma_fast_period}={sma_fast_1h} < SMA{self.filter_sma_slow_period}={sma_slow_1h}).")
                final_decision = "SELL" # Decisão final é Vender
            elif filter_active:
                logger.info(f"Filtro SMA 1h REJEITOU o SELL (SMA{self.filter_sma_fast_period}={sma_fast_1h} >= SMA{self.filter_sma_slow_period}={sma_slow_1h}).")
                # Mensagem Telegram opcional
                # send_telegram_message(f"Filtro ({self.symbol}): Sinal AI 'SELL' ignorado (SMA 1h não confirma).", disable_notification=True)
            # Se filter_active for False, final_decision continua HOLD

        elif signal == "HOLD":
            logger.info("Sinal AI é HOLD. Nenhuma ação será considerada.")
            final_decision = "HOLD"

        else: # Casos incoerentes (ex: Sinal BUY mas já tem BTC)
             logger.info(f"Nenhuma ação necessária (Sinal AI: {signal}, Posição: {current_asset_held}).")
             final_decision = "HOLD"

        # --- Execução da Ação (Simulada) com base na Decisão Final ---
        logger.info(f"Decisão Final da Estratégia Híbrida: {final_decision}")
        try:
            if final_decision == "BUY":
                logger.info(f"Ação: Executando COMPRA simulada de {self.base_asset}...")
                quote_balance = self.binance_handler.get_asset_balance(self.quote_asset)
                if quote_balance is not None and quote_balance >= self.min_quote_balance_to_buy:
                    order_size_quote = quote_balance * self.risk_percentage
                    logger.info(f"SIMULANDO ORDEM COMPRA mercado {self.symbol} (aprox {order_size_quote:.2f} {self.quote_asset}).")
                    # Atualiza estado e notifica
                    self.redis_handler.set_state(self.position_state_key, self.base_asset)
                    message = f"✅ Ação Simulada ({self.symbol}):\nCOMPRA a mercado (AI+SMA) (usando {order_size_quote:.2f} {self.quote_asset}).\nPosição: {self.base_asset}"
                    send_telegram_message(message)
                else:
                    logger.warning(f"Saldo {self.quote_asset} ({quote_balance}) insuficiente (min: {self.min_quote_balance_to_buy}). Compra cancelada.")
                    send_telegram_message(f"⚠️ Alerta ({self.symbol}): Sinal COMPRA confirmado, mas saldo {self.quote_asset} baixo ({quote_balance}).", disable_notification=True)

            elif final_decision == "SELL":
                logger.info(f"Ação: Executando VENDA simulada de {self.base_asset}...")
                base_balance = self.binance_handler.get_asset_balance(self.base_asset)
                if base_balance is not None and base_balance >= self.min_base_balance_to_sell:
                    order_size_base = base_balance
                    logger.info(f"SIMULANDO ORDEM VENDA mercado {self.symbol} de {order_size_base:.8f} {self.base_asset}.")
                    # Atualiza estado e notifica
                    self.redis_handler.set_state(self.position_state_key, self.quote_asset)
                    message = f"💰 Ação Simulada ({self.symbol}):\nVENDA a mercado (AI+SMA) ({order_size_base:.8f} {self.base_asset}).\nPosição: {self.quote_asset}"
                    send_telegram_message(message)
                else:
                    logger.warning(f"Saldo {self.base_asset} ({base_balance}) insuficiente (min: {self.min_base_balance_to_sell}). Venda cancelada.")
                    send_telegram_message(f"⚠️ Alerta ({self.symbol}): Sinal VENDA confirmado, mas saldo {self.base_asset} baixo ({base_balance}).", disable_notification=True)

            elif final_decision == "HOLD":
                logger.info("Ação: Manter posição atual.")

        except Exception as e:
            logger.error("Erro inesperado durante execução da ação da estratégia.", exc_info=True)
            send_telegram_message(f"ERRO ESTRATEGIA ({self.symbol}): Falha ao executar ação {final_decision}.\nErro: {str(e)[:100]}", disable_notification=False)

        logger.info(f"--- Decisão de estratégia HÍBRIDA concluída para {self.symbol} ---")