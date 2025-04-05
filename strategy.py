# quantis_crypto_trader_gemini/strategy.py

from redis_client import RedisHandler
from binance_client import BinanceHandler
from telegram_interface import send_telegram_message, escape_markdown_v2

class StrategyManager:
    def __init__(self, redis_handler: RedisHandler, binance_handler: BinanceHandler):
        """
        Inicializa o gerenciador de estratégia.

        Args:
            redis_handler (RedisHandler): Instância do manipulador Redis para estado.
            binance_handler (BinanceHandler): Instância do manipulador Binance para saldos/ordens.
        """
        self.redis_handler = redis_handler
        self.binance_handler = binance_handler
        self.base_asset = "BTC" # Ativo que queremos comprar/vender
        self.quote_asset = "USDT" # Ativo que usamos para comprar/vender (moeda base)
        self.symbol = f"{self.base_asset}{self.quote_asset}" # Ex: BTCUSDT
        # Chave Redis para saber qual ativo (base ou quote) estamos segurando atualmente
        self.position_state_key = f"position_asset:{self.symbol}"

    def decide_action(self, signal: str | None):
        """
        Decide qual ação tomar com base no sinal e no estado atual.
        Por enquanto, apenas simula ordens.

        Args:
            signal (str | None): O sinal recebido ('BUY', 'SELL', 'HOLD') ou None.
        """
        if not signal:
            print("Nenhum sinal recebido, nenhuma ação a ser tomada.")
            # Podemos enviar uma notificação de alerta? Ou apenas logar?
            # send_telegram_message(f"⚠️ Alerta ({escape_markdown_v2(self.symbol)}): Nenhum sinal de trade recebido.", disable_notification=True)
            return

        # --- Obtém o estado atual ---
        # Verifica qual ativo (base ou quote) está registrado como sendo mantido no Redis
        current_asset_held = self.redis_handler.get_state(self.position_state_key)

        # --- Tratamento do Estado Inicial ---
        # Se nenhum estado for encontrado no Redis (primeira execução ou cache limpo)...
        if current_asset_held is None:
            print(f"Nenhum estado de posição encontrado para {self.symbol}. Assumindo {self.quote_asset} como inicial.")
            # Define o estado localmente para este ciclo
            current_asset_held = self.quote_asset
            # >>> CORREÇÃO: Salva imediatamente o estado assumido (USDT) no Redis <<<
            # Isso garante que nos próximos ciclos, o estado será lido corretamente.
            self.redis_handler.set_state(self.position_state_key, self.quote_asset)
            print(f"Estado inicial ({self.quote_asset}) salvo no Redis para {self.position_state_key}.")


        # Exibe o estado atual lido (ou recém-salvo) e o sinal
        print(f"Estado atual da posição: Possui {current_asset_held}")
        print(f"Sinal recebido: {signal}")

        # --- Lógica de Decisão ---

        # CONDIÇÃO DE COMPRA: Sinal é BUY e atualmente temos a moeda QUOTE (USDT)
        if signal == "BUY" and current_asset_held == self.quote_asset:
            print(f"Ação: Avaliando COMPRA de {self.base_asset}...")
            # Verifica o saldo da moeda QUOTE (USDT)
            quote_balance = self.binance_handler.get_asset_balance(self.quote_asset)

            # Verifica se temos saldo suficiente (ex: mais que 10 USDT)
            if quote_balance is not None and quote_balance > 10:
                # Lógica de Gerenciamento de Risco: Quanto comprar?
                # Exemplo: usar 95% do saldo disponível em USDT.
                order_size_quote = quote_balance * 0.95
                # Nota: Para uma ordem real, precisaríamos do preço atual para calcular a quantidade do BASE asset.
                # Ex: quantity_base = order_size_quote / current_market_price

                print(f"SIMULANDO ORDEM DE COMPRA a mercado para {self.symbol} usando aprox {order_size_quote:.2f} {self.quote_asset}.")
                # --- Aqui viria a chamada real para a API da Binance: ---
                # order_result = self.binance_handler.place_market_order(symbol=self.symbol, side='BUY', quantity=quantity_base) # Precisaria calcular quantity_base
                # if order_result: # Verificar se a ordem foi criada com sucesso

                # >>> SIMULAÇÃO: Atualiza o estado no Redis para indicar que agora possuímos o BASE asset (BTC) <<<
                self.redis_handler.set_state(self.position_state_key, self.base_asset)
                # Envia notificação da ação simulada para o Telegram
                message = f"✅ *Ação Simulada* ({escape_markdown_v2(self.symbol)}):\nCOMPRA a mercado executada \\(usando {order_size_quote:.2f} {escape_markdown_v2(self.quote_asset)}\\)\\.\n*Posição atual: {escape_markdown_v2(self.base_asset)}*"
                send_telegram_message(message)
            else:
                # Saldo insuficiente
                print(f"Saldo insuficiente de {self.quote_asset} ({quote_balance}) para executar a COMPRA.")
                send_telegram_message(f"⚠️ *Alerta* ({escape_markdown_v2(self.symbol)}): Sinal de COMPRA recebido, mas saldo de {escape_markdown_v2(self.quote_asset)} insuficiente \\({quote_balance}\\)\\.", disable_notification=True)

        # CONDIÇÃO DE VENDA: Sinal é SELL e atualmente temos a moeda BASE (BTC)
        elif signal == "SELL" and current_asset_held == self.base_asset:
            print(f"Ação: Avaliando VENDA de {self.base_asset}...")
            # Verifica o saldo da moeda BASE (BTC)
            base_balance = self.binance_handler.get_asset_balance(self.base_asset)

            # Verifica se temos algum saldo para vender (ex: um valor mínimo)
            if base_balance is not None and base_balance > 0.0001: # Ajuste este mínimo conforme necessário
                # Lógica de Gerenciamento de Risco: Quanto vender?
                # Exemplo: vender todo o saldo disponível do BASE asset.
                order_size_base = base_balance

                print(f"SIMULANDO ORDEM DE VENDA a mercado para {self.symbol} de {order_size_base} {self.base_asset}.")
                # --- Aqui viria a chamada real para a API da Binance: ---
                # order_result = self.binance_handler.place_market_order(symbol=self.symbol, side='SELL', quantity=order_size_base)
                # if order_result: # Verificar se a ordem foi criada com sucesso

                # >>> SIMULAÇÃO: Atualiza o estado no Redis para indicar que agora possuímos o QUOTE asset (USDT) <<<
                self.redis_handler.set_state(self.position_state_key, self.quote_asset)
                # Envia notificação da ação simulada para o Telegram
                message = f"💰 *Ação Simulada* ({escape_markdown_v2(self.symbol)}):\nVENDA a mercado executada \\({order_size_base:.6f} {escape_markdown_v2(self.base_asset)}\\)\\.\n*Posição atual: {escape_markdown_v2(self.quote_asset)}*"
                send_telegram_message(message)
            else:
                # Saldo insuficiente
                print(f"Saldo insuficiente de {self.base_asset} ({base_balance}) para executar a VENDA.")
                send_telegram_message(f"⚠️ *Alerta* ({escape_markdown_v2(self.symbol)}): Sinal de VENDA recebido, mas saldo de {escape_markdown_v2(self.base_asset)} insuficiente \\({base_balance}\\)\\.", disable_notification=True)

        # CONDIÇÃO HOLD: Sinal é HOLD
        elif signal == "HOLD":
            print("Ação: Manter posição atual.")
            # Nenhuma mudança de estado é necessária para HOLD.
            # Opcional: Enviar notificação periódica de HOLD?
            # send_telegram_message(f" HOLD - Posição em {escape_markdown_v2(current_asset_held)} mantida para {escape_markdown_v2(self.symbol)}.", disable_notification=True)

        # OUTRAS CONDIÇÕES: Sinal incoerente com a posição (Ex: BUY mas já tem BTC)
        else:
            print(f"Nenhuma ação necessária (Sinal: {signal}, Posição atual: {current_asset_held}).")