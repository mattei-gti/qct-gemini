# quantis_crypto_trader_gemini/binance_client.py

from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
import pandas as pd
import time
import logging # Importa logging

# Obtém um logger para este módulo
logger = logging.getLogger(__name__)

class BinanceHandler:
    def __init__(self, api_key: str, api_secret: str):
        """Inicializa o cliente da Binance."""
        if not api_key or not api_secret:
            logger.error("API Key ou Secret Key da Binance não fornecidas.")
            raise ValueError("API Key e Secret Key da Binance não podem ser vazias.")

        self.api_key = api_key
        self.api_secret = api_secret
        self.client: Client | None = None # Define o tipo esperado

        try:
            logger.info("Tentando conectar à API da Binance...")
            self.client = Client(self.api_key, self.api_secret)
            # Testa a conexão fazendo uma chamada simples
            self.client.ping()
            # server_time = self.client.get_server_time() # Chamada alternativa de teste
            logger.info("Conexão com a API da Binance estabelecida com sucesso.")
        except (BinanceAPIException, BinanceRequestException) as e:
            logger.critical(f"Erro de API/Request ao conectar com a Binance: Status {e.status_code}, Mensagem: {e.message}", exc_info=True)
            raise ConnectionError(f"Falha ao conectar/autenticar na Binance: {e}") from e
        except Exception as e:
            logger.critical(f"Um erro inesperado ocorreu na inicialização do BinanceHandler.", exc_info=True)
            raise e # Re-levanta a exceção

    def get_server_time(self) -> int | None:
        """Retorna o timestamp atual do servidor da Binance em milissegundos."""
        if not self.client: return None # Garante que o cliente foi inicializado
        try:
            server_time = self.client.get_server_time()
            ts = server_time['serverTime']
            logger.debug(f"Tempo do servidor Binance obtido: {ts}")
            return ts
        except (BinanceAPIException, BinanceRequestException) as e:
            logger.error(f"Erro da API Binance ao obter o tempo do servidor.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Erro inesperado em get_server_time.", exc_info=True)
            return None

    def get_klines(self, symbol: str, interval: str, limit: int = 500, start_str: str | None = None, end_str: str | None = None) -> pd.DataFrame | None:
        """Busca dados de velas (candlesticks) para um símbolo e intervalo."""
        if not self.client: return None # Garante que o cliente foi inicializado
        logger.info(f"Buscando {limit} klines para {symbol} no intervalo {interval}...")
        try:
            start_time_ms = None
            if start_str:
                # Tentar converter para ms se for string (ex: '2 hours ago UTC') ou assumir que já é ms
                # A biblioteca python-binance geralmente lida com formatos de string comuns.
                 logger.debug(f"Usando start_str: {start_str}")
                 start_time_ms = start_str # Passa como está, a lib trata
            end_time_ms = None
            if end_str:
                 logger.debug(f"Usando end_str: {end_str}")
                 end_time_ms = end_str # Passa como está, a lib trata

            klines = self.client.get_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_time_ms, # A API espera startTime
                endTime=end_time_ms      # A API espera endTime
            )

            if not klines:
                logger.warning(f"Nenhum dado de klines retornado para {symbol} com os parâmetros fornecidos.")
                return None

            # Nomes das colunas conforme documentação da API Binance
            columns = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                       'Quote asset volume', 'Number of trades', 'Taker buy base asset volume',
                       'Taker buy quote asset volume', 'Ignore']
            df = pd.DataFrame(klines, columns=columns)

            # Conversões de tipo (importante para cálculos futuros)
            numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote asset volume',
                               'Taker buy base asset volume', 'Taker buy quote asset volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col])

            df['Open time'] = pd.to_datetime(df['Open time'], unit='ms')
            df['Close time'] = pd.to_datetime(df['Close time'], unit='ms')
            df['Number of trades'] = df['Number of trades'].astype(int)

            logger.info(f"{len(df)} Klines para {symbol} carregados com sucesso.")
            return df

        except (BinanceAPIException, BinanceRequestException) as e:
            logger.error(f"Erro da API Binance ao buscar klines para {symbol}: Status {e.status_code}, Mensagem: {e.message}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Erro inesperado ao buscar klines para {symbol}.", exc_info=True)
            return None

    def get_asset_balance(self, asset: str) -> float | None:
        """Obtém o saldo livre (disponível) de um ativo específico."""
        if not self.client: return None # Garante que o cliente foi inicializado
        logger.info(f"Verificando saldo para {asset}...")
        try:
            balance_info = self.client.get_asset_balance(asset=asset)
            # Exemplo de resposta: {'asset': 'USDT', 'free': '100.00000000', 'locked': '0.00000000'}
            if balance_info and 'free' in balance_info:
                free_balance = float(balance_info['free'])
                logger.info(f"Saldo livre encontrado para {asset}: {free_balance}")
                return free_balance
            else:
                logger.warning(f"Resposta inesperada ou sem saldo 'free' ao buscar saldo para {asset}. Resposta: {balance_info}")
                return None # Ou talvez 0.0? Depende de como queremos tratar saldo inexistente. None é mais seguro.
        except (BinanceAPIException, BinanceRequestException) as e:
            logger.error(f"Erro da API Binance ao obter saldo para {asset}: Status {e.status_code}, Mensagem: {e.message}", exc_info=True)
            return None
        except ValueError as e:
             logger.error(f"Erro ao converter saldo 'free' para float para {asset}. Valor recebido: {balance_info.get('free', 'N/A') if balance_info else 'N/A'}", exc_info=True)
             return None
        except Exception as e:
            logger.error(f"Erro inesperado ao obter saldo para {asset}.", exc_info=True)
            return None

    # --- Adicionar funções de ordem (place_market_order, place_limit_order, etc.) aqui ---
    # Exemplo (NÃO USAR AINDA - PRECISA TESTAR E VALIDAR):
    # def place_market_order(self, symbol: str, side: str, quantity: float) -> dict | None:
    #     if not self.client: return None
    #     side_upper = side.upper()
    #     if side_upper not in ['BUY', 'SELL']:
    #         logger.error(f"Lado inválido para ordem: {side}. Deve ser BUY ou SELL.")
    #         return None
    #     logger.info(f"Tentando colocar ordem a mercado: {side_upper} {quantity} {symbol}")
    #     try:
    #         # Para ordens a mercado, a quantidade é do ATIVO BASE (ex: BTC)
    #         # Se for comprar com USDT, precisa calcular quanto BTC comprar ou usar 'quoteOrderQty'
    #         # Exemplo simples com quantidade do base asset:
    #         order = self.client.create_order(
    #             symbol=symbol,
    #             side=side_upper,
    #             type=Client.ORDER_TYPE_MARKET,
    #             quantity=quantity # Quantidade do base asset (ex: BTC)
    #             # Para gastar uma quantidade de QUOTE asset (ex: USDT):
    #             # quoteOrderQty=quote_quantity # Quantidade do quote asset (ex: USDT)
    #         )
    #         logger.info(f"Ordem a mercado enviada com sucesso: {order}")
    #         return order
    #     except (BinanceAPIException, BinanceRequestException) as e:
    #         logger.error(f"Erro da API Binance ao colocar ordem a mercado para {symbol}: Status {e.status_code}, Mensagem: {e.message}", exc_info=True)
    #         return None
    #     except Exception as e:
    #          logger.error(f"Erro inesperado ao colocar ordem a mercado para {symbol}.", exc_info=True)
    #          return None