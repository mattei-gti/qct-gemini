# quantis_crypto_trader_gemini/binance_client.py

from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
import pandas as pd
import time
import logging

logger = logging.getLogger(__name__)

class BinanceHandler:
    def __init__(self, api_key: str, api_secret: str):
        """Inicializa o cliente da Binance."""
        # ... (init como antes, sem ';' e com try/except corretos) ...
        if not api_key or not api_secret: logger.error("API Key/Secret Binance não fornecidas."); raise ValueError("API Key/Secret Binance não podem ser vazias.")
        self.api_key = api_key; self.api_secret = api_secret; self.client: Client | None = None
        try:
            logger.info("Tentando conectar à API da Binance..."); self.client = Client(self.api_key, self.api_secret); self.client.ping()
            logger.info("Conexão API Binance estabelecida.")
        except (BinanceAPIException, BinanceRequestException) as e: logger.critical(f"Erro API/Request conectar Binance: Status {e.status_code}, Msg: {e.message}", exc_info=True); raise ConnectionError(f"Falha conectar/autenticar Binance: {e}") from e
        except Exception as e: logger.critical("Erro inesperado init BinanceHandler.", exc_info=True); raise e

    def get_server_time(self) -> int | None:
        # ... (função como antes) ...
        if not self.client: return None; 
        try: server_time = self.client.get_server_time(); ts = server_time['serverTime']; logger.debug(f"Tempo servidor Binance: {ts}"); return ts
        except (BinanceAPIException, BinanceRequestException) as e: logger.error("Erro API Binance get server time.", exc_info=True); return None
        except Exception as e: logger.error("Erro inesperado get_server_time.", exc_info=True); return None

    def get_klines(self, symbol: str, interval: str, limit: int = 500, start_str: str | None = None, end_str: str | None = None) -> pd.DataFrame | None:
        # ... (função como antes, corrigida para start_str) ...
        if not self.client: return None; logger.info(f"Buscando {limit} klines {symbol} ({interval})..."); 
        try:
            start_time_ms = start_str if start_str else None; end_time_ms = end_str if end_str else None # Passa strings diretamente
            klines = self.client.get_klines(symbol=symbol, interval=interval, limit=limit, startTime=start_time_ms, endTime=end_time_ms)
            if not klines: logger.warning(f"Nenhum klines retornado {symbol} ({interval}) params: start={start_str}, end={end_str}, limit={limit}."); return None
            columns = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
            df = pd.DataFrame(klines, columns=columns); numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote asset volume', 'Taker buy base asset volume', 'Taker buy quote asset volume']
            for col in numeric_columns: df[col] = pd.to_numeric(df[col])
            df['Open time'] = pd.to_datetime(df['Open time'], unit='ms'); df['Close time'] = pd.to_datetime(df['Close time'], unit='ms')
            # NÃO define índice aqui, deixa para quem chama decidir
            df['Number of trades'] = df['Number of trades'].astype(int); logger.info(f"{len(df)} Klines {symbol} carregados."); return df
        except (BinanceAPIException, BinanceRequestException) as e: logger.error(f"Erro API Binance klines {symbol}: Status {e.status_code}, Msg: {e.message}", exc_info=True); return None
        except Exception as e: logger.error(f"Erro inesperado buscar klines {symbol}.", exc_info=True); return None

    def get_historical_klines(self, symbol: str, interval: str, start_str: str, end_str: str | None = None) -> pd.DataFrame | None:
        # ... (função como antes, já definia índice) ...
        if not self.client: logger.error("Cliente Binance não init (hist)."); return None
        logger.info(f"Buscando klines históricos {symbol} ({interval}) de '{start_str}' até '{end_str if end_str else 'Agora'}'...")
        try:
            klines = self.client.get_historical_klines(symbol, interval, start_str, end_str)
            if not klines: logger.warning(f"Nenhum klines histórico retornado {symbol} ({interval})."); return None
            columns = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
            df = pd.DataFrame(klines, columns=columns); numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote asset volume', 'Taker buy base asset volume', 'Taker buy quote asset volume']
            for col in numeric_columns: df[col] = pd.to_numeric(df[col])
            df['Open time'] = pd.to_datetime(df['Open time'], unit='ms'); df['Close time'] = pd.to_datetime(df['Close time'], unit='ms'); df.set_index('Open time', inplace=True)
            df['Number of trades'] = df['Number of trades'].astype(int); df.drop('Ignore', axis=1, inplace=True, errors='ignore'); logger.info(f"{len(df)} Klines históricos {symbol} carregados ({df.index.min()} a {df.index.max()})."); return df
        except (BinanceAPIException, BinanceRequestException) as e: logger.error(f"Erro API Binance hist klines {symbol}.", exc_info=True); return None
        except Exception as e: logger.error(f"Erro inesperado buscar hist klines {symbol}.", exc_info=True); return None

    # *** FUNÇÃO get_asset_balance CORRIGIDA ***
    def get_asset_balance(self, asset: str) -> float:
        """
        Obtém o saldo livre de um ativo específico. Retorna 0.0 se não encontrado ou erro.

        Args:
            asset (str): O símbolo do ativo (ex: 'USDT', 'BTC').

        Returns:
            float: O saldo livre (disponível para trade). Retorna 0.0 em caso de erro ou não encontrado.
        """
        if not self.client:
            logger.error(f"Cliente Binance não inicializado ao buscar saldo de {asset}.")
            return 0.0 # Retorna 0.0 em caso de erro de cliente
        logger.info(f"Verificando saldo para {asset}...")
        try:
            balance_info = self.client.get_asset_balance(asset=asset)
            # Exemplo: {'asset': 'USDT', 'free': '100.00000000', 'locked': '0.00000000'}
            if balance_info and 'free' in balance_info:
                try:
                    free_balance = float(balance_info['free'])
                    logger.info(f"Saldo livre encontrado para {asset}: {free_balance}")
                    return free_balance
                except (ValueError, TypeError) as conv_err:
                     logger.error(f"Erro ao converter saldo 'free' para float para {asset}. Valor: '{balance_info['free']}'. Erro: {conv_err}")
                     return 0.0 # Retorna 0.0 se a conversão falhar
            else:
                # Pode acontecer se o ativo não existir na conta ou a resposta for inesperada
                logger.warning(f"Não foi possível obter informações de saldo 'free' para {asset}. Resposta API: {balance_info}")
                return 0.0 # Retorna 0.0 se 'free' não estiver presente
        except (BinanceAPIException, BinanceRequestException) as e:
            logger.error(f"Erro da API Binance ao obter saldo para {asset}: Status {e.status_code}, Mensagem: {e.message}")
            # Não loga exc_info aqui pois a mensagem da API já é informativa
            return 0.0 # Retorna 0.0 em caso de erro da API
        except Exception as e:
            logger.error(f"Erro inesperado ao obter saldo para {asset}.", exc_info=True)
            return 0.0 # Retorna 0.0 em caso de outros erros
    
    # *** NOVA FUNÇÃO: Obter Preço Atual do Ticker ***
    def get_ticker_price(self, symbol: str) -> float | None:
        """Obtém o preço de mercado mais recente para um símbolo."""
        if not self.client:
            logger.error(f"Cliente Binance não init. ao buscar preço de {symbol}.")
            return None
        logger.debug(f"Buscando preço ticker para {symbol}...")
        try:
            ticker_info = self.client.get_symbol_ticker(symbol=symbol)
            # Exemplo: {'symbol': 'BTCUSDT', 'price': '83000.50000000'}
            if ticker_info and 'price' in ticker_info:
                price = float(ticker_info['price'])
                logger.debug(f"Preço ticker {symbol}: {price}")
                return price
            else:
                logger.warning(f"Resposta inesperada ou sem 'price' ao buscar ticker para {symbol}. Resposta: {ticker_info}")
                return None
        except (BinanceAPIException, BinanceRequestException) as e:
            logger.error(f"Erro da API Binance ao obter ticker para {symbol}: Status {e.status_code}, Mensagem: {e.message}")
            return None
        except ValueError as e:
             logger.error(f"Erro ao converter preço do ticker para float para {symbol}. Valor: {ticker_info.get('price', 'N/A') if ticker_info else 'N/A'}", exc_info=True)
             return None
        except Exception as e:
            logger.error(f"Erro inesperado ao obter ticker para {symbol}.", exc_info=True)
            return None


    # --- Adicionar funções de ordem (place_market_order, place_limit_order, etc.) aqui ---
    # (Manter comentado por enquanto)