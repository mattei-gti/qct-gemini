# quantis_crypto_trader_gemini/binance_client.py

from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
import pandas as pd
import time # Para timestamp

class BinanceHandler:
    def __init__(self, api_key: str, api_secret: str):
        """
        Inicializa o cliente da Binance.

        Args:
            api_key (str): Sua chave de API da Binance.
            api_secret (str): Seu segredo de API da Binance.
        """
        if not api_key or not api_secret:
            raise ValueError("API Key e Secret Key da Binance não podem ser vazias.")

        try:
            self.client = Client(api_key, api_secret)
            # Testa a conexão fazendo uma chamada simples
            self.client.ping()
            print("Conexão com a API da Binance bem-sucedida.")
        except (BinanceAPIException, BinanceRequestException) as e:
            print(f"Erro ao conectar com a API da Binance: {e}")
            # Você pode querer lançar o erro novamente ou tratar de forma diferente
            raise e
        except Exception as e:
            print(f"Um erro inesperado ocorreu na inicialização do BinanceHandler: {e}")
            raise e

    def get_server_time(self) -> int:
        """Retorna o timestamp atual do servidor da Binance."""
        try:
            server_time = self.client.get_server_time()
            return server_time['serverTime']
        except (BinanceAPIException, BinanceRequestException) as e:
            print(f"Erro ao obter o tempo do servidor da Binance: {e}")
            return int(time.time() * 1000) # Retorna tempo local como fallback? Ou None?
        except Exception as e:
            print(f"Erro inesperado em get_server_time: {e}")
            return int(time.time() * 1000)

    def get_klines(self, symbol: str, interval: str, limit: int = 500, start_str: str | None = None, end_str: str | None = None) -> pd.DataFrame | None:
        """
        Busca dados de velas (candlesticks) para um símbolo e intervalo.

        Args:
            symbol (str): O par de moedas (ex: 'BTCUSDT').
            interval (str): O intervalo das velas (ex: Client.KLINE_INTERVAL_1HOUR, '1h', '4h', '1d').
            limit (int): Número máximo de velas a retornar (máx. 1000).
            start_str (str | None): Timestamp de início opcional (formato string ou ms).
            end_str (str | None): Timestamp de fim opcional (formato string ou ms).

        Returns:
            pd.DataFrame | None: DataFrame com os dados das velas ou None em caso de erro.
                                  Colunas: ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
                                            'Close time', 'Quote asset volume', 'Number of trades',
                                            'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
        """
        print(f"Buscando {limit} klines para {symbol} no intervalo {interval}...")
        try:
            # O método get_klines espera start_str e end_str se forem fornecidos
            klines = self.client.get_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_str, # Nome do parâmetro na API
                endTime=end_str      # Nome do parâmetro na API
            )

            if not klines:
                print(f"Nenhum dado de klines retornado para {symbol} com os parâmetros fornecidos.")
                return None

            # Definir nomes das colunas conforme documentação da API da Binance
            columns = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                       'Quote asset volume', 'Number of trades', 'Taker buy base asset volume',
                       'Taker buy quote asset volume', 'Ignore']
            df = pd.DataFrame(klines, columns=columns)

            # Converter colunas numéricas para float/int apropriados
            numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote asset volume',
                               'Taker buy base asset volume', 'Taker buy quote asset volume']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col])

            # Converter timestamps para datetime (opcional, mas útil)
            df['Open time'] = pd.to_datetime(df['Open time'], unit='ms')
            df['Close time'] = pd.to_datetime(df['Close time'], unit='ms')

            # Converter 'Number of trades' para int
            df['Number of trades'] = df['Number of trades'].astype(int)

            print(f"Klines para {symbol} carregados com sucesso.")
            return df

        except (BinanceAPIException, BinanceRequestException) as e:
            print(f"Erro da API Binance ao buscar klines para {symbol}: {e}")
            return None
        except Exception as e:
            print(f"Erro inesperado ao buscar klines para {symbol}: {e}")
            return None

    def get_asset_balance(self, asset: str) -> float | None:
        """
        Obtém o saldo livre de um ativo específico na conta da Binance.

        Args:
            asset (str): O símbolo do ativo (ex: 'USDT', 'BTC').

        Returns:
            float | None: O saldo livre (disponível para trade) ou None em caso de erro.
        """
        print(f"Verificando saldo para {asset}...")
        try:
            balance_info = self.client.get_asset_balance(asset=asset)
            if balance_info and 'free' in balance_info:
                free_balance = float(balance_info['free'])
                print(f"Saldo livre de {asset}: {free_balance}")
                return free_balance
            else:
                print(f"Não foi possível obter informações de saldo para {asset}.")
                return None
        except (BinanceAPIException, BinanceRequestException) as e:
            print(f"Erro da API Binance ao obter saldo para {asset}: {e}")
            return None
        except Exception as e:
            print(f"Erro inesperado ao obter saldo para {asset}: {e}")
            return None