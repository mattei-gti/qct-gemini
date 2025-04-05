# quantis_crypto_trader_gemini/redis_client.py

import redis
import pandas as pd
import json # Usaremos JSON para serializar/desserializar DataFrames
from io import StringIO

class RedisHandler:
    def __init__(self, host: str, port: int, db: int):
        """
        Inicializa o cliente Redis.

        Args:
            host (str): Endereço do servidor Redis.
            port (int): Porta do servidor Redis.
            db (int): Número do banco de dados Redis a usar.
        """
        try:
            self.client = redis.Redis(host=host, port=port, db=db, decode_responses=False) # decode_responses=False para trabalhar com bytes (melhor para serialização)
            self.client.ping()
            print(f"Conexão com Redis bem-sucedida (Host: {host}, Porta: {port}, DB: {db}).")
        except redis.exceptions.ConnectionError as e:
            print(f"Erro ao conectar com Redis: {e}")
            raise e
        except Exception as e:
            print(f"Erro inesperado na inicialização do RedisHandler: {e}")
            raise e

    def _generate_kline_key(self, symbol: str, interval: str) -> str:
        """Gera uma chave padronizada para armazenar klines."""
        return f"klines:{symbol}:{interval}"

    def cache_dataframe(self, key: str, df: pd.DataFrame, ttl_seconds: int = 3600):
        """
        Armazena um DataFrame Pandas no Redis com um tempo de expiração (TTL).
        Serializa o DataFrame para JSON.

        Args:
            key (str): A chave sob a qual armazenar os dados.
            df (pd.DataFrame): O DataFrame a ser armazenado.
            ttl_seconds (int): Tempo em segundos para o cache expirar (default: 1 hora).
        """
        if df is None or df.empty:
            print(f"Aviso: Tentativa de cache de DataFrame vazio ou None para a chave '{key}'.")
            return

        try:
            # Serializa o DataFrame para JSON (orient='split' preserva melhor os tipos e o índice)
            # Convertemos timestamps para string ISO para compatibilidade JSON
            df_copy = df.copy()
            if 'Open time' in df_copy.columns and pd.api.types.is_datetime64_any_dtype(df_copy['Open time']):
                 df_copy['Open time'] = df_copy['Open time'].astype(str)
            if 'Close time' in df_copy.columns and pd.api.types.is_datetime64_any_dtype(df_copy['Close time']):
                 df_copy['Close time'] = df_copy['Close time'].astype(str)

            json_data = df_copy.to_json(orient='split', date_format='iso')
            # Armazena no Redis com expiração
            self.client.setex(key, ttl_seconds, json_data)
            print(f"DataFrame armazenado no cache Redis sob a chave '{key}' com TTL de {ttl_seconds}s.")
        except Exception as e:
            print(f"Erro ao serializar ou armazenar DataFrame no Redis para a chave '{key}': {e}")

    def get_dataframe(self, key: str) -> pd.DataFrame | None:
        """
        Recupera um DataFrame do Redis.
        Desserializa de JSON.

        Args:
            key (str): A chave dos dados a serem recuperados.

        Returns:
            pd.DataFrame | None: O DataFrame recuperado ou None se não encontrado ou erro.
        """
        try:
            json_data = self.client.get(key)
            if json_data:
                # Decodifica os bytes para uma string UTF-8 ANTES de passar para read_json
                json_string = json_data.decode('utf-8')
                
                # Embrulha a string em StringIO
                json_io = StringIO(json_string)
                df = pd.read_json(json_io, orient='split')

                # Tenta reconverter colunas de tempo para datetime se existirem
                # (pode precisar de mais tratamento dependendo do formato exato)
                if 'Open time' in df.columns:
                    df['Open time'] = pd.to_datetime(df['Open time'], errors='coerce')
                if 'Close time' in df.columns:
                    df['Close time'] = pd.to_datetime(df['Close time'], errors='coerce')

                print(f"DataFrame recuperado do cache Redis da chave '{key}'.")
                return df
            else:
                # Chave não encontrada no cache
                print(f"Chave '{key}' não encontrada no cache Redis.")
                return None
        except json.JSONDecodeError as e:
             print(f"Erro ao decodificar JSON do Redis para a chave '{key}': {e}")
             return None
        except UnicodeDecodeError as e:
            print(f"Erro ao decodificar bytes (UTF-8) do Redis para a chave '{key}': {e}")
            return None
        except Exception as e:
            print(f"Erro inesperado ao recuperar ou desserializar DataFrame do Redis para a chave '{key}': {e}")
            return None
        

    def _generate_state_key(self, context: str) -> str:
        """Gera uma chave padronizada para armazenar estados."""
        # Ex: 'state:position_open:BTCUSDT'
        return f"state:{context}"

    def set_state(self, context: str, value: str, ttl_seconds: int | None = None):
        """
        Define um valor de estado no Redis.

        Args:
            context (str): Um identificador para o estado (ex: 'position_open:BTCUSDT').
            value (str): O valor a ser armazenado (ex: 'True', 'False', 'BTC', 'USDT').
            ttl_seconds (int | None): TTL opcional em segundos. Se None, persiste indefinidamente.
        """
        key = self._generate_state_key(context)
        try:
            if ttl_seconds:
                self.client.setex(key, ttl_seconds, value)
            else:
                self.client.set(key, value)
            print(f"Estado '{key}' definido como '{value}' no Redis.")
        except Exception as e:
            print(f"Erro ao definir estado '{key}' no Redis: {e}")

    def get_state(self, context: str) -> str | None:
        """
        Obtém um valor de estado do Redis.

        Args:
            context (str): O identificador do estado.

        Returns:
            str | None: O valor do estado ou None se não encontrado ou erro.
        """
        key = self._generate_state_key(context)
        try:
            value_bytes = self.client.get(key)
            if value_bytes:
                # Decodifica os bytes para string (assumindo que estados são strings simples)
                value = value_bytes.decode('utf-8')
                # print(f"Estado '{key}' lido do Redis: '{value}'.") # Log opcional
                return value
            else:
                # print(f"Estado '{key}' não encontrado no Redis.") # Log opcional
                return None
        except Exception as e:
            print(f"Erro ao obter estado '{key}' do Redis: {e}")
            return None