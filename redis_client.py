# quantis_crypto_trader_gemini/redis_client.py

import redis
import pandas as pd
import json
from io import StringIO # Para correção do pd.read_json
import logging # Importa logging

# Obtém um logger para este módulo
logger = logging.getLogger(__name__)

class RedisHandler:
    def __init__(self, host: str, port: int, db: int):
        """Inicializa o cliente Redis."""
        self.host = host
        self.port = port
        self.db_num = db
        try:
            # decode_responses=False -> Retorna bytes, bom para dados serializados
            self.client = redis.Redis(host=self.host, port=self.port, db=self.db_num, decode_responses=False)
            self.client.ping()
            logger.info(f"Conexão com Redis bem-sucedida (Host: {self.host}, Porta: {self.port}, DB: {self.db_num}).")
        except redis.exceptions.ConnectionError as e:
            logger.critical(f"Erro CRÍTICO ao conectar com Redis ({self.host}:{self.port}, DB {self.db_num}). Verifique se está rodando.", exc_info=True)
            # Levantar a exceção pode ser melhor para parar a inicialização do serviço que depende do Redis
            raise ConnectionError(f"Falha ao conectar ao Redis: {e}") from e
        except Exception as e:
            logger.critical(f"Erro inesperado na inicialização do RedisHandler.", exc_info=True)
            raise e # Re-levanta a exceção

    def _generate_kline_key(self, symbol: str, interval: str) -> str:
        """Gera uma chave padronizada para armazenar klines."""
        key = f"klines:{symbol}:{interval}"
        logger.debug(f"Gerada chave Kline: {key}")
        return key

    def _generate_state_key(self, context: str) -> str:
        """Gera uma chave padronizada para armazenar estados."""
        key = f"state:{context}"
        logger.debug(f"Gerada chave State: {key}")
        return key

    def cache_dataframe(self, key: str, df: pd.DataFrame, ttl_seconds: int = 3600):
        """Armazena um DataFrame Pandas no Redis com TTL (serializado como JSON)."""
        if df is None or df.empty:
            logger.warning(f"Tentativa de cache de DataFrame vazio ou None para a chave '{key}'. Ignorando.")
            return

        try:
            # Prepara cópia e converte timestamps para string ISO
            df_copy = df.copy()
            if 'Open time' in df_copy.columns and pd.api.types.is_datetime64_any_dtype(df_copy['Open time']):
                 df_copy['Open time'] = df_copy['Open time'].astype(str)
            if 'Close time' in df_copy.columns and pd.api.types.is_datetime64_any_dtype(df_copy['Close time']):
                 df_copy['Close time'] = df_copy['Close time'].astype(str)

            json_data = df_copy.to_json(orient='split', date_format='iso')
            # Armazena com expiração usando setex (value deve ser bytes ou string compatível)
            self.client.setex(key, ttl_seconds, json_data.encode('utf-8')) # Codifica para bytes
            logger.info(f"DataFrame ({len(df)} linhas) armazenado no cache Redis sob a chave '{key}' com TTL de {ttl_seconds}s.")
        except Exception as e:
            logger.error(f"Erro ao serializar ou armazenar DataFrame no Redis para a chave '{key}'.", exc_info=True)

    def get_dataframe(self, key: str) -> pd.DataFrame | None:
        """Recupera um DataFrame do Redis (desserializa de JSON)."""
        logger.debug(f"Tentando recuperar DataFrame da chave Redis '{key}'...")
        try:
            json_data_bytes = self.client.get(key) # Obtém os dados como bytes
            if json_data_bytes:
                # Decodifica os bytes para uma string UTF-8
                json_string = json_data_bytes.decode('utf-8')
                # Usa StringIO para evitar FutureWarning do pandas
                json_io = StringIO(json_string)
                df = pd.read_json(json_io, orient='split')

                # Tenta reconverter colunas de tempo para datetime
                if 'Open time' in df.columns:
                    df['Open time'] = pd.to_datetime(df['Open time'], errors='coerce')
                if 'Close time' in df.columns:
                    df['Close time'] = pd.to_datetime(df['Close time'], errors='coerce')

                logger.info(f"DataFrame ({len(df)} linhas) recuperado do cache Redis da chave '{key}'.")
                return df
            else:
                logger.info(f"Chave '{key}' não encontrada no cache Redis.")
                return None
        except json.JSONDecodeError as e:
             logger.error(f"Erro ao decodificar JSON do Redis para a chave '{key}'. Dados podem estar corrompidos.", exc_info=True)
             return None
        except UnicodeDecodeError as e:
            logger.error(f"Erro ao decodificar bytes (UTF-8) do Redis para a chave '{key}'.", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Erro inesperado ao recuperar ou desserializar DataFrame do Redis para a chave '{key}'.", exc_info=True)
            return None

    def set_state(self, context: str, value: str, ttl_seconds: int | None = None):
        """Define um valor de estado no Redis."""
        key = self._generate_state_key(context)
        try:
            value_bytes = value.encode('utf-8') # Converte valor para bytes
            if ttl_seconds:
                self.client.setex(key, ttl_seconds, value_bytes)
                logger.info(f"Estado '{key}' definido como '{value}' no Redis com TTL de {ttl_seconds}s.")
            else:
                self.client.set(key, value_bytes)
                logger.info(f"Estado '{key}' definido como '{value}' no Redis (sem TTL).")
        except Exception as e:
            logger.error(f"Erro ao definir estado '{key}' no Redis.", exc_info=True)

    def get_state(self, context: str) -> str | None:
        """Obtém um valor de estado do Redis."""
        key = self._generate_state_key(context)
        logger.debug(f"Tentando obter estado da chave Redis '{key}'...")
        try:
            value_bytes = self.client.get(key)
            if value_bytes:
                value = value_bytes.decode('utf-8') # Decodifica bytes para string
                logger.info(f"Estado '{key}' lido do Redis: '{value}'.")
                return value
            else:
                logger.info(f"Estado '{key}' não encontrado no Redis.")
                return None
        except Exception as e:
            logger.error(f"Erro ao obter estado '{key}' do Redis.", exc_info=True)
            return None