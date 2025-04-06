# quantis_crypto_trader_gemini/redis_client.py

import redis
import pandas as pd
import json
from io import StringIO
import logging
import time

logger = logging.getLogger(__name__)

class RedisHandler:
    def __init__(self, host: str, port: int, db: int):
        """Inicializa o cliente Redis."""
        self.host = host; self.port = port; self.db_num = db; self.client: redis.Redis | None = None
        try:
            self.client = redis.Redis(host=self.host, port=self.port, db=self.db_num, decode_responses=False)
            self.client.ping()
            logger.info(f"Conexão Redis OK (Host: {self.host}, Port: {self.port}, DB: {self.db_num}).")
        except redis.exceptions.ConnectionError as e:
            logger.critical("Erro CRÍTICO conectar Redis.", exc_info=True)
            raise ConnectionError(f"Falha conectar Redis: {e}") from e
        except Exception as e:
            logger.critical("Erro inesperado init RedisHandler.", exc_info=True)
            raise e

    # --- Funções de Cache Recente (DataFrame) ---
    def _generate_cache_key(self, symbol: str, interval: str) -> str:
        key = f"cache:klines:{symbol}:{interval}"; logger.debug(f"Gerada chave Cache DF: {key}"); return key

    def cache_dataframe(self, key: str, df: pd.DataFrame, ttl_seconds: int = 3600):
        if df is None or df.empty: logger.warning(f"Cache DF vazio/None para '{key}'."); return
        try:
            df_copy = df.copy();
            if 'Open time' in df_copy.columns and pd.api.types.is_datetime64_any_dtype(df_copy['Open time']): df_copy['Open time'] = df_copy['Open time'].astype(str)
            if 'Close time' in df_copy.columns and pd.api.types.is_datetime64_any_dtype(df_copy['Close time']): df_copy['Close time'] = df_copy['Close time'].astype(str)
            json_data = df_copy.to_json(orient='split', date_format='iso'); self.client.setex(key, ttl_seconds, json_data.encode('utf-8'))
            logger.info(f"DF Cache ({len(df)}L) salvo '{key}' TTL {ttl_seconds}s.")
        except Exception as e: logger.error(f"Erro cache DF Redis '{key}'.", exc_info=True)

    def get_dataframe_from_cache(self, key: str) -> pd.DataFrame | None:
        logger.debug(f"Tentando recuperar DF do cache '{key}'...")
        try:
            json_data_bytes = self.client.get(key)
            if json_data_bytes:
                json_string = json_data_bytes.decode('utf-8'); json_io = StringIO(json_string); df = pd.read_json(json_io, orient='split')
                if 'Open time' in df.columns: df['Open time'] = pd.to_datetime(df['Open time'], errors='coerce')
                if 'Close time' in df.columns: df['Close time'] = pd.to_datetime(df['Close time'], errors='coerce')
                logger.info(f"DF Cache ({len(df)}L) recuperado de '{key}'."); return df
            else: logger.info(f"Chave Cache DF '{key}' nao encontrada."); return None
        except Exception as e: logger.error(f"Erro ao recuperar/desserializar DF do cache Redis para '{key}'.", exc_info=True); return None

    # --- Funções de Estado Simples (Chave-Valor) ---
    def _generate_state_key(self, context: str) -> str: key = f"state:{context}"; logger.debug(f"Gerada chave State: {key}"); return key

    # *** CORREÇÃO AQUI na função set_state ***
    def set_state(self, context: str, value: str, ttl_seconds: int | None = None):
        """Define um valor de estado simples no Redis."""
        key = self._generate_state_key(context)
        # *** CORREÇÃO: Removido ';' e movido try para linha seguinte ***
        try:
            value_bytes = value.encode('utf-8')
            if ttl_seconds:
                self.client.setex(key, ttl_seconds, value_bytes)
                logger.info(f"Estado '{key}'='{value}' TTL {ttl_seconds}s.")
            else:
                self.client.set(key, value_bytes)
                logger.info(f"Estado '{key}'='{value}' (sem TTL).")
        except Exception as e:
            logger.error(f"Erro set estado '{key}'.", exc_info=True)
    # *** FIM DA CORREÇÃO ***

    def get_state(self, context: str) -> str | None:
        """Obtém um valor de estado simples do Redis."""
        key = self._generate_state_key(context)
        logger.debug(f"Tentando get estado '{key}'...")
        try:
            value_bytes = self.client.get(key)
            if value_bytes: value = value_bytes.decode('utf-8'); logger.info(f"Estado '{key}' lido: '{value}'."); return value
            else: logger.info(f"Estado '{key}' nao encontrado."); return None
        except Exception as e: logger.error(f"Erro get estado '{key}'.", exc_info=True); return None

    # --- Funções Histórico Klines (Sorted Set) ---
    def _generate_hist_key(self, symbol: str, interval: str) -> str: key = f"hist:klines:{symbol}:{interval}"; logger.debug(f"Gerada chave Histórico: {key}"); return key
    def _serialize_kline(self, kline_row: pd.Series) -> str:
        data = {"o": float(kline_row['Open']), "h": float(kline_row['High']), "l": float(kline_row['Low']), "c": float(kline_row['Close']), "v": float(kline_row['Volume']), "T": int(kline_row['Close time'].timestamp() * 1000)}
        return json.dumps(data, separators=(',', ':'))
    def _deserialize_kline(self, kline_json_str: str | bytes, open_time_ms: int) -> dict | None:
        """Desserializa a string JSON de uma vela e adiciona o Open time."""
        try:
            if isinstance(kline_json_str, bytes): kline_json_str = kline_json_str.decode('utf-8')
            data = json.loads(kline_json_str); data['t'] = open_time_ms; return data
        except Exception: logger.error(f"Erro desserializar kline JSON: {kline_json_str}", exc_info=True); return None
    def add_klines_to_hist(self, symbol: str, interval: str, klines_df: pd.DataFrame, chunk_size: int = 10000):
        """Adiciona velas de um DataFrame ao Sorted Set histórico em chunks."""
        if not self.client: logger.error("Cliente Redis não inicializado."); return 0
        if klines_df is None or klines_df.empty: logger.warning(f"Tentativa add klines vazios/None p/ hist {symbol}/{interval}."); return 0
        key = self._generate_hist_key(symbol, interval); total_processed = 0; total_added_updated_redis = 0; df_len = len(klines_df)
        logger.info(f"Adicionando {df_len} klines ao hist Redis '{key}' chunks ~{chunk_size}...")
        try:
            if not isinstance(klines_df.index, pd.DatetimeIndex): logger.error(f"DF para '{key}' não possui DatetimeIndex! Tipo: {type(klines_df.index)}. Abortando add."); return 0
            for i in range(0, df_len, chunk_size):
                chunk_df = klines_df.iloc[i : i + chunk_size]; chunk_start_time = time.time(); chunk_num = i // chunk_size + 1; total_chunks = (df_len + chunk_size - 1) // chunk_size
                logger.info(f"Processando chunk {chunk_num}/{total_chunks} (linhas {i+1}-{min(i+chunk_size, df_len)})...")
                items_to_add = {}; valid_rows_in_chunk = 0
                for index, row in chunk_df.iterrows(): score = int(index.timestamp() * 1000); value = self._serialize_kline(row); items_to_add[value.encode('utf-8')] = score; valid_rows_in_chunk += 1
                total_processed += valid_rows_in_chunk
                if not items_to_add: logger.warning("Nenhuma linha válida neste chunk."); continue
                try:
                    with self.client.pipeline() as pipe: pipe.zadd(key, items_to_add); results = pipe.execute()
                    chunk_added_updated = results[0] if results else 0; total_added_updated_redis += chunk_added_updated; chunk_duration = time.time() - chunk_start_time
                    logger.info(f"Chunk {chunk_num}/{total_chunks}: {valid_rows_in_chunk} processadas, {chunk_added_updated} add/update Redis em {chunk_duration:.2f}s.")
                except Exception as pipe_e: logger.error(f"Erro pipeline Redis chunk {chunk_num}.", exc_info=True); continue
            logger.info(f"Concluído para '{key}'. Processado: {total_processed}. Add/Update Redis: {total_added_updated_redis}.")
            return total_added_updated_redis
        except Exception as e: logger.error(f"Erro geral add klines hist Redis '{key}'.", exc_info=True); return total_added_updated_redis
    def get_last_hist_timestamp(self, symbol: str, interval: str) -> int | None:
        """Obtém o timestamp (score) da última vela no histórico."""
        key = self._generate_hist_key(symbol, interval)
        if not self.client: logger.error(f"Redis não init. ao buscar last ts para {key}"); return None
        logger.debug(f"Buscando last ts '{key}'...")
        try:
            result = self.client.zrevrange(key, 0, 0, withscores=True)
            if result: timestamp_ms = int(result[0][1]); logger.info(f"Last ts '{key}': {timestamp_ms} ({pd.to_datetime(timestamp_ms, unit='ms')})"); return timestamp_ms
            else: logger.info(f"Histórico '{key}' não encontrado."); return None
        except Exception as e: logger.error(f"Erro get last ts '{key}'.", exc_info=True); return None
    def get_last_n_hist_klines(self, symbol: str, interval: str, n: int) -> pd.DataFrame | None:
        """Obtém as N últimas velas do histórico e retorna como DataFrame."""
        key = self._generate_hist_key(symbol, interval)
        if not self.client: logger.error(f"Redis não init. ao buscar N klines para {key}"); return None
        logger.info(f"Buscando ultimas {n} klines hist '{key}'...")
        try:
            results = self.client.zrevrange(key, 0, n - 1, withscores=True)
            if not results: logger.warning(f"Histórico '{key}' vazio/não encontrado (get N)."); return None
            klines_data = [];
            for value_bytes, score_float in reversed(results): # Reverte para ordem cronológica
                ts_ms = int(score_float); kline_dict = self._deserialize_kline(value_bytes, ts_ms)
                if kline_dict: klines_data.append(kline_dict)
            if not klines_data: logger.error(f"Falha desserializar klines '{key}'."); return None
            df = pd.DataFrame(klines_data); df.rename(columns={'t': 'Open time', 'o': 'Open', 'h': 'High', 'l': 'Low','c': 'Close', 'v': 'Volume', 'T': 'Close time'}, inplace=True)
            df['Open time'] = pd.to_datetime(df['Open time'], unit='ms'); df['Close time'] = pd.to_datetime(df['Close time'], unit='ms'); df.set_index('Open time', inplace=True)
            numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume'];
            for col in numeric_cols:
                 if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
            logger.info(f"{len(df)}/{n} klines recentes recuperadas '{key}'."); return df
        except Exception as e: logger.error(f"Erro buscar N klines '{key}'.", exc_info=True); return None
    def get_hist_klines_range(self, symbol: str, interval: str, start_ts_ms: int, end_ts_ms: int) -> pd.DataFrame | None:
        """Obtém velas históricas de um range de timestamps do Sorted Set."""
        key = self._generate_hist_key(symbol, interval)
        if not self.client: logger.error(f"Redis não init. ao buscar range para {key}"); return None
        start_dt = pd.to_datetime(start_ts_ms, unit='ms'); end_dt = pd.to_datetime(end_ts_ms, unit='ms')
        logger.info(f"Buscando hist '{key}' range {start_dt} a {end_dt}...");
        try:
            results = self.client.zrangebyscore(key, start_ts_ms, end_ts_ms, withscores=True)
            if not results: logger.warning(f"Histórico '{key}' sem dados no range."); return None
            klines_data = []
            for value_bytes, score_float in results:
                ts_ms = int(score_float); kline_dict = self._deserialize_kline(value_bytes, ts_ms)
                if kline_dict: klines_data.append(kline_dict)
            if not klines_data: logger.error(f"Falha desserializar klines range '{key}'."); return None
            df = pd.DataFrame(klines_data); df.rename(columns={'t': 'Open time', 'o': 'Open', 'h': 'High', 'l': 'Low','c': 'Close', 'v': 'Volume', 'T': 'Close time'}, inplace=True)
            df['Open time'] = pd.to_datetime(df['Open time'], unit='ms'); df['Close time'] = pd.to_datetime(df['Close time'], unit='ms'); df.set_index('Open time', inplace=True)
            numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume'];
            for col in numeric_cols:
                 if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
            logger.info(f"{len(df)} klines recuperados range '{key}'."); return df
        except Exception as e: logger.error(f"Erro buscar hist range '{key}'.", exc_info=True); return None