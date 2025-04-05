# quantis_crypto_trader_gemini/config.py

import os
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from database import get_db, Setting # Importamos get_db e o modelo Setting
import logging # Importa logging

# Obtém um logger para este módulo
logger = logging.getLogger(__name__)

load_dotenv()

# --- Acesso a Configurações do .env ---
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

# --- Carregamento Inicial das Configs do Redis (do .env com defaults) ---
try:
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    redis_port_str = os.getenv('REDIS_PORT', '6379')
    REDIS_PORT = int(redis_port_str) if redis_port_str is not None else 6379
    redis_db_str = os.getenv('REDIS_DB', '0')
    REDIS_DB = int(redis_db_str) if redis_db_str is not None else 0
    # Logger ainda não está configurado aqui, então usamos print se precisar muito,
    # ou confiamos que a configuração do logger em main.py mostrará isso se quisermos.
    # print(f"Debug Config: Redis carregado do .env/padrões: Host={REDIS_HOST}, Port={REDIS_PORT}, DB={REDIS_DB}")
except ValueError as e:
    # Usamos print aqui pois o logger pode não estar configurado ainda quando este módulo é importado
    print(f"ERRO CONFIG: Erro ao converter configurações Redis de .env para int: {e}. Usando padrões.")
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379
    REDIS_DB = 0
except Exception as e:
     print(f"ERRO CONFIG: Erro inesperado ao carregar configs Redis do .env: {e}. Usando padrões.")
     REDIS_HOST = 'localhost'
     REDIS_PORT = 6379
     REDIS_DB = 0


# --- Funções de Acesso ao Banco de Dados (Settings Table) ---

def get_setting(db: Session, key: str, default: str | None = None) -> str | None:
    """Busca uma configuração no banco de dados pela chave."""
    try:
        setting = db.query(Setting).filter(Setting.key == key).first()
        if setting:
            # logger.debug(f"Setting '{key}' lido do DB: '{setting.value}'") # Debug opcional
            return setting.value
    except Exception as e:
        # Usamos logger aqui pois get_setting é chamado depois da config do logger
        logger.error(f"Erro ao buscar setting '{key}' no DB.", exc_info=True)
    # logger.debug(f"Setting '{key}' não encontrado no DB, usando default: '{default}'") # Debug opcional
    return default

def set_setting(db: Session, key: str, value: str):
    """Define ou atualiza uma configuração no banco de dados."""
    try:
        setting = db.query(Setting).filter(Setting.key == key).first()
        if setting:
            if setting.value != value: # Evita commit desnecessário se o valor for o mesmo
                setting.value = value
                db.commit()
                logger.info(f"Configuração '{key}' atualizada no DB para '{value}'.")
            # else:
                # logger.debug(f"Configuração '{key}' já estava com valor '{value}' no DB.") # Debug opcional
        else:
            setting = Setting(key=key, value=value)
            db.add(setting)
            db.commit()
            logger.info(f"Configuração '{key}' adicionada ao DB com valor '{value}'.")
    except Exception as e:
         logger.error(f"Erro ao definir setting '{key}'='{value}' no DB.", exc_info=True)
         try:
             db.rollback() # Tenta reverter a transação em caso de erro
         except Exception as rb_e:
             logger.error(f"Erro adicional ao tentar rollback após falha em set_setting para '{key}': {rb_e}", exc_info=True)


# --- Função para Carregar/Definir Configs Iniciais do DB (Chamada por main.py) ---
def load_or_set_initial_db_settings():
    """Carrega configurações do DB ou define valores padrão se não existirem."""
    global REDIS_HOST, REDIS_PORT, REDIS_DB # Permite modificar as variáveis globais

    logger.info("Verificando/Atualizando configurações (Redis) no banco de dados...")
    try:
        with next(get_db()) as db_session:
            # Lê do DB ou usa o valor já carregado do .env/padrão como default
            db_redis_host = get_setting(db_session, 'redis_host', REDIS_HOST)
            db_redis_port_str = get_setting(db_session, 'redis_port', str(REDIS_PORT))
            db_redis_db_str = get_setting(db_session, 'redis_db', str(REDIS_DB))

            # Define no DB se não existir, usando os valores do .env/padrão como base
            # set_setting já loga se adicionar/atualizar
            set_setting(db_session, 'redis_host', db_redis_host)
            set_setting(db_session, 'redis_port', db_redis_port_str)
            set_setting(db_session, 'redis_db', db_redis_db_str)

            # Atualiza as variáveis globais com os valores (preferencialmente do DB)
            REDIS_HOST = db_redis_host
            try:
                REDIS_PORT = int(db_redis_port_str) if db_redis_port_str is not None else 6379
            except ValueError:
                logger.warning(f"Valor inválido para REDIS_PORT no DB ('{db_redis_port_str}'). Usando .env/padrão {REDIS_PORT}.", exc_info=True)
                # Mantém o valor carregado do .env/padrão se o do DB for inválido
            try:
                REDIS_DB = int(db_redis_db_str) if db_redis_db_str is not None else 0
            except ValueError:
                 logger.warning(f"Valor inválido para REDIS_DB no DB ('{db_redis_db_str}'). Usando .env/padrão {REDIS_DB}.", exc_info=True)
                 # Mantém o valor carregado do .env/padrão

        logger.info(f"Configurações Redis (após verificação DB): Host={REDIS_HOST}, Port={REDIS_PORT}, DB={REDIS_DB}")

    except Exception as e:
        # Erro CRÍTICO aqui pode indicar problema com o DB
        logger.critical("Erro CRÍTICO ao carregar/definir configurações do banco de dados.", exc_info=True)
        logger.warning("Mantendo configurações Redis carregadas do .env/padrões como fallback.")


# logger.debug("Módulo de configuração 'config.py' carregado.") # Debug opcional