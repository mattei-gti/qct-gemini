# quantis_crypto_trader_gemini/config.py

import os
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from database import get_db, Setting # Importamos get_db e o modelo Setting

load_dotenv()

# --- Acesso a Configurações do .env ---
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

# --- Carregamento Inicial das Configs do Redis (do .env com defaults) ---
# Fazemos isso no nível superior para que fiquem disponíveis na importação
try:
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    # Tenta converter a porta para int, usa 6379 como padrão se falhar ou não existir
    redis_port_str = os.getenv('REDIS_PORT', '6379')
    REDIS_PORT = int(redis_port_str) if redis_port_str is not None else 6379
    # Tenta converter o db para int, usa 0 como padrão se falhar ou não existir
    redis_db_str = os.getenv('REDIS_DB', '0')
    REDIS_DB = int(redis_db_str) if redis_db_str is not None else 0
    print(f"Configurações Redis carregadas inicialmente do .env/padrões: Host={REDIS_HOST}, Port={REDIS_PORT}, DB={REDIS_DB}")
except ValueError as e:
    print(f"Erro ao converter configurações Redis de .env para int: {e}. Usando padrões.")
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379
    REDIS_DB = 0
except Exception as e:
     print(f"Erro inesperado ao carregar configs Redis do .env: {e}. Usando padrões.")
     REDIS_HOST = 'localhost'
     REDIS_PORT = 6379
     REDIS_DB = 0


# --- Funções de Acesso ao Banco de Dados (Settings Table) ---

def get_setting(db: Session, key: str, default: str | None = None) -> str | None:
    # ... (código da função get_setting continua igual) ...
    try:
        setting = db.query(Setting).filter(Setting.key == key).first()
        if setting:
            return setting.value
    except Exception as e:
        print(f"Alerta: Erro ao buscar setting '{key}': {e}")
        pass
    return default

def set_setting(db: Session, key: str, value: str):
    # ... (código da função set_setting continua igual) ...
    try:
        setting = db.query(Setting).filter(Setting.key == key).first()
        if setting:
            setting.value = value
            print(f"Configuração '{key}' atualizada no DB.")
        else:
            setting = Setting(key=key, value=value)
            db.add(setting)
            print(f"Configuração '{key}' adicionada ao DB.")
        db.commit()
    except Exception as e:
         print(f"Alerta: Erro ao definir setting '{key}': {e}")
         db.rollback()

# --- Função para Carregar/Definir Configs Iniciais do DB (Chamada por main.py) ---
# Esta função agora irá SOBRESCREVER as variáveis globais se encontrar valores no DB

def load_or_set_initial_db_settings():
    """Carrega configurações do DB ou define valores padrão se não existirem."""
    global REDIS_HOST, REDIS_PORT, REDIS_DB # Permite modificar as variáveis globais

    print("Verificando/Atualizando configurações (Redis) no banco de dados...")
    try:
        with next(get_db()) as db_session:
            # Lê do DB ou usa o valor já carregado do .env/padrão como default
            db_redis_host = get_setting(db_session, 'redis_host', REDIS_HOST)
            db_redis_port_str = get_setting(db_session, 'redis_port', str(REDIS_PORT))
            db_redis_db_str = get_setting(db_session, 'redis_db', str(REDIS_DB))

            # Define no DB se não existir, usando os valores do .env/padrão como base
            if get_setting(db_session, 'redis_host') is None:
                set_setting(db_session, 'redis_host', db_redis_host)
            if get_setting(db_session, 'redis_port') is None:
                set_setting(db_session, 'redis_port', db_redis_port_str)
            if get_setting(db_session, 'redis_db') is None:
                set_setting(db_session, 'redis_db', db_redis_db_str)

            # Atualiza as variáveis globais com os valores (preferencialmente do DB)
            REDIS_HOST = db_redis_host
            try:
                REDIS_PORT = int(db_redis_port_str) if db_redis_port_str is not None else 6379
            except ValueError:
                print(f"Aviso: Valor inválido para REDIS_PORT no DB ('{db_redis_port_str}'). Usando padrão 6379.")
                REDIS_PORT = 6379
            try:
                REDIS_DB = int(db_redis_db_str) if db_redis_db_str is not None else 0
            except ValueError:
                 print(f"Aviso: Valor inválido para REDIS_DB no DB ('{db_redis_db_str}'). Usando padrão 0.")
                 REDIS_DB = 0

        print(f"Configurações Redis (após verificação DB): Host={REDIS_HOST}, Port={REDIS_PORT}, DB={REDIS_DB}")

    except Exception as e:
        print(f"Erro CRÍTICO ao carregar/definir configurações do banco de dados: {e}")
        # Mantém os valores carregados do .env/padrões como fallback


print("Módulo de configuração 'config.py' carregado.")