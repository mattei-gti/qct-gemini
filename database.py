# quantis_crypto_trader_gemini/database.py

import os
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import logging # Importa logging

# Obtém um logger para este módulo
logger = logging.getLogger(__name__)

# Carrega as variáveis de ambiente do arquivo .env
# É chamado aqui e em config.py, não tem problema
load_dotenv()

# Pega a URL do banco de dados do .env
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./quantis_trader.db") # Default para SQLite local

# Cria a engine do SQLAlchemy
try:
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
        echo=False # Definir como True para ver SQL gerado (nível DEBUG)
    )
    logger.debug(f"SQLAlchemy engine criada para: {DATABASE_URL}")
except Exception as e:
    logger.critical(f"Falha ao criar SQLAlchemy engine para {DATABASE_URL}", exc_info=True)
    raise # Re-levanta a exceção para parar a inicialização

# Cria uma fábrica de sessões para interagir com o banco
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Cria uma classe Base da qual nossos modelos (tabelas) herdarão
Base = declarative_base()

# --- Definição Inicial de Modelos ---

class Setting(Base):
    __tablename__ = "settings"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String, unique=True, index=True, nullable=False)
    value = Column(String, nullable=True)

    def __repr__(self):
        return f"<Setting(key='{self.key}', value='{self.value}')>"

# Adicionar outros modelos aqui depois (Users, UserApiKeys, TradeHistory...)

# --- Função para Inicializar o Banco de Dados ---

def init_db():
    """Cria as tabelas no banco de dados se elas não existirem."""
    logger.info("Inicializando o banco de dados (verificando/criando tabelas)...")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Verificação/criação de tabelas concluída.")
    except Exception as e:
        logger.critical("Erro CRÍTICO ao inicializar o banco de dados (create_all).", exc_info=True)
        # Considerar se deve parar a aplicação aqui ou tentar continuar
        raise # Re-levanta a exceção

# --- Função para obter uma sessão do banco de dados (Context Manager) ---
def get_db():
    """Fornece uma sessão do banco de dados gerenciada."""
    db = SessionLocal()
    logger.debug(f"Sessão DB {id(db)} aberta.")
    try:
        yield db
    except Exception:
        logger.error("Erro ocorrido dentro do contexto da sessão DB.", exc_info=True)
        db.rollback() # Garante rollback em caso de exceção não tratada
        raise # Re-levanta para que o erro seja tratado mais acima se necessário
    finally:
        logger.debug(f"Sessão DB {id(db)} fechada.")
        db.close()