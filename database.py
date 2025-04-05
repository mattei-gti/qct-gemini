# quantis_crypto_trader_gemini/database.py

import os # Importa o módulo os
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv # Importa apenas load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Pega a URL do banco de dados do .env usando os.getenv
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./quantis_trader.db") # Default para SQLite local

# Cria a engine do SQLAlchemy
# echo=True mostra os comandos SQL gerados (bom para debug, pode remover depois)
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}, echo=False)

# Cria uma fábrica de sessões para interagir com o banco
# autocommit=False e autoflush=False são configurações padrão recomendadas
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Cria uma classe Base da qual nossos modelos (tabelas) herdarão
Base = declarative_base()

# --- Definição Inicial de Modelos ---
# Vamos começar com uma tabela simples para configurações gerais

class Setting(Base):
    __tablename__ = "settings"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String, unique=True, index=True, nullable=False)
    value = Column(String, nullable=True) # Permite valores nulos

    def __repr__(self):
        return f"<Setting(key='{self.key}', value='{self.value}')>"

# --- Função para Inicializar o Banco de Dados ---

def init_db():
    """Cria as tabelas no banco de dados se elas não existirem."""
    print("Inicializando o banco de dados...")
    try:
        Base.metadata.create_all(bind=engine)
        print("Tabelas criadas (ou já existentes).")
    except Exception as e:
        print(f"Erro ao inicializar o banco de dados: {e}")

# --- Função para obter uma sessão do banco de dados (Context Manager) ---
# Usaremos isso em outros módulos para interagir com o DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()