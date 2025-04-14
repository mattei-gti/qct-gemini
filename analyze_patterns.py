# analyze_patterns.py

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import logging
import sys
import os

# Configura logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

# --- Parâmetros ---
# *** ATUALIZADO: Nome do novo arquivo CSV gerado ***
CSV_FILE_PATH = 'FIRST_successful_entries_BTCUSDT_15m_profit2pct_lookahead24.csv'

# *** Lista de indicadores para análise inicial ***
# Verifique no seu novo CSV se a coluna ATR é 'ATR_14' ou ainda 'ATRr_14' e ajuste aqui se necessário.
INDICATORS_TO_ANALYZE = [
    'Close', 'SMA_30', 'SMA_60', 'RSI_14', 'MACD_12_26_9', 'MACDh_12_26_9',
    'MACDs_12_26_9', 'OBV', 'ITS_21', 'IKS_34', 'ISA_21', 'ISB_52',
    'BBL_20_2.0', 'BBM_20_2.0', 'BBU_20_2.0', 'BBP_20_2.0',
    'ATR_14', # <<< Assumindo que find_patterns.py agora gerou ATR_14. VERIFIQUE!
    'VWAP_D'
]
# Diretório para salvar os gráficos
PLOT_OUTPUT_DIR = "analysis_plots_15m_FIRST" # Novo diretório para não misturar
os.makedirs(PLOT_OUTPUT_DIR, exist_ok=True)

# --- Carregar Dados ---
logger.info(f"Carregando dados de: {CSV_FILE_PATH}")
try:
    df = pd.read_csv(CSV_FILE_PATH, parse_dates=['entry_timestamp'], index_col='entry_timestamp')
    logger.info(f"Dados carregados com sucesso. {len(df)} entradas (primeiras do grupo) encontradas.")
    # Tenta converter colunas para numérico
    for col in df.columns:
        if col != df.index.name:
             df[col] = pd.to_numeric(df[col], errors='ignore')
except FileNotFoundError: logger.error(f"Erro: Arquivo '{CSV_FILE_PATH}' não encontrado."); sys.exit(1)
except Exception as e: logger.error(f"Erro ao carregar/processar o CSV: {e}", exc_info=True); sys.exit(1)

# --- Análise Básica ---
logger.info("\n--- Informações do DataFrame (Dados Refinados) ---")
df.info(verbose=True, show_counts=True) # Mostra info detalhada
logger.info("\n--- Primeiras 5 Entradas Lucrativas (Primeiras do Grupo) ---")
try: print(df.head().to_markdown(numalign="left", stralign="left"))
except ImportError: print(df.head())

logger.info("\n--- Estatísticas Descritivas (Dados Refinados) ---")
# Seleciona colunas numéricas para descrever
df_numeric = df.select_dtypes(include=np.number)
cols_to_describe = [col for col in INDICATORS_TO_ANALYZE if col in df_numeric.columns]
if cols_to_describe:
    stats = df_numeric[cols_to_describe].describe()
    try: print(stats.to_markdown())
    except ImportError: print(stats)
else:
    logger.warning("Nenhuma coluna da lista INDICATORS_TO_ANALYZE encontrada ou numérica.")
    logger.info(f"Colunas numéricas disponíveis: {df_numeric.columns.tolist()}")

logger.info("\n--- Gerando Histogramas Individuais (Dados Refinados) ---")
# Seleciona indicadores para histogramas
skip_hist = ['Open','High','Low','Close','Volume','OBV','VWAP_D', 'ITS_21', 'IKS_34', 'ISA_21', 'ISB_52', 'BBL_20_2.0', 'BBM_20_2.0', 'BBU_20_2.0', 'SMA_30', 'SMA_60']
cols_for_hist = [col for col in cols_to_describe if col not in skip_hist]

for indicator_col in cols_for_hist:
    logger.info(f"Gerando histograma para: {indicator_col}")
    plt.figure(figsize=(10, 6))
    data_to_plot = df[indicator_col].dropna()
    if not data_to_plot.empty:
        plt.hist(data_to_plot, bins=50, alpha=0.75, edgecolor='black', density=True)
        data_to_plot.plot(kind='kde', ax=plt.gca(), color='red', linewidth=1.5)
        plt.title(f'Distribuição de {indicator_col} em Entradas Lucrativas (>=2%, Primeiro do Grupo)') # Título Atualizado
        plt.xlabel(f'Valor {indicator_col}'); plt.ylabel('Densidade de Frequência')
        plt.grid(axis='y', alpha=0.5, linestyle='--')
        plot_filename = f"histogram_{indicator_col}_FIRST.png" # Novo nome de arquivo
        plt.savefig(os.path.join(PLOT_OUTPUT_DIR, plot_filename)); plt.close()
        logger.info(f"Histograma salvo em: {os.path.join(PLOT_OUTPUT_DIR, plot_filename)}")
    else: logger.warning(f"Sem dados válidos para histograma de {indicator_col}.")

# --- Análise Condicional (Filtros Combinados - Usando Dados Refinados) ---
logger.info("\n" + "="*10 + " INICIANDO ANÁLISE CONDICIONAL (Dados Refinados) " + "="*10)
INDICATORS_TO_DESCRIBE_CONDITIONALLY = ['RSI_14', 'MACDh_12_26_9', 'BBP_20_2.0', 'ATR_14' if 'ATR_14' in df.columns else 'ATRr_14'] # Usa ATR ou ATRr
conditional_cols = [col for col in INDICATORS_TO_DESCRIBE_CONDITIONALLY if col in df_numeric.columns]

if conditional_cols: # Só executa se tiver colunas para descrever
    # Filtro 1: RSI < 40
    logger.info("\n--- Filtro 1: Entradas com RSI_14 < 40 ---")
    df_rsi_low = df[df['RSI_14'] < 40]
    if not df_rsi_low.empty:
        logger.info(f"Encontradas {len(df_rsi_low)} entradas ({len(df_rsi_low)/len(df)*100:.1f}% do total) com RSI_14 < 40.")
        stats_rsi_low = df_rsi_low[conditional_cols].describe(); 
        try: print(stats_rsi_low.to_markdown())
        except ImportError: print(stats_rsi_low)
    else: logger.info("Nenhuma entrada encontrada com RSI_14 < 40.")

    # Filtro 2: BBP < 0.2
    logger.info("\n--- Filtro 2: Entradas com BBP_20_2.0 < 0.2 ---")
    df_bbp_low = df[df['BBP_20_2.0'] < 0.2]
    if not df_bbp_low.empty:
        logger.info(f"Encontradas {len(df_bbp_low)} entradas ({len(df_bbp_low)/len(df)*100:.1f}% do total) com BBP_20_2.0 < 0.2.")
        stats_bbp_low = df_bbp_low[conditional_cols].describe(); 
        try: print(stats_bbp_low.to_markdown())
        except ImportError: print(stats_bbp_low)
    else: logger.info("Nenhuma entrada encontrada com BBP_20_2.0 < 0.2.")

    # Filtro 3: SMA_30 > SMA_60
    logger.info("\n--- Filtro 3: Entradas com SMA_30 > SMA_60 ---")
    df_sma_bull = df[df['SMA_30'] > df['SMA_60']]
    if not df_sma_bull.empty:
        logger.info(f"Encontradas {len(df_sma_bull)} entradas ({len(df_sma_bull)/len(df)*100:.1f}% do total) com SMA_30 > SMA_60.")
        stats_sma_bull = df_sma_bull[conditional_cols].describe(); 
        try: print(stats_sma_bull.to_markdown())
        except ImportError: print(stats_sma_bull)
    else: logger.info("Nenhuma entrada encontrada com SMA_30 > SMA_60.")

    # Filtro 4: MACDh > 0
    logger.info("\n--- Filtro 4: Entradas com MACDh_12_26_9 > 0 ---")
    df_macdh_pos = df[df['MACDh_12_26_9'] > 0]
    if not df_macdh_pos.empty:
        logger.info(f"Encontradas {len(df_macdh_pos)} entradas ({len(df_macdh_pos)/len(df)*100:.1f}% do total) com MACDh > 0.")
        stats_macdh_pos = df_macdh_pos[conditional_cols].describe(); 
        try: print(stats_macdh_pos.to_markdown())
        except ImportError: print(stats_macdh_pos)
    else: logger.info("Nenhuma entrada encontrada com MACDh > 0.")

    # Filtro 5: Combinado (RSI < 40 E SMA_30 > SMA_60)
    logger.info("\n--- Filtro 5: Entradas com RSI_14 < 40 E SMA_30 > SMA_60 ---")
    df_combo = df[(df['RSI_14'] < 40) & (df['SMA_30'] > df['SMA_60'])]
    if not df_combo.empty:
        logger.info(f"Encontradas {len(df_combo)} entradas ({len(df_combo)/len(df)*100:.1f}% do total) com RSI < 40 E SMA30 > SMA60.")
        stats_combo = df_combo[conditional_cols].describe(); 
        try: print(stats_combo.to_markdown())
        except ImportError: print(stats_combo)
    else: logger.info("Nenhuma entrada encontrada com RSI < 40 E SMA30 > SMA60.")
else:
     logger.error("Nenhuma coluna de indicador válida encontrada para análise condicional.")


logger.info("\n" + "="*10 + " ANÁLISE CONDICIONAL CONCLUÍDA " + "="*10)
logger.info(f"Verifique os gráficos PNG na pasta: {PLOT_OUTPUT_DIR}")
logger.info("Compare estas estatísticas condicionais com as estatísticas gerais e os histogramas.")