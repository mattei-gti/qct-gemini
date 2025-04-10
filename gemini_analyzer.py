# quantis_crypto_trader_gemini/gemini_analyzer.py

import google.generativeai as genai
import pandas as pd
import config
import logging
import json

logger = logging.getLogger(__name__)

class GeminiAnalyzer:
    def __init__(self, api_key: str):
        """Inicializa o cliente Gemini (Google AI)."""
        if not api_key: logger.error("API Key Gemini não fornecida."); raise ValueError("API Key Gemini não pode ser vazia.")
        self.api_key = api_key; self.model = None; self.model_name = "models/gemini-1.5-flash-latest"
        try:
            logger.info("Configurando API Google Generative AI..."); genai.configure(api_key=self.api_key)
            safety_settings = [{"category": c, "threshold": "BLOCK_MEDIUM_AND_ABOVE"} for c in ["HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH", "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"]]
            logger.info(f"Inicializando modelo Gemini: {self.model_name}..."); self.model = genai.GenerativeModel(model_name=self.model_name, safety_settings=safety_settings)
            logger.info(f"Modelo Gemini ('{self.model_name}') inicializado.")
        except Exception as e: logger.critical("Erro CRÍTICO ao inicializar modelo Gemini.", exc_info=True); raise ConnectionError(f"Falha init Gemini: {e}") from e

    # Função original (MTA simples com SMAs) - Mantida para referência, mas não usada por main.py agora
    def get_trade_signal_mta(self, mta_data: dict, symbol: str) -> str | None:
        logger.warning("Chamando DEPRECATED get_trade_signal_mta. Use get_trade_signal_mta_indicators.")
        # Chama a nova função com dados limitados
        indicators_only = {tf: {'sma_fast': data.get('sma_fast'), 'sma_slow': data.get('sma_slow')} for tf, data in mta_data.items()}
        return self.get_trade_signal_mta_indicators(indicators_only, symbol)


    # >>> Função ATUALIZADA para Análise MTA com Múltiplos Indicadores <<<
    def get_trade_signal_mta_indicators(self, mta_indicators_data: dict, symbol: str) -> str | None:
        """
        Analisa indicadores de múltiplos timeframes e retorna sinal via Gemini.

        Args:
            mta_indicators_data (dict): Dict onde chaves=TFs, valores=dict de indicadores{'rsi':v,...}.
            symbol (str): Símbolo do par.

        Returns: str | None: Sinal ("BUY", "SELL", "HOLD") ou None.
        """
        if self.model is None: logger.error("Modelo Gemini não inicializado."); return None
        if not mta_indicators_data: logger.warning("Nenhum dado MTA+Ind fornecido para análise Gemini."); return None

        logger.info(f"Iniciando análise MTA+Indicadores com Gemini para {symbol}...")

        # --- 1. Preparação de Dados para o Prompt ---
        data_str = f"Contexto de Mercado Multi-Timeframe para {symbol}:\n"
        try:
            ordered_tfs = ['1M', '1d', '1h', '15m', '1m'] # Ordem de apresentação
            for tf_label in ordered_tfs:
                if tf_label in mta_indicators_data:
                    indicators = mta_indicators_data[tf_label]
                    data_str += f"\n--- Timeframe {tf_label} ---\n"
                    if not indicators: data_str += "Indicadores indisponíveis.\n"; continue

                    indicator_parts = []
                    # Formata cada indicador encontrado (arredonda floats)
                    for key, value in indicators.items():
                        if value is not None:
                            # Define formatação específica por indicador se necessário
                            if key in ['obv']: decimals = 0
                            elif key in ['atr', 'vwap']: decimals = 4 # Mais precisão para estes
                            else: decimals = 2 # Padrão
                            try: indicator_parts.append(f"{key.replace('_',' ').title()}={value:.{decimals}f}")
                            except (TypeError, ValueError): indicator_parts.append(f"{key.replace('_',' ').title()}={value}")

                    if indicator_parts: data_str += ", ".join(indicator_parts) + "\n"
                    else: data_str += "Nenhum indicador calculado disponível.\n"

            # Adiciona o preço mais recente (pega do 1m ou 15m se disponível nos indicadores)
            latest_price = None
            for tf in ['1m', '15m', '1h']: # Tenta pegar de um TF curto/médio
                 # Precisa garantir que o 'Close' foi passado ou buscar no DF se ele fosse passado
                 # ALTERNATIVA: Pegar o 'sma_fast' ou 'bb_middle' do timeframe mais curto como proxy
                 if tf in mta_indicators_data and mta_indicators_data[tf]:
                      if mta_indicators_data[tf].get('sma_fast') is not None: # Usa SMA rápida como proxy de preço
                           latest_price = mta_indicators_data[tf]['sma_fast']; break
            if latest_price: data_str = f"Preço/SMA30 Recente Aprox ({symbol}): {latest_price:.2f}\n" + data_str

            logger.debug(f"Dados MTA+Ind preparados para prompt Gemini:\n{data_str}")

        except Exception as e:
            logger.error("Erro ao preparar dados MTA+Indicadores para o Gemini.", exc_info=True)
            return None

        # --- 2. Engenharia do Prompt MTA com Indicadores ---
        prompt = f"""
        Você é um analista quantitativo de criptomoedas experiente em Multi-Timeframe Analysis (MTA) para {symbol}. Seu objetivo é gerar um sinal de trade de curto prazo (próximas horas a 1 dia).

        Contexto de Mercado Fornecido (valores recentes dos indicadores para cada timeframe):
        {data_str}

        Guia de Interpretação Rápida dos Indicadores:
        - SMA 30/60: Tendência (curta > longa = alta). Preço vs SMAs.
        - RSI(14): Momentum (<30 sobrevendido, >70 sobrecomprado).
        - MACD(12,26,9): Momentum (Linha vs Sinal, Histograma > 0).
        - OBV: Confirmação de volume (OBV acompanha preço?).
        - Ichimoku(21,34,52): Tendência/Suporte/Resistência (Preço vs Nuvem, Tenkan vs Kijun).
        - BBands(20,2): Volatilidade (largura das bandas), Extremos (preço vs bandas sup/inf).
        - ATR(14): Medida de volatilidade recente (valor absoluto).
        - VWAP: Preço médio ponderado por volume (referência de valor).

        Tarefa:
        1. Avalie a tendência principal (1M, 1d) usando SMAs e Ichimoku.
        2. Analise a tendência e momentum de médio prazo (1h) usando SMAs, MACD, Ichimoku.
        3. Verifique as condições de curto prazo (15m, 1m) usando RSI, BBands, MACD para timing e confirmação. Considere o VWAP.
        4. Busque confluência entre timeframes e indicadores. Divergências podem indicar cautela ou reversão. Use OBV e ATR como contexto adicional.
        5. Com base na análise integrada, qual o sinal mais provável e prudente para as PRÓXIMAS HORAS?

        Responda APENAS com uma única palavra: BUY, SELL, ou HOLD.
        """

        logger.info("Enviando prompt MTA+Indicadores para o Gemini...")
        logger.debug(f"Prompt MTA+Ind Gemini para {symbol}:\n---\n{prompt}\n---")

        # --- 3. Chamada da API e Parse (Helper Function) ---
        return self._call_gemini_api(prompt)


    def _call_gemini_api(self, prompt: str) -> str | None:
        """Função helper para chamar a API Gemini e processar a resposta."""
        if self.model is None: logger.error("Modelo Gemini não pronto para chamada API."); return None
        try:
            generation_config = genai.types.GenerationConfig(candidate_count=1, temperature=0.4) # Aumentei levemente temp para análise complexa
            response = self.model.generate_content(prompt, generation_config=generation_config)
            logger.debug(f"Resposta bruta Gemini: {response}")

            if response and response.parts:
                 signal_text = "".join(part.text for part in response.parts).strip().upper()
                 # Limpeza extra
                 if signal_text.startswith("```") and signal_text.endswith("```"): signal_text = signal_text[3:-3].strip()
                 elif signal_text.startswith("`") and signal_text.endswith("`"): signal_text = signal_text[1:-1].strip()

                 logger.info(f"Sinal bruto recebido do Gemini: '{signal_text}'")
                 valid_signals = ["BUY", "SELL", "HOLD"]
                 if signal_text in valid_signals:
                     logger.info(f"Sinal de trade validado: {signal_text}"); return signal_text
                 else: # Tenta extrair se a resposta for mais longa
                      found_signal = None
                      for valid_sig in valid_signals:
                          if f" {valid_sig} " in f" {signal_text} " or signal_text.startswith(valid_sig) or signal_text.endswith(valid_sig):
                              logger.warning(f"Resposta Gemini continha texto extra, sinal '{valid_sig}' extraído de '{signal_text}'.")
                              found_signal = valid_sig; break
                      if found_signal: return found_signal
                      # Se não encontrou nada parecido
                      logger.warning(f"Resposta Gemini ('{signal_text}') não é/contém sinal válido (BUY/SELL/HOLD).")
                      try: logger.warning(f"Prompt Feedback: {response.prompt_feedback}")
                      except Exception: pass
                      return None
            else: # Resposta vazia ou bloqueada
                 logger.warning("Resposta Gemini vazia ou bloqueada."); 
                 try: logger.warning(f"Prompt Feedback: {response.prompt_feedback}")
                 except Exception: pass; return None
        except Exception as e:
            logger.error("Erro durante chamada/processamento API Gemini.", exc_info=True)
            return None