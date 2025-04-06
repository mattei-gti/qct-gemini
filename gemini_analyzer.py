# quantis_crypto_trader_gemini/gemini_analyzer.py

import google.generativeai as genai
import pandas as pd
import config
import logging

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

    # Função para Análise Multi-Timeframe
    def get_trade_signal_mta(self, mta_data: dict, symbol: str) -> str | None:
        """
        Analisa dados de múltiplos timeframes e retorna sinal (BUY, SELL, HOLD) via Gemini.

        Args:
            mta_data (dict): Dicionário contendo dados e SMAs para cada timeframe.
                             Ex: {'1M': {'df': df_1M, 'sma_fast': v, 'sma_slow': v}, '1d': {...}, ...}
            symbol (str): O símbolo do par (ex: BTCUSDT).

        Returns:
            str | None: O sinal ("BUY", "SELL", "HOLD") ou None.
        """
        if self.model is None: logger.error("Modelo Gemini não inicializado."); return None
        if not mta_data: logger.warning("Nenhum dado MTA fornecido para análise Gemini."); return None

        logger.info(f"Iniciando análise MTA com Gemini para {symbol}...")

        # --- 1. Preparação de Dados MTA para o Prompt ---
        data_str = f"Análise Multi-Timeframe para {symbol}:\n"
        current_price_source = None
        closes_limits = {'1M': 3, '1d': 5, '1h': 10, '15m': 12, '1m': 15} # Incluído 1m

        try:
            # Usa a ordem definida em main.py para apresentar os dados
            defined_intervals = ['1M', '1d', '1h', '15m', '1m']
            for tf_label in defined_intervals:
                if tf_label not in mta_data: continue # Pula se o TF não foi carregado

                data_str += f"\n--- Timeframe {tf_label} ---\n"
                tf_info = mta_data[tf_label]
                df = tf_info.get('df')
                sma_f = tf_info.get('sma_fast')
                sma_s = tf_info.get('sma_slow')

                if df is not None and not df.empty:
                    limit = closes_limits.get(tf_label, 5)
                    closes = df['Close'].tail(limit).round(2).to_list()
                    # volumes = df['Volume'].tail(limit).round(2).to_list() # Descomentar se quiser Volume
                    data_str += f"Ultimos {len(closes)} Fechamentos: {closes}\n"
                    # data_str += f"Ultimos {len(volumes)} Volumes: {volumes}\n"

                    # Pega o preço atual da fonte mais rápida (1m ou 15m ou 1h)
                    if current_price_source is None and tf_label in ['1m', '15m', '1h'] and closes:
                        current_price_source = closes[-1]

                    if sma_f is not None and sma_s is not None:
                        trend = "ALTA" if sma_f > sma_s else "BAIXA" if sma_f < sma_s else "LATERAL"
                        data_str += f"SMA30={sma_f}, SMA60={sma_s} (Tendencia {tf_label}: {trend})\n"
                    else: data_str += "SMAs nao disponiveis.\n"
                else: data_str += "Dados nao disponiveis.\n"

            # Adiciona Preço Atual no início
            current_price_str = f"Preco Atual Aprox ({symbol}): {current_price_source if current_price_source else 'N/A'}\n"
            data_str = current_price_str + data_str

            logger.debug(f"Dados MTA preparados para prompt Gemini:\n{data_str}")

        except Exception as e:
            logger.error("Erro ao preparar dados MTA para o Gemini.", exc_info=True)
            return None

        # --- 2. Engenharia do Prompt MTA ---
        prompt = f"""
        Você é um assistente de análise técnica para o mercado de criptomoedas ({symbol}), fornecendo sinais de trade de curto prazo (próximas horas) baseado nos dados fornecidos. Seja direto e objetivo.

        Dados recentes de Múltiplos Timeframes:
        {data_str}

        Instruções:
        1. Considere a tendência de longo prazo (1M, 1d).
        2. Analise a tendência de médio prazo (1h).
        3. Use os timeframes curtos (15m, 1m) para timing e confirmação.
        4. Procure por confluência ou divergência entre os timeframes.
        5. Baseado na sua análise MTA integrada, qual o sinal de trade mais apropriado para as PRÓXIMAS HORAS?

        Responda APENAS com uma única palavra: BUY, SELL, ou HOLD.
        """

        logger.info("Enviando prompt MTA para o Gemini...")
        logger.debug(f"Prompt MTA Gemini para {symbol}:\n---\n{prompt}\n---")

        # --- 3. Chamada da API e Parse da Resposta ---
        try:
            generation_config = genai.types.GenerationConfig(candidate_count=1, temperature=0.3)
            response = self.model.generate_content(prompt, generation_config=generation_config)
            logger.debug(f"Resposta bruta Gemini (MTA): {response}")

            if response and response.parts:
                 signal_text = "".join(part.text for part in response.parts).strip().upper()
                 if signal_text.startswith("```") and signal_text.endswith("```"): signal_text = signal_text[3:-3].strip()
                 elif signal_text.startswith("`") and signal_text.endswith("`"): signal_text = signal_text[1:-1].strip()

                 logger.info(f"Sinal bruto (MTA) recebido do Gemini: '{signal_text}'")
                 if signal_text in ["BUY", "SELL", "HOLD"]:
                     logger.info(f"Sinal de trade (MTA) validado: {signal_text}")
                     return signal_text
                 else:
                     logger.warning(f"Resposta MTA do Gemini ('{signal_text}') nao e sinal valido.")
                     try: logger.warning(f"Prompt Feedback MTA: {response.prompt_feedback}")
                     except Exception: pass
                     return None
            else:
                 logger.warning("Resposta MTA do Gemini vazia ou bloqueada.")
                 try: logger.warning(f"Prompt Feedback MTA (se disponivel): {response.prompt_feedback}")
                 except Exception: pass
                 return None
        except Exception as e:
            logger.error("Erro durante a chamada ou processamento da API Gemini (MTA).", exc_info=True)
            return None