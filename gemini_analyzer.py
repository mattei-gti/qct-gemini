# quantis_crypto_trader_gemini/gemini_analyzer.py

import google.generativeai as genai
import pandas as pd
import config
import logging
import json
import re

logger = logging.getLogger(__name__)

class GeminiAnalyzer:
    def __init__(self, api_key: str):
        """Inicializa o cliente Gemini (Google AI)."""
        # ... (init como antes) ...
        if not api_key: logger.error("API Key Gemini não fornecida."); raise ValueError("API Key Gemini não pode ser vazia.")
        self.api_key = api_key; self.model = None; self.model_name = "models/gemini-1.5-flash-latest"
        try:
            logger.info("Configurando API Google Generative AI..."); genai.configure(api_key=self.api_key)
            safety_settings = [{"category": c, "threshold": "BLOCK_MEDIUM_AND_ABOVE"} for c in ["HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH", "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"]]
            logger.info(f"Inicializando modelo Gemini: {self.model_name}..."); self.model = genai.GenerativeModel(model_name=self.model_name, safety_settings=safety_settings)
            logger.info(f"Modelo Gemini ('{self.model_name}') inicializado.")
        except Exception as e: logger.critical("Erro CRÍTICO ao inicializar modelo Gemini.", exc_info=True); raise ConnectionError(f"Falha init Gemini: {e}") from e


    # >>> Função ATUALIZADA para receber current_ticker_price <<<
    def get_trade_signal_mta_indicators(self,
                                        mta_indicators_data: dict,
                                        symbol: str,
                                        current_ticker_price: float | None = None # NOVO ARGUMENTO
                                        ) -> tuple[str | None, str | None]:
        """
        Analisa indicadores MTA Intraday e retorna sinal e justificativa via Gemini.

        Args:
            mta_indicators_data (dict): Dict[tf_label, dict_de_indicadores].
            symbol (str): Símbolo do par.
            current_ticker_price (float | None): Preço mais recente do ticker (opcional).

        Returns:
            tuple[str | None, str | None]: (sinal, justificativa) ou (None, None).
        """
        if self.model is None: logger.error("Modelo Gemini não inicializado."); return None, None
        if not mta_indicators_data: logger.warning("Nenhum dado MTA+Ind fornecido."); return None, None

        logger.info(f"Iniciando análise Intraday+Indicadores com Gemini para {symbol} (Ticker: {current_ticker_price})...")

        # --- 1. Preparação de Dados para o Prompt ---
        data_str = f"Contexto de Mercado Intraday para {symbol}:\n"
        try:
            # *** USA O current_ticker_price RECEBIDO ***
            price_display = f"{current_ticker_price:.2f}" if current_ticker_price is not None else "N/A"
            current_price_str = f"Preco ATUAL Ticker ({symbol}): {price_display}\n"
            # *** FIM DA MUDANÇA ***

            ordered_tfs = ['1h', '15m', '1m'] # Foco Intraday
            # latest_price = None # Removido, usamos o argumento

            for tf_label in ordered_tfs:
                if tf_label in mta_indicators_data:
                    indicators = mta_indicators_data[tf_label]
                    data_str += f"\n--- Timeframe {tf_label} ---\n"
                    if not indicators: data_str += "Indicadores indisponíveis.\n"; continue
                    indicator_parts = []
                    for key, value in indicators.items(): # Formata indicadores
                        if value is not None:
                            decimals = 0 if key in ['obv'] else 4 if key in ['atr', 'vwap'] else 2
                            try: indicator_parts.append(f"{key.replace('_',' ').title()}={value:.{decimals}f}")
                            except (TypeError, ValueError): indicator_parts.append(f"{key.replace('_',' ').title()}={value}")
                    if indicator_parts: data_str += ", ".join(indicator_parts) + "\n"
                    else: data_str += "Nenhum indicador calculado.\n"
                else: # Log se um TF esperado não veio
                     logger.warning(f"Dados para timeframe {tf_label} não encontrados em mta_indicators_data.")
                     data_str += f"\n--- Timeframe {tf_label} ---\nDados indisponíveis.\n"

            data_str = current_price_str + data_str # Prepend o preço do ticker
            logger.debug(f"Dados Intraday+Ind preparados para prompt Gemini:\n{data_str}")

        except Exception as e:
            logger.error("Erro ao preparar dados Intraday+Ind para o Gemini.", exc_info=True)
            return None, None

        # --- 2. Engenharia do Prompt (Intraday, Menos Conservador) ---
        # O prompt em si não precisa mudar muito, pois já pede análise dos dados fornecidos
        # e o preço do ticker agora está incluído nesses dados.
        prompt = f"""
        Você é um assistente de análise técnica para {symbol}, focado em identificar oportunidades de trade de CURTO PRAZO (próximas horas) usando Análise Multi-Timeframe Intraday. Seja DECISIVO quando houver sinais claros.

        Contexto de Mercado Fornecido (Indicadores recentes para 1h, 15m, 1m e Preço Atual do Ticker):
        {data_str}

        Guia Rápido de Indicadores:
        - Tendência: SMAs (30/60), Ichimoku (Preço vs Nuvem, Tenkan vs Kijun). (Foco no 1h)
        - Momentum: RSI (14) (<30 Sobrevenda, >70 Sobrecompra), MACD (Linha vs Sinal, Histograma). (Foco 1h/15m)
        - Volatilidade/Extremos: Bollinger Bands (Preço vs Bandas), ATR (Valor). (Foco 15m/1m)
        - Confirmação/Timing: OBV (Fluxo Volume), VWAP (Preço vs VWAP), Preço Ticker vs Últimos Fechamentos/Indicadores. (Foco 15m/1m)

        Tarefa:
        1. Avalie a tendência e o contexto principal no timeframe de 1h.
        2. Analise os timeframes 15m e 1m para identificar momentum, condições de sobrecompra/venda e possíveis pontos de entrada/saída, comparando com o preço atual do ticker.
        3. PROCURE POR CONFLUÊNCIA: Se múltiplos indicadores e TFs (1h, 15m, 1m) apontam na mesma direção, seja mais propenso a dar um sinal (BUY/SELL).
        4. SEJA MENOS CONSERVADOR: Se os TFs curtos (15m, 1m) mostrarem um sinal forte (ex: RSI < 30 claro, MACD cruzando) mesmo com o 1h lateral, considere um sinal de entrada, mas evite ir contra uma tendência MUITO FORTE e clara no 1h. Use HOLD apenas se os sinais forem genuinamente conflitantes ou muito fracos em todos os TFs Intraday (1h, 15m, 1m).
        5. Determine o sinal (BUY, SELL ou HOLD) para as PRÓXIMAS HORAS.

        Formato OBRIGATÓRIO da Resposta:
        Linha 1: APENAS a palavra BUY, SELL, ou HOLD.
        Linha 2: Justificativa MUITO BREVE (máx 15 palavras), focada nos TFs/indicadores decisivos.
        """

        logger.info("Enviando prompt Intraday+Ind+Ticker (com justif.) para o Gemini...")
        logger.debug(f"Prompt Intraday+Ind Gemini para {symbol}:\n---\n{prompt}\n---")

        # --- 3. Chamada da API e Parse (Helper Function) ---
        return self._call_gemini_api_with_justification(prompt)


    def _call_gemini_api_with_justification(self, prompt: str) -> tuple[str | None, str | None]:
        """Chama a API Gemini e tenta extrair Sinal e Justificativa."""
        # ... (código _call_gemini_api_with_justification como antes) ...
        if self.model is None: logger.error("Modelo Gemini não pronto."); return None, None; signal = None; justification = None; 
        try:
            generation_config = genai.types.GenerationConfig(candidate_count=1, temperature=0.4); response = self.model.generate_content(prompt, generation_config=generation_config)
            logger.debug(f"Resposta bruta Gemini: {response}")
            if response and response.parts:
                 full_text = "".join(part.text for part in response.parts).strip(); logger.info(f"Texto completo Gemini: '{full_text}'"); lines = full_text.split('\n', 1); potential_signal = lines[0].strip().upper(); potential_signal = re.sub(r'[`\*_]', '', potential_signal).strip()
                 valid_signals = ["BUY", "SELL", "HOLD"];
                 if potential_signal in valid_signals: signal = potential_signal; logger.info(f"Sinal validado: {signal}")
                 else: logger.warning(f"Linha 1 ('{lines[0]}') nao e sinal valido.");
                 if signal is None: # Fallback
                    for vs in valid_signals:
                         if f" {vs} " in f" {potential_signal} " or potential_signal == vs: signal = vs; logger.warning(f"Sinal '{signal}' extraido linha 1 c/ texto extra."); break
                 if len(lines) > 1: justification = lines[1].strip(); # Pega justificativa
                 elif signal: logger.warning("Justificativa nao encontrada (Linha 2).")
                 if justification: logger.info(f"Justificativa extraída: '{justification}'")
            else: logger.warning("Resposta Gemini vazia/bloqueada."); 
            try: logger.warning(f"Prompt Feedback: {response.prompt_feedback}") 
            except Exception: pass
        except Exception as e: logger.error("Erro chamada/proc API Gemini.", exc_info=True)
        if signal is None: logger.warning("Nao foi possivel validar/extrair sinal trade.")
        return signal, justification