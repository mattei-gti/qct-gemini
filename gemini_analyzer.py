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
        if not api_key: logger.error("API Key Gemini não fornecida."); raise ValueError("API Key Gemini não pode ser vazia.")
        self.api_key = api_key; self.model = None; self.model_name = "models/gemini-1.5-flash-latest"
        try:
            logger.info("Configurando API Google Generative AI..."); genai.configure(api_key=self.api_key)
            safety_settings = [{"category": c, "threshold": "BLOCK_MEDIUM_AND_ABOVE"} for c in ["HARM_CATEGORY_HARASSMENT", "HARM_CATEGORY_HATE_SPEECH", "HARM_CATEGORY_SEXUALLY_EXPLICIT", "HARM_CATEGORY_DANGEROUS_CONTENT"]]
            logger.info(f"Inicializando modelo Gemini: {self.model_name}..."); self.model = genai.GenerativeModel(model_name=self.model_name, safety_settings=safety_settings)
            logger.info(f"Modelo Gemini ('{self.model_name}') inicializado.")
        except Exception as e: logger.critical("Erro CRÍTICO ao inicializar modelo Gemini.", exc_info=True); raise ConnectionError(f"Falha init Gemini: {e}") from e

    # >>> Função ATUALIZADA para Análise MTA com Múltiplos Indicadores <<<
    def get_trade_signal_mta_indicators(self, mta_indicators_data: dict, symbol: str) -> tuple[str | None, str | None]:
        """
        Analisa indicadores MTA e retorna sinal (BUY/SELL/HOLD) e justificativa via Gemini.
        """
        if self.model is None: logger.error("Modelo Gemini não inicializado."); return None, None
        if not mta_indicators_data: logger.warning("Nenhum dado MTA+Ind fornecido."); return None, None

        logger.info(f"Iniciando análise MTA+Indicadores com Gemini para {symbol} (com justificativa)...")

        # --- 1. Preparação de Dados para o Prompt ---
        data_str = f"Contexto de Mercado Multi-Timeframe para {symbol}:\n"
        try:
            ordered_tfs = ['1M', '1d', '1h', '15m', '1m']; latest_price = None
            for tf_label in ordered_tfs:
                if tf_label in mta_indicators_data:
                    indicators = mta_indicators_data[tf_label]; data_str += f"\n--- Timeframe {tf_label} ---\n"
                    if not indicators: data_str += "Indicadores indisponíveis.\n"; continue
                    indicator_parts = []
                    for key, value in indicators.items():
                        if value is not None:
                            decimals = 0 if key in ['obv'] else 4 if key in ['atr', 'vwap'] else 2
                            try: indicator_parts.append(f"{key.replace('_',' ').title()}={value:.{decimals}f}")
                            except (TypeError, ValueError): indicator_parts.append(f"{key.replace('_',' ').title()}={value}")
                    if indicator_parts: data_str += ", ".join(indicator_parts) + "\n"
                    else: data_str += "Nenhum indicador calculado disponível.\n"
                    # Pega preço proxy da SMA rápida mais recente (1m, 15m ou 1h)
                    if latest_price is None and tf_label in ['1m', '15m', '1h'] and indicators.get('sma_fast') is not None:
                         latest_price = indicators['sma_fast']

            # *** CORREÇÃO AQUI: Formata o preço ANTES de colocá-lo na string principal ***
            price_display = f"{latest_price:.2f}" if latest_price is not None else "N/A"
            current_price_str = f"Preço/SMA30 Recente Aprox ({symbol}): {price_display}\n"
            # *** FIM DA CORREÇÃO ***

            data_str = current_price_str + data_str # Adiciona o preço no início
            logger.debug(f"Dados MTA+Ind preparados para prompt Gemini:\n{data_str}")

        except Exception as e:
            logger.error("Erro ao preparar dados MTA+Indicadores para o Gemini.", exc_info=True)
            return None, None

        # --- 2. Engenharia do Prompt MTA com Indicadores (Prompt revisado para clareza) ---
        prompt = f"""
        Você é um assistente de análise técnica para {symbol}, realizando Análise Multi-Timeframe (MTA).

        Dados Atuais (Valores recentes de indicadores por timeframe):
        {data_str}

        Guia Rápido de Indicadores:
        - Tendência: SMAs (30/60), Ichimoku (Preço vs Nuvem, Tenkan vs Kijun).
        - Momentum: RSI (14) (<30 Sobrevenda, >70 Sobrecompra), MACD (Linha vs Sinal, Histograma).
        - Volatilidade: Bollinger Bands (Largura, Preço vs Bandas), ATR (Valor).
        - Volume: OBV (Confirmação).
        - Preço Médio: VWAP.

        Tarefa:
        1. Avalie a tendência principal (1M, 1d).
        2. Analise o médio prazo (1h).
        3. Verifique o curto prazo (15m, 1m) para timing/confirmação.
        4. Busque confluência ou divergências entre TFs e indicadores.
        5. Determine o sinal de trade (BUY, SELL ou HOLD) para as PRÓXIMAS HORAS.

        Formato OBRIGATÓRIO da Resposta:
        Linha 1: APENAS a palavra BUY, SELL, ou HOLD.
        Linha 2: Justificativa MUITO BREVE (máx 15 palavras). Ex: "Tendência alta 1h/1d, RSI 15m sobrevendido."
        """

        logger.info("Enviando prompt MTA+Indicadores (com justif.) para o Gemini...")
        logger.debug(f"Prompt MTA+Ind Gemini para {symbol}:\n---\n{prompt}\n---")

        # --- 3. Chamada da API e Parse (Helper Function) ---
        return self._call_gemini_api_with_justification(prompt)


    def _call_gemini_api_with_justification(self, prompt: str) -> tuple[str | None, str | None]:
        """Chama a API Gemini e tenta extrair Sinal e Justificativa."""
        # ... (código _call_gemini_api_with_justification como antes) ...
        if self.model is None: logger.error("Modelo Gemini não pronto."); return None, None
        signal = None; justification = None
        try:
            generation_config = genai.types.GenerationConfig(candidate_count=1, temperature=0.4)
            response = self.model.generate_content(prompt, generation_config=generation_config)
            logger.debug(f"Resposta bruta Gemini: {response}")
            if response and response.parts:
                 full_text = "".join(part.text for part in response.parts).strip(); logger.info(f"Texto completo Gemini: '{full_text}'")
                 lines = full_text.split('\n', 1); potential_signal = lines[0].strip().upper(); potential_signal = re.sub(r'[`\*_]', '', potential_signal).strip()
                 valid_signals = ["BUY", "SELL", "HOLD"];
                 if potential_signal in valid_signals: signal = potential_signal; logger.info(f"Sinal validado: {signal}")
                 else: logger.warning(f"Linha 1 ('{lines[0]}') nao e sinal valido."); # Tenta fallback
                 if signal is None: # Fallback se não achou na primeira linha isolada
                    for vs in valid_signals:
                         if f" {vs} " in f" {potential_signal} " or potential_signal == vs: signal = vs; logger.warning(f"Sinal '{signal}' extraido da linha 1 com texto extra."); break
                 if len(lines) > 1: justification = lines[1].strip(); # Pega justificativa
                 elif signal: logger.warning("Justificativa nao encontrada (Linha 2).")
                 if justification: logger.info(f"Justificativa extraída: '{justification}'")
            else: logger.warning("Resposta Gemini vazia/bloqueada."); 
            try: logger.warning(f"Prompt Feedback: {response.prompt_feedback}") 
            except Exception: pass
        except Exception as e: logger.error("Erro chamada/proc API Gemini.", exc_info=True)
        if signal is None: logger.warning("Nao foi possivel validar/extrair sinal trade.")
        return signal, justification