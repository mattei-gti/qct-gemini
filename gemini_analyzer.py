# quantis_crypto_trader_gemini/gemini_analyzer.py

import google.generativeai as genai
import pandas as pd
import config # Para acessar a API Key
import logging # Importa logging

# Obtém um logger para este módulo
logger = logging.getLogger(__name__)

class GeminiAnalyzer:
    def __init__(self, api_key: str):
        """Inicializa o cliente Gemini (Google AI)."""
        if not api_key:
            logger.error("API Key do Gemini não fornecida.")
            raise ValueError("API Key do Gemini não pode ser vazia.")

        self.api_key = api_key
        self.model = None # Inicializa como None

        try:
            logger.info("Configurando a API Google Generative AI...")
            genai.configure(api_key=self.api_key)
            # Modelos disponíveis (confirmado pela listagem anterior):
            # models/gemini-1.5-flash-latest
            # models/gemini-1.5-pro-latest
            self.model_name = "models/gemini-1.5-flash-latest" # Usamos o Flash como padrão

            logger.info(f"Inicializando o modelo Gemini: {self.model_name}...")
            # Configurações de segurança podem ser ajustadas aqui se necessário
            # Veja: https://ai.google.dev/docs/safety_setting_gemini
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            ]
            self.model = genai.GenerativeModel(
                model_name=self.model_name,
                safety_settings=safety_settings
             )
            # Teste simples para verificar se o modelo está acessível (opcional)
            # self.model.generate_content("Teste rápido.", generation_config=genai.types.GenerationConfig(candidate_count=1))
            logger.info(f"Modelo Gemini ('{self.model_name}') inicializado com sucesso.")
        except Exception as e:
            logger.critical(f"Erro CRÍTICO ao configurar ou inicializar o modelo Gemini.", exc_info=True)
            # Considerar se deve parar a aplicação ou tentar continuar sem análise AI
            raise ConnectionError(f"Falha ao inicializar Gemini: {e}") from e

    def get_trade_signal(self, klines_df: pd.DataFrame, symbol: str) -> str | None:
        """Analisa Klines e retorna sinal de trade (BUY, SELL, HOLD) via Gemini."""
        if self.model is None:
             logger.error("Modelo Gemini não inicializado. Impossível obter sinal.")
             return None

        if klines_df is None or klines_df.empty:
            logger.warning(f"Nenhum dado de Klines fornecido para análise do Gemini para {symbol}.")
            return None

        logger.info(f"Iniciando análise com Gemini para {symbol}...")

        # --- 1. Preparação de Dados (Exemplo Básico - PRECISA MELHORAR) ---
        # Poderia calcular indicadores: RSI, MACD, Médias Móveis, etc.
        # E formatar os dados de forma mais estruturada para o prompt.
        try:
            # Pega os últimos N fechamentos e volumes para o prompt
            num_klines_for_prompt = 10
            klines_subset = klines_df.tail(num_klines_for_prompt)
            if klines_subset.empty:
                 logger.warning(f"DataFrame de klines vazio após tail({num_klines_for_prompt}).")
                 return None

            closes = klines_subset['Close'].to_list()
            volumes = klines_subset['Volume'].round(2).to_list() # Arredonda volume
            current_price = closes[-1] if closes else "desconhecido"
            data_str = f"Últimos {len(closes)} preços de fechamento: {closes}\n"
            data_str += f"Últimos {len(volumes)} volumes: {volumes}"
            logger.debug(f"Dados preparados para prompt Gemini: {data_str}")

        except Exception as e:
            logger.error(f"Erro ao preparar dados do DataFrame para o Gemini.", exc_info=True)
            return None

        # --- 2. Engenharia do Prompt (Exemplo Básico - PRECISA MELHORAR) ---
        # Instruções claras, contexto, dados, e formato de resposta esperado.
        prompt = f"""
        Você é um assistente de análise técnica para o mercado de criptomoedas, focado em sinais de curtíssimo prazo (próximas horas). Seja conciso.
        Analise os seguintes dados recentes para o par {symbol}:
        {data_str}
        Preço atual aproximado: {current_price}

        Com base APENAS nestes dados de preço e volume, e considerando uma estratégia simples de momentum/reversão, qual é o sinal de trade mais apropriado para as próximas 1-4 horas?

        Responda APENAS com uma das seguintes palavras: BUY, SELL, ou HOLD.
        Não adicione explicações, justificativas ou qualquer outro texto.
        """

        logger.info("Enviando prompt para o Gemini...")
        logger.debug(f"Prompt Gemini para {symbol}:\n---\n{prompt}\n---") # Log do prompt no nível DEBUG

        # --- 3. Chamada da API e Parse da Resposta ---
        try:
            # Configuração da Geração
            generation_config = genai.types.GenerationConfig(
                candidate_count=1,
                temperature=0.2 # Baixa temperatura para respostas mais diretas/menos criativas
            )

            response = self.model.generate_content(
                prompt,
                generation_config=generation_config,
                # safety_settings=... # Se precisar sobrescrever os padrões do modelo
            )

            logger.debug(f"Resposta bruta Gemini: {response}") # Log da resposta completa em DEBUG

            # Tratamento da resposta - Verifica se há texto e extrai
            if response and response.parts:
                 signal_text = "".join(part.text for part in response.parts).strip().upper()
                 logger.info(f"Sinal bruto recebido do Gemini: '{signal_text}'")
                 # Validação rigorosa da resposta
                 if signal_text in ["BUY", "SELL", "HOLD"]:
                     logger.info(f"Sinal de trade validado: {signal_text}")
                     return signal_text
                 else:
                     logger.warning(f"Resposta do Gemini ('{signal_text}') não é um sinal válido (BUY/SELL/HOLD).")
                     # Logar mais detalhes se a resposta for inesperada
                     try:
                        logger.warning(f"Prompt Feedback: {response.prompt_feedback}")
                        # if response.candidates: logger.warning(f"Candidate Details: {response.candidates[0]}")
                     except Exception:
                        logger.warning("Não foi possível obter detalhes adicionais da resposta Gemini.")
                     return None
            else:
                # Caso a resposta seja vazia ou bloqueada por segurança (sem erro explícito)
                logger.warning("Resposta do Gemini vazia ou bloqueada.")
                try:
                    logger.warning(f"Prompt Feedback (se disponível): {response.prompt_feedback}")
                except Exception:
                     logger.warning("Não foi possível obter Prompt Feedback.")
                return None


        except Exception as e:
            logger.error(f"Erro durante a chamada ou processamento da API Gemini.", exc_info=True)
            # Tentar obter mais detalhes do erro da API, se disponíveis
            # if hasattr(e, 'response'): logger.error(f"Detalhes do erro API: {e.response}")
            return None