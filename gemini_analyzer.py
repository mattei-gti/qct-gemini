# quantis_crypto_trader_gemini/gemini_analyzer.py

import google.generativeai as genai
import pandas as pd
import config # Para acessar a API Key

class GeminiAnalyzer:
    def __init__(self, api_key: str):
        """
        Inicializa o cliente Gemini (Google AI).

        Args:
            api_key (str): Sua chave de API do Google AI (Gemini).
        """
        if not api_key:
            raise ValueError("API Key do Gemini não pode ser vazia.")

        try:
            genai.configure(api_key=api_key)
            # Configurações de segurança podem ser ajustadas aqui se necessário
            # safety_settings = [...]
            self.model = genai.GenerativeModel(
                model_name="models/gemini-1.5-flash-latest",
                # safety_settings=safety_settings
             )
            print("Modelo Gemini ('models/gemini-1.5-flash-latest') inicializado com sucesso.")
        except Exception as e:
            print(f"Erro ao configurar ou inicializar o modelo Gemini: {e}")
            raise e

    def get_trade_signal(self, klines_df: pd.DataFrame, symbol: str) -> str | None:
        """
        Analisa os dados de Klines e retorna um sinal de trade (BUY, SELL, HOLD)
        usando o Gemini.

        Args:
            klines_df (pd.DataFrame): DataFrame contendo os dados de Klines recentes.
            symbol (str): O símbolo do par (ex: BTCUSDT) para contexto.

        Returns:
            str | None: O sinal ("BUY", "SELL", "HOLD") ou None em caso de erro ou resposta inválida.
        """
        if klines_df is None or klines_df.empty:
            print("Aviso: Nenhum dado de Klines fornecido para análise do Gemini.")
            return None

        print(f"\nIniciando análise com Gemini para {symbol}...")

        # --- 1. Preparação de Dados (Exemplo MUITO Básico) ---
        # Pegamos apenas os últimos 5 preços de fechamento como exemplo.
        # **ESTA PARTE PRECISA SER MELHORADA COM BASE NA SUA ESTRATÉGIA!**
        # Poderia incluir volume, indicadores (RSI, MACD), etc.
        try:
            last_closes = klines_df['Close'].tail(5).to_list()
            data_str = f"Últimos {len(last_closes)} preços de fechamento: {last_closes}"
            current_price = last_closes[-1] if last_closes else "desconhecido"
        except Exception as e:
            print(f"Erro ao preparar dados para o Gemini: {e}")
            return None

        # --- 2. Engenharia do Prompt (Exemplo MUITO Básico) ---
        # **ESTA PARTE É CRUCIAL E PRECISA SER EXPERIMENTADA E REFINADA!**
        prompt = f"""
        Você é um assistente de análise técnica de criptomoedas conciso.
        Analise os seguintes dados recentes para o par {symbol}:
        {data_str}
        Preço atual aproximado: {current_price}

        Com base APENAS nesses dados limitados, qual seria o sinal de trade de curtíssimo prazo mais provável?

        Responda EXATAMENTE com uma das seguintes palavras: BUY, SELL, ou HOLD.
        Não inclua nenhuma outra palavra ou explicação.
        """

        print("Enviando prompt para o Gemini...")
        # print(f"DEBUG - Prompt:\n---\n{prompt}\n---") # Descomente para ver o prompt

        # --- 3. Chamada da API e Parse da Resposta ---
        try:
            # Configuração da Geração - pode ajustar temperature, etc.
            generation_config = genai.types.GenerationConfig(
                candidate_count=1,
                # temperature=0.7 # Pode experimentar para variar a criatividade/determinismo
            )

            response = self.model.generate_content(
                prompt,
                generation_config=generation_config,
                # safety_settings=... # Se precisar ajustar segurança
            )

            # print(f"DEBUG - Resposta Bruta Gemini: {response}") # Descomente para debug

            # Extrai o texto da resposta
            # Às vezes a resposta pode vir em partes, mas para gemini-pro geralmente é direto
            signal_text = response.text.strip().upper()

            print(f"Sinal bruto recebido do Gemini: '{signal_text}'")

            # Valida se a resposta é uma das esperadas
            if signal_text in ["BUY", "SELL", "HOLD"]:
                print(f"Sinal de trade validado: {signal_text}")
                return signal_text
            else:
                print(f"Erro: Resposta do Gemini ('{signal_text}') não é um sinal válido (BUY, SELL, HOLD).")
                # Tentar entender o que veio na resposta pode ser útil aqui
                # print(f"Conteúdo completo da resposta: {response.candidates}")
                # print(f"Prompt Feedback: {response.prompt_feedback}")
                return None

        except Exception as e:
            print(f"Erro durante a chamada ou processamento da API Gemini: {e}")
            # Tentar obter mais detalhes do erro da API, se disponíveis
            # if hasattr(e, 'response'): print(f"Detalhes do erro API: {e.response}")
            return None