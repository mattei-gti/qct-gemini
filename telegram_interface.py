# quantis_crypto_trader_gemini/telegram_interface.py

import requests
import config # Para pegar o Token e Chat ID

# Constante para a URL base da API do Telegram Bot
TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/{method}"

def send_telegram_message(message_text: str, disable_notification: bool = False):
    """
    Envia uma mensagem de texto para o Chat ID configurado via Telegram Bot API.

    Args:
        message_text (str): O texto da mensagem a ser enviada.
        disable_notification (bool): Se True, envia a mensagem silenciosamente.
                                      Usuários receberão uma notificação sem som.
    """
    bot_token = config.TELEGRAM_BOT_TOKEN
    chat_id = config.TELEGRAM_CHAT_ID

    if not bot_token or not chat_id:
        print("Erro: Token do Bot ou Chat ID do Telegram não configurados em config.py/.env.")
        return False # Indica falha

    # Monta a URL do método sendMessage
    url = TELEGRAM_API_URL.format(token=bot_token, method="sendMessage")

    # Prepara os dados para a requisição POST
    payload = {
        'chat_id': chat_id,
        'text': message_text,
        'parse_mode': 'MarkdownV2', # Ou 'HTML'. Permite formatação básica. Cuidado com caracteres especiais!
        'disable_notification': disable_notification
    }

    try:
        response = requests.post(url, data=payload, timeout=10) # Timeout de 10 segundos
        response.raise_for_status() # Levanta um erro para status HTTP ruins (4xx ou 5xx)

        # Verifica a resposta da API do Telegram
        response_json = response.json()
        if response_json.get("ok"):
            print(f"Mensagem enviada com sucesso para o Telegram (Chat ID: {chat_id}).")
            return True # Indica sucesso
        else:
            print(f"Erro retornado pela API do Telegram: {response_json.get('description')}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"Erro de rede/HTTP ao enviar mensagem para o Telegram: {e}")
        return False
    except Exception as e:
        print(f"Erro inesperado ao enviar mensagem para o Telegram: {e}")
        return False

def escape_markdown_v2(text: str) -> str:
    """
    Escapa caracteres especiais para o modo MarkdownV2 do Telegram.
    Veja: https://core.telegram.org/bots/api#markdownv2-style
    """
    # Cuidado: Esta lista pode precisar de ajustes dependendo do seu uso.
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    # Escapa cada caractere na string que está na lista de escape_chars
    return "".join(f'\\{char}' if char in escape_chars else char for char in text)