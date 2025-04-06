# quantis_crypto_trader_gemini/telegram_interface.py

import requests
import config # Para pegar o Token e Chat ID
import logging

logger = logging.getLogger(__name__)

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/{method}"

# Removida a função escape_markdown_v2

def send_telegram_message(message_text: str, disable_notification: bool = False):
    """
    Envia uma mensagem de texto simples para o Chat ID configurado via Telegram Bot API.
    """
    bot_token = config.TELEGRAM_BOT_TOKEN
    chat_id = config.TELEGRAM_CHAT_ID

    if not bot_token or not chat_id:
        logger.error("Token do Bot ou Chat ID do Telegram não configurados.")
        return False

    url = TELEGRAM_API_URL.format(token=bot_token, method="sendMessage")

    # Prepara os dados SEM parse_mode para enviar texto simples
    payload = {
        'chat_id': chat_id,
        'text': message_text,
        'disable_notification': disable_notification,
        'disable_web_page_preview': True
    }

    # Limita o tamanho da mensagem para evitar erros do Telegram (limite é 4096)
    if len(message_text) > 4090:
        logger.warning(f"Mensagem Telegram excedeu limite, truncando. Original: '{message_text[:100]}...'")
        message_text = message_text[:4090] + "..."
        payload['text'] = message_text # Atualiza payload com texto truncado


    logger.debug(f"Enviando mensagem Telegram (simples) para Chat ID {chat_id}: '{message_text[:70]}...'")

    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status() # Levanta erro para status HTTP 4xx/5xx

        response_json = response.json()
        if response_json.get("ok"):
            logger.info(f"Mensagem enviada com sucesso para o Telegram (Chat ID: {chat_id}).")
            return True
        else:
            error_desc = response_json.get('description', 'Sem descrição')
            error_code = response_json.get('error_code', 'N/A')
            logger.error(f"Erro da API Telegram ao enviar mensagem: Código {error_code} - {error_desc}")
            logger.debug(f"Payload que causou erro: {payload}") # Log do payload ajuda a depurar 400
            return False

    except requests.exceptions.Timeout:
        logger.error(f"Timeout ao enviar mensagem para o Telegram (URL: {url}).")
        return False
    except requests.exceptions.RequestException as e:
        logger.error("Erro de rede/HTTP ao enviar mensagem para o Telegram.", exc_info=True)
        return False
    except Exception as e:
        logger.error("Erro inesperado ao enviar mensagem para o Telegram.", exc_info=True)
        return False