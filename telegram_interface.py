# quantis_crypto_trader_gemini/telegram_interface.py

import requests
import config # Para pegar o Token e Chat ID
import logging # Importa logging

# Obtém um logger para este módulo
logger = logging.getLogger(__name__)

# Constante para a URL base da API do Telegram Bot
TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/{method}"

def send_telegram_message(message_text: str, disable_notification: bool = False):
    """Envia uma mensagem de texto para o Chat ID configurado via Telegram Bot API."""
    bot_token = config.TELEGRAM_BOT_TOKEN
    chat_id = config.TELEGRAM_CHAT_ID

    if not bot_token or not chat_id:
        logger.error("Token do Bot ou Chat ID do Telegram não configurados.")
        return False # Indica falha

    # Monta a URL do método sendMessage
    url = TELEGRAM_API_URL.format(token=bot_token, method="sendMessage")

    # Prepara os dados para a requisição POST
    payload = {
        'chat_id': chat_id,
        'text': message_text,
        'parse_mode': 'MarkdownV2', # Mantém MarkdownV2
        'disable_notification': disable_notification,
        'disable_web_page_preview': True # Desabilita previews de links
    }

    logger.debug(f"Enviando mensagem Telegram para Chat ID {chat_id}: '{message_text[:50]}...'") # Loga início da msg

    try:
        response = requests.post(url, data=payload, timeout=10) # Timeout de 10 segundos
        response.raise_for_status() # Levanta erro para status HTTP 4xx/5xx

        response_json = response.json()
        if response_json.get("ok"):
            logger.info(f"Mensagem enviada com sucesso para o Telegram (Chat ID: {chat_id}).")
            return True
        else:
            # Loga o erro retornado pela API do Telegram
            error_desc = response_json.get('description', 'Sem descrição')
            error_code = response_json.get('error_code', 'N/A')
            logger.error(f"Erro da API Telegram ao enviar mensagem: Código {error_code} - {error_desc}")
            # Logar o payload pode ajudar a depurar erros 400 (Bad Request)
            logger.debug(f"Payload enviado que causou erro: {payload}")
            return False

    except requests.exceptions.Timeout:
        logger.error(f"Timeout ao enviar mensagem para o Telegram (URL: {url}).")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de rede/HTTP ao enviar mensagem para o Telegram.", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Erro inesperado ao enviar mensagem para o Telegram.", exc_info=True)
        return False

def escape_markdown_v2(text: str) -> str:
    """Escapa caracteres especiais para o modo MarkdownV2 do Telegram."""
    if not isinstance(text, str): # Garante que a entrada é string
        try:
            text = str(text)
        except Exception:
            logger.warning(f"Não foi possível converter '{type(text)}' para string em escape_markdown_v2. Retornando string vazia.")
            return ""

    # Lista de caracteres a escapar segundo a documentação oficial
    # https://core.telegram.org/bots/api#markdownv2-style
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    # Usa uma compreensão de lista para construir a nova string escapada
    return "".join(['\\' + char if char in escape_chars else char for char in text])