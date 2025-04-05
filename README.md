Core Engine / Orquestrador: O cérebro do robô, que coordena todos os outros módulos.
Binance Client: Responsável pela comunicação com a API da Binance (coleta de dados, envio de ordens).
Redis Cache: Gerencia o armazenamento e recuperação de dados históricos e de estado.
Gemini Analyzer: Interage com a API do Gemini para obter insights e sinais de trade.
Telegram Interface: Lida com o envio de notificações/confirmações e recebimento de comandos via Telegram.
Data Processor: Prepara os dados para análise (pelo Gemini ou outras lógicas).
Strategy Manager: Define e aplica a lógica de trade baseada nos sinais do Gemini e outras regras.
Streamlit Dashboard: (Um script separado) Visualiza dados, performance e status do robô.
Configuração e Logging: Gerencia chaves de API, parâmetros e registra eventos.
