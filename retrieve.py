import os
import asyncio
import json
import csv 
from twikit import Client
from datetime import datetime, timedelta, timezone
import pytz
import time
from dotenv import load_dotenv
import logging  # Importar o módulo logging

# Configurar o logging
logging.basicConfig(
    level=logging.INFO,  # Nível mínimo de log
    format='%(asctime)s - %(levelname)s - %(message)s',  # Formato das mensagens de log
    handlers=[
        logging.FileHandler("logUS.txt", encoding='utf-8'),  # Log para o arquivo
        logging.StreamHandler()  # Log para o console
    ]
)

# Carregar variáveis de ambiente
load_dotenv(override=True)

USERNAME = os.getenv("USERNAME")
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")

COOKIES_FILE = "cookies.json"
client = Client('en-US')

RATE_LIMIT_SLEEP = 900  # 15 minutos em segundos
SEARCH_RATE = 49  # Limite de consultas antes de pausar

# Definir o fuso horário de São Paulo
saopaulo_tz = pytz.timezone('America/Sao_Paulo')

async def login_with_cookies():
    try:
        if os.path.exists(COOKIES_FILE):
            with open(COOKIES_FILE, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
                client.set_cookies(cookies)
                logging.info("Login realizado usando cookies.")
                return

        await client.login(auth_info_1=USERNAME, auth_info_2=EMAIL, password=PASSWORD)
        cookies = client.get_cookies()
        with open(COOKIES_FILE, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, ensure_ascii=False, indent=4)
        logging.info("Login realizado e cookies salvos.")
    except Exception as e:
        logging.error(f"Erro durante o login: {e}")

def format_datetime_for_query(dt):
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime('%Y-%m-%d_%H:%M:%S_UTC')

def parse_twitter_datetime(date_str):
    return datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y')

async def fetch_and_save_tweets(start_date, end_date, keyword, parameters, output_dir):
    query = f"{keyword} {parameters} since:{format_datetime_for_query(start_date)} until:{format_datetime_for_query(end_date)}"
    logging.info(f"Executando consulta: {query}")

    next_cursor = None
    request_count = 0  # Contador de solicitações

    while True:
        try:
            # Verificar se atingiu o limite de consultas
            if request_count >= SEARCH_RATE:
                logging.warning(f"Atingiu o limite de {SEARCH_RATE} solicitações. Aguardando {RATE_LIMIT_SLEEP / 60} minutos...")
                time.sleep(RATE_LIMIT_SLEEP)
                request_count = 0  # Reiniciar o contador

            # Buscar lote de tweets
            result = await client.search_tweet(query=query, product="Latest", count=20, cursor=next_cursor)
            logging.info(f"Número de tweets obtidos: {len(result)}")

            # Parar a execução se não encontrar tweets
            if len(result) == 0:
                logging.info("Nenhum tweet encontrado. Encerrando a execução.")
                return None  # Interrompe o processo

            # Ordenar os tweets por data de criação
            sorted_tweets = sorted(result, key=lambda t: parse_twitter_datetime(t.created_at))

            # Obter data do primeiro e último tweet
            first_tweet_date = parse_twitter_datetime(sorted_tweets[0].created_at)
            last_tweet_date = parse_twitter_datetime(sorted_tweets[-1].created_at)

            # Nome do arquivo JSON
            filename = f"{first_tweet_date.strftime('%Y-%m-%d_%H-%M-%S')}_to_{last_tweet_date.strftime('%Y-%m-%d_%H-%M-%S')}.json"
            filepath = os.path.join(output_dir, filename)

            # Converter tweets para dicionários e salvar imediatamente
            tweet_data = [
                {
                    "User": tweet.user.name,
                    "Content": tweet.text,
                    "Created_at": tweet.created_at,
                }
                for tweet in sorted_tweets
            ]

            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(tweet_data, f, ensure_ascii=False, indent=4)
                logging.info(f"Tweets salvos em: {filepath}")
            except Exception as e:
                logging.error(f"Erro ao salvar JSON: {e}")

            # Atualizar o cursor para a próxima iteração
            next_cursor = result.next_cursor

            # Incrementar o contador de solicitações
            request_count += 1

            if not next_cursor:
                break

        except Exception as e:
            logging.error(f"Erro ao buscar tweets: {e}")
            logging.warning(f"Aguardando {RATE_LIMIT_SLEEP / 60} minutos devido ao rate limit...")
            time.sleep(RATE_LIMIT_SLEEP)
            continue

    # Retorna a data do primeiro tweet deste lote para atualizar `current_end`
    return first_tweet_date

async def main():
    await login_with_cookies()

    csv_file = "input.csv"  # Nome do arquivo CSV de entrada

    # Definir os parâmetros de busca
    parameters = "lang:pt"

    # Definir o intervalo de datas (pode ser ajustado conforme necessário)
    start_date_sp = saopaulo_tz.localize(datetime(2024, 10, 20, 0, 0, 0))
    end_date_sp = saopaulo_tz.localize(datetime(2024, 10, 27, 8, 0, 0))

    # Ler o arquivo CSV
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = list(reader)
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo CSV: {e}")
        return

    # Iterar sobre cada linha do CSV
    for index, row in enumerate(rows, start=1):
        if len(row) < 2:
            logging.warning(f"Linha {index} do CSV está incompleta. Pulando...")
            continue

        output_dir, keyword = row[0].strip(), row[1].strip()

        # Criar o diretório de saída se não existir
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Iniciando extração para a busca: '{keyword}' e salvando em '{output_dir}'")

        current_start = start_date_sp
        current_end = end_date_sp

        try:
            while current_start < current_end:
                logging.info(f"Iniciando busca de {current_start} até {current_end} para a keyword '{keyword}'")

                # Buscar e salvar tweets, e obter a data do primeiro tweet para atualizar current_end
                first_tweet_date = await fetch_and_save_tweets(
                    current_start,
                    current_end,
                    keyword,
                    parameters,
                    output_dir
                )

                # Parar se nenhum tweet foi encontrado
                if first_tweet_date is None:
                    logging.info("Processo interrompido por falta de tweets.")
                    break

                # Atualizar current_end para a data do primeiro tweet - 1 segundo
                current_end = first_tweet_date.astimezone(saopaulo_tz) - timedelta(seconds=1)
                logging.info(f"Atualizando janela para: {current_start} até {current_end}")

        except KeyboardInterrupt:
            logging.warning("Processo interrompido pelo usuário.")
            return  # Opcionalmente, pode salvar o estado ou realizar outras ações

        logging.info(f"Extração concluída para a busca: '{keyword}'\n")

    logging.info("Todas as extrações foram concluídas.")

# Executar a função principal
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.warning("Execução do programa interrompida.")
