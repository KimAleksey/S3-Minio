import logging
import time

from requests import get

from minio import Minio

# Импорт из локальной переменной секретных данных
from dotenv import load_dotenv
from os import getenv
from pathlib import Path



# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# Получаем сикреты
env_path = Path(__file__).resolve().parent / "conf" / ".env"
load_dotenv(env_path)
# access key для подключения к bucket
access_key = getenv("MINIO_ACCESS_KEY")
# secret key для подключения к bucket
secret_key = getenv("MINIO_SECRET_KEY")


def main():
    logging.info("Starting working with project using Minio.")

    # Не меняется, это единый endpoint для подключения к S3
    endpoint = 'localhost:9000'
    bucket_name = 'project-bucket'

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    if not client.bucket_exists(bucket_name):
        client.make_bucket(
            bucket_name=bucket_name,
        )

    # Загрузка данных за 2025-год
    for i in range(1, 2):
        # Фиксируем время старта
        start = time.time()
        # Имя файла - имя, с которым выгружается файл
        filename = f"yellow_tripdata_2025-{i:02}.parquet"
        # Путь до файла
        file_path = "./yellow_tripdata_2025/" + filename
        # Ссылка для скачивания файла - для каждого месяца меняется
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
        # INFO - Start
        logging.info(f"Start of downloading {filename}")
        logging.info(f"URL: {url}")
        try:
            # нужен для больших файлов, чтобы MinIO мог получать их напрямую, по частям, без сохранения на диск.
            r = get(url, stream=True, timeout=60)
            # Проверка статуса = ОК
            r.raise_for_status()
            # Размер файла
            length = int(r.headers.get("Content-Length", -1))
            # Загружаем файл
            client.put_object(
                bucket_name=bucket_name,
                object_name=filename,
                data=r.raw,
                length=length,
                part_size=10 * 1024 * 1024, # 50MB
                content_type=r.headers.get("Content-Type", "application/octet-stream"),
            )
            logging.info(f"Downloaded {filename} into {file_path}")
        except Exception as e:
            logging.error(e)
        end = time.time()
        # INFO - End
        logging.info(f"End of downloading {filename}.")
        logging.info(f"{filename} was downloaded in {round((end - start), 2)} seconds.")
    logging.info(f"End of downloading data into Minio.")

if __name__ == "__main__":
    main()