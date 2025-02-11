import time
import requests
import os
from quixstreams import Application
import json
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.Logger(__name__)
app = Application(
    broker_address="localhost:9092",
    loglevel="DEBUG",
)

API_KEY_NEWS = os.getenv("API_KEY_NEWS")


def get_response_from_news():
    logger.debug("Getting data from News API")

    response = requests.get(
        f"https://newsapi.org/v2/top-headlines?q=Nvidia&country=us&category=technology&apiKey={API_KEY_NEWS}"
    )

    response.raise_for_status()

    logger.debug("200 Response")

    return response.json()


def main():
    with app.get_producer() as producer:
        logger.info("Starting the producer and connection to Kafka")

        news_data = get_response_from_news()

        producer.produce(
            topic="news_data",
            key="news",
            value=json.dumps(news_data),
        )

        logger.info("News sent to the broker!")

    time.sleep(10)


if __name__ == "__main__":
    main()
