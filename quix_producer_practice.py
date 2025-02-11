import time
import requests
from quixstreams import Application
import json
import logging

# Configuración del logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_weather():
    try:
        logger.debug("Obteniendo datos del clima...")
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": 51.5,
                "longitude": -0.11,
                "current": "temperature_2m",
            },
        )
        response.raise_for_status()  # Lanza una excepción si hay un error HTTP
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al obtener datos del clima: {e}")
        return None


# Configuración de la aplicación Kafka
app = Application(
    broker_address="localhost:9092",
    loglevel="DEBUG",
)

# Crear el productor Kafka
with app.get_producer() as producer:
    while True:
        weather = get_weather()
        if weather is not None:
            logger.info("Datos del clima obtenidos: %s", weather)
            try:
                # Enviar los datos al topic Kafka
                producer.produce(
                    topic="weather_data_demo",
                    key="London",
                    value=json.dumps(weather),
                )
                logger.info("Datos del clima enviados a Kafka")
            except Exception as e:
                logger.error(f"Error al enviar datos a Kafka: {e}")
        else:
            logger.warning(
                "No se pudieron obtener datos del clima. Reintentando en 10 segundos..."
            )

        # Esperar antes de la siguiente iteración
        time.sleep(10)

# import time
# import requests
# from quixstreams import Application
# import json
# from logging import Logger
# import logging
#
# logger = Logger(__name__, level=logging.DEBUG)
#
#
# def get_weather():
#     logger.debug("Getting the weather...")
#     response = requests.get(
#         "https://api.open-meteo.com/v1/forecast",
#         params={
#             "latitude": 51.5,
#             "longitude": -0.11,
#             "current": "temperature_2m",
#         },
#     )
#     return response.json()
#
#
# app = Application(
#     broker_address="localhost:9092",
#     loglevel="DEBUG",
# )
#
# with app.get_producer() as producer:
#     while True:
#         weather = get_weather()
#         logger.info("Weather data: %s", weather)
#         producer.produce(
#             topic="weather_data_demo",
#             key="London",
#             value=json.dumps(weather),
#         )
#         logger.info("Weather data sent to Kafka")
#         time.sleep(10)
