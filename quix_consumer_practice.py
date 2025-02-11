from quixstreams import Application
import time
import logging

# Configuración del logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Application(
    broker_address="localhost:9092",
    consumer_group="weather_data_reader",
    loglevel="DEBUG",
    auto_offset_reset="earliest",
)


def main():
    try:
        with app.get_consumer() as consumer:
            # Suscribirse al topic
            consumer.subscribe(["weather_data_demo"])
            logger.info("Suscrito al topic 'weather_data_demo'")

            while True:
                msg = consumer.poll(1.0)  # Esperar hasta 1 segundo por un mensaje
                if msg is None:
                    logger.debug("Esperando mensajes...")
                    continue

                if msg.error():
                    # Manejar errores en el mensaje
                    logger.error(f"Error al recibir mensaje: {msg.error()}")
                    continue

                # Procesar el mensaje
                key = msg.key().decode("utf-8") if msg.key() else "No key"
                offset = msg.offset()
                value = msg.value().decode("utf-8") if msg.value() else "No value"

                logger.info(
                    f"Mensaje recibido: Key={key}, Value={value}, Offset={offset}"
                )

                # Confirmar el offset manualmente
                # consumer.store_offsets(msg)
                # logger.debug("Offset confirmado")

                # Simular procesamiento
                time.sleep(2)

    except KeyboardInterrupt:
        logger.info("Deteniendo el consumidor...")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")


if __name__ == "__main__":
    main()

# from quixstreams import Application
# import time
#
#
# app = Application(
#     broker_address="localhost:9092",
#     consumer_group="weather_data_reader",
#     loglevel="DEBUG",
#     auto_offset_reset="latest",
# )
#
#
# def main():
#     with app.get_consumer() as consumer:
#         consumer.subscribe(["weather_data_demo"])
#
#         while True:
#             msg = consumer.poll(1)
#
#             if msg is None:
#                 print("Waiting...")
#             elif msg.error() is not None:
#                 raise Exception(msg.error())
#             else:
#                 key = msg.key()
#                 offset = msg.offset()
#                 value = msg.value()
#
#                 print(f"\nReceived message: {key} \t {value} at offset {offset}")
#
#                 consumer.store_offsets(msg)
#
#                 time.sleep(2)
#
#
# if __name__ == "__main__":
#     main()
