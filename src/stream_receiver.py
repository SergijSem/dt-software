import json
import logging
import queue
import threading
import time

from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from message_buffer import MessageBuffer


# Параметри MQTT-брокера
MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60

# Ідентифікатор електроскутера
OBJECT_ID = "scooter_01"

# MQTT-теми
TOPIC = f"object/{OBJECT_ID}/sensor/#"
STATUS_TOPIC = f"object/{OBJECT_ID}/status"

# Часове вікно впорядкування, с
REORDER_WINDOW = 0.5


# Налаштування журналу
logging.basicConfig(
    filename="stream_receiver.log",
    level=logging.INFO,
    encoding="utf-8",
    format="%(asctime)s | %(message)s"
)


# Черга для передавання повідомлень між потоками
incoming_queue = queue.Queue()

stop_event = threading.Event()

message_buffer = MessageBuffer(
    reorder_window=REORDER_WINDOW
)


def on_connect(client, userdata, flags, reason_code, properties):
    """Оброблення підключення до MQTT-брокера."""

    if reason_code == 0:
        print("З'єднання з MQTT-брокером встановлено.")

        client.subscribe(TOPIC, qos=0)
        client.subscribe(STATUS_TOPIC, qos=1)

        print(f"Підписка на телеметрію: {TOPIC}")
        print(f"Підписка на стан джерела: {STATUS_TOPIC}\n")

    else:
        print(f"Помилка підключення: {reason_code}")


def on_message(client, userdata, message):
    """Отримання MQTT-повідомлення."""

    # Окреме оброблення статусу джерела
    if message.topic == STATUS_TOPIC:
        try:
            status = message.payload.decode("utf-8")

            print(f"[СТАН ДЖЕРЕЛА] {status}")

            logging.info(
                "connection_status=%s",
                status
            )

        except UnicodeDecodeError as error:
            logging.warning(
                "Некоректне статусне повідомлення: %s",
                error
            )

        return

    try:
        # Декодування JSON
        payload = message.payload.decode("utf-8")
        data = json.loads(payload)

        if not isinstance(data, dict):
            raise ValueError(
                "Повідомлення має бути JSON-об'єктом"
            )

        # Отримання часової мітки генерації
        if not isinstance(data.get("timestamp"), str):
            raise ValueError(
                "Відсутня коректна часова мітка timestamp"
            )

        timestamp = datetime.fromisoformat(
            data["timestamp"].replace("Z", "+00:00")
        )

        if timestamp.tzinfo is None:
            raise ValueError(
                "Часова мітка повинна містити часовий пояс"
            )

        timestamp = timestamp.astimezone(timezone.utc)

        # Фіксація часу отримання повідомлення
        received_at = datetime.now(timezone.utc)

        item = {
            "topic": message.topic,
            "data": data,
            "parsed_timestamp": timestamp,
            "received_at": received_at.isoformat(),
            "received_monotonic": time.monotonic()
        }

        # Передавання повідомлення до черги
        incoming_queue.put(item)

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValueError,
        AttributeError
    ) as error:

        print(f"[ПОМИЛКА ПРИЙМАННЯ] {error}")

        logging.warning(
            "Некоректне повідомлення з топіка %s: %s",
            message.topic,
            error
        )


def process_message(item):
    """Передавання впорядкованого повідомлення далі."""

    data = dict(item["data"])

    # Додавання часу отримання
    data["received_at"] = item["received_at"]

    print(f"\n[ОТРИМАНО] {item['topic']}")

    print(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2
        )
    )

    if item["late"]:
        print("[УВАГА] Повідомлення надійшло із запізненням.")

    logging.info(
        "topic=%s | late=%s | data=%s",
        item["topic"],
        item["late"],
        json.dumps(data, ensure_ascii=False)
    )


def processing_worker():
    """Оброблення черги та впорядкування повідомлень."""

    while not stop_event.is_set() or not incoming_queue.empty():

        try:
            item = incoming_queue.get(timeout=0.1)

            try:
                message_buffer.add(item)
            finally:
                incoming_queue.task_done()

        except queue.Empty:
            pass

        for item in message_buffer.get_ready():
            process_message(item)

    # Передавання решти повідомлень із буфера
    for item in message_buffer.get_ready(force=True):
        process_message(item)


def main():

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="scooter_stream_receiver"
    )

    client.on_connect = on_connect
    client.on_message = on_message

    worker = threading.Thread(
        target=processing_worker,
        daemon=True
    )

    print("Запуск компонента приймання телеметрії...")

    worker.start()

    try:
        client.connect(
            MQTT_HOST,
            MQTT_PORT,
            keepalive=MQTT_KEEPALIVE
        )

        client.loop_forever()

    except KeyboardInterrupt:
        print("\nПриймання телеметрії зупинено.")

    except (OSError, ValueError) as error:
        print(f"Помилка MQTT-з'єднання: {error}")

    finally:
        if client.is_connected():
            client.disconnect()

        stop_event.set()
        worker.join()

        print("Компонент приймання завершив роботу.")


if __name__ == "__main__":
    main()
