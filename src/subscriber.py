import json
import logging
import sys

import paho.mqtt.client as mqtt


MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60

TOPIC = "object/scooter_01/sensor/#"

STATUS_TOPIC = "object/scooter_01/status"

REQUIRED_FIELDS = {
    "sensor_id",
    "name",
    "value",
    "unit",
    "timestamp"
}


# Налаштування журналу отриманих повідомлень
logging.basicConfig(
    filename="telemetry.log",
    level=logging.INFO,
    encoding="utf-8",
    format="%(asctime)s | %(message)s"
)


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("З'єднання з MQTT-брокером встановлено.")

        client.subscribe(TOPIC, qos=2)
        client.subscribe(STATUS_TOPIC, qos=1)

        print(f"Підписка на телеметрію: {TOPIC}\n")
        print(f"Підписка на стан джерела даних: {STATUS_TOPIC}\n")
    else:
        print(f"Помилка підключення: {reason_code}")


def on_message(client, userdata, message):
    # Окреме оброблення повідомлень про стан джерела
    if message.topic == STATUS_TOPIC:
        status = message.payload.decode("utf-8")
        print(f"[СТАН ДЖЕРЕЛА ДАНИХ] {status}")
        return

    try:
        # Декодування отриманого повідомлення
        payload = message.payload.decode("utf-8")
        data = json.loads(payload)

        # Перевірка типу повідомлення
        if not isinstance(data, dict):
            raise ValueError("Повідомлення має бути JSON-об'єктом")

        # Перевірка наявності обов'язкових полів
        missing_fields = REQUIRED_FIELDS - data.keys()

        if missing_fields:
            raise ValueError(
                f"Відсутні поля: {', '.join(sorted(missing_fields))}"
            )

        # Перевірка базових типів даних
        if not isinstance(data["sensor_id"], str):
            raise ValueError("sensor_id має бути рядком")

        if not isinstance(data["name"], str):
            raise ValueError("name має бути рядком")

        if not isinstance(data["unit"], str):
            raise ValueError("unit має бути рядком")

        if not isinstance(data["value"], (int, float)) or isinstance(
            data["value"], bool
        ):
            raise ValueError("value має бути числом")

        if not isinstance(data["timestamp"], str):
            raise ValueError("timestamp має бути рядком")

        # Виведення валідованого повідомлення
        print(f"Топік: {message.topic}")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        print("-" * 50)

        # Запис у журнал
        logging.info(
            "topic=%s | data=%s",
            message.topic,
            json.dumps(data, ensure_ascii=False)
        )

    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        print(f"[ПОМИЛКА ВАЛІДАЦІЇ] {error}")
        logging.warning(
            "Некоректне повідомлення з топіка %s: %s",
            message.topic,
            error
        )


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="scooter_subscriber"
    )

    client.on_connect = on_connect
    client.on_message = on_message

    print("Підключення до MQTT-брокера...")

    try:
        client.connect(
            MQTT_HOST,
            MQTT_PORT,
            keepalive=MQTT_KEEPALIVE
        )
        client.loop_forever()

    except KeyboardInterrupt:
        print("\nРоботу підписника завершено.")

    except (OSError, ValueError) as error:
        print(f"Помилка MQTT-з'єднання: {error}")

    finally:
        client.disconnect()


if __name__ == "__main__":
    main()