import argparse
import json
import logging
import queue
import threading
import time

from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from message_buffer import MessageBuffer
from data_validator import DataValidator
from outlier_filter import OutlierFilter
from missing_data_handler import MissingDataHandler
from resampler import Resampler
from state_manager import StateManager
from state_synchronizer import StateSynchronizer
from database_manager import database_manager
from performance_monitor import PerformanceMonitor


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
connected_event = threading.Event()

# Лічильник отриманих повідомлень
received_messages = 0
received_lock = threading.Lock()


# Ініціалізація компонентів конвеєра
message_buffer = MessageBuffer(
    reorder_window=REORDER_WINDOW
)

data_validator = DataValidator()
outlier_filter = OutlierFilter()
missing_data_handler = MissingDataHandler()
resampler = Resampler()

# Компонент вимірювання продуктивності
performance_monitor = PerformanceMonitor()

# Менеджер актуального стану цифрового двійника
state_manager = StateManager(
    performance_monitor=performance_monitor
)

# Компонент синхронізації
state_synchronizer = StateSynchronizer(
    state_manager
)


def interface_subscriber(state):
    """Імітація підписника інтерфейсу."""

    print(
        "[SUBSCRIBER: INTERFACE] "
        "Отримано оновлення стану цифрового двійника."
    )


def analytics_subscriber(state):
    """Імітація підписника аналітичного модуля."""

    print(
        "[SUBSCRIBER: ANALYTICS] "
        "Отримано оновлення для аналізу телеметрії."
    )


def on_connect(client, userdata, flags, reason_code, properties):
    """Оброблення підключення до MQTT-брокера."""

    if reason_code == 0:
        print("З'єднання з MQTT-брокером встановлено.")

        client.subscribe(TOPIC, qos=0)
        client.subscribe(STATUS_TOPIC, qos=1)

        print(f"Підписка на телеметрію: {TOPIC}")
        print(f"Підписка на стан джерела: {STATUS_TOPIC}\n")

        connected_event.set()

    else:
        print(f"Помилка підключення: {reason_code}")


def on_message(client, userdata, message):
    """Отримання MQTT-повідомлення."""

    global received_messages

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

    # Рахуємо отримані телеметричні повідомлення.
    # Статусні повідомлення до цього лічильника не входять.
    with received_lock:
        received_messages += 1

    try:
        payload = message.payload.decode("utf-8")
        data = json.loads(payload)

        if not isinstance(data, dict):
            raise ValueError(
                "Повідомлення має бути JSON-об'єктом"
            )

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

        received_at = datetime.now(timezone.utc)

        item = {
            "topic": message.topic,
            "data": data,
            "parsed_timestamp": timestamp,
            "received_at": received_at.isoformat(),
            "received_monotonic": time.monotonic()
        }

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
    """Попереднє оброблення та перевірка правил синхронізації."""

    data = item["data"]
    timestamp = item["parsed_timestamp"]
    topic = item["topic"]

    # Запізнілі повідомлення не змінюють актуальний стан.
    if item["late"]:
        logging.warning(
            "Пропущено запізніле повідомлення: topic=%s",
            topic
        )
        return

    # Етап 1. Валідація повідомлення.
    valid, reason = data_validator.validate(
        data,
        topic
    )

    if not valid:
        print(f"[ПОМИЛКА ВАЛІДАЦІЇ] {reason}")

        logging.warning(
            "Відхилено повідомлення: topic=%s | reason=%s",
            topic,
            reason
        )
        return

    # Етап 2. Виявлення аномальних вимірювань.
    accepted, reason = outlier_filter.check(
        data,
        timestamp
    )

    if not accepted:
        print(f"[ВИКИД] {data['sensor_id']}: {reason}")

        logging.warning(
            "Відхилено викид: sensor=%s | reason=%s",
            data["sensor_id"],
            reason
        )
        return

    # Етап 3. Оновлення останнього достовірного значення.
    missing_data_handler.update(
        data,
        timestamp
    )

    # Збереження прийнятого вимірювання в InfluxDB.
    database_manager.write_telemetry(
        data,
        timestamp
    )

    # Отримання актуального зрізу телеметрії.
    snapshot = missing_data_handler.get_snapshot(
        datetime.now(timezone.utc)
    )

    # Етап 4. Перевірка синхронізації за подією або порогом.
    state_synchronizer.check_message(
        data,
        snapshot
    )

    # Реєстрація успішно обробленого повідомлення.
    performance_monitor.record_processed_message()

    logging.info(
        "Прийнято та оброблено: sensor=%s | value=%s",
        data["sensor_id"],
        data["value"]
    )


def emit_resampled_data():
    """Формування узгодженого набору даних із частотою 10 Гц."""

    sample = resampler.resample(
        missing_data_handler
    )

    telemetry = sample["telemetry"]

    speed = telemetry["SIG_VEL_01"]["value"]
    temperature = telemetry["SIG_TMP_05"]["value"]
    soc = telemetry["SIG_SOC_06"]["value"]

    print(
        f"[ЗРІЗ 10 Гц] "
        f"Швидкість: {speed} км/год | "
        f"Температура: {temperature} °C | "
        f"SoC: {soc} %"
    )

    logging.info(
        "resampled_state=%s",
        json.dumps(sample, ensure_ascii=False)
    )


def processing_worker():
    """Оброблення черги, буфера та синхронізація стану."""

    while not stop_event.is_set() or not incoming_queue.empty():

        try:
            item = incoming_queue.get(timeout=0.05)

            try:
                message_buffer.add(item)
            finally:
                incoming_queue.task_done()

        except queue.Empty:
            pass

        # Передавання готових повідомлень із буфера.
        for item in message_buffer.get_ready():
            process_message(item)

        # Отримання актуального зрізу телеметрії.
        snapshot = missing_data_handler.get_snapshot(
            datetime.now(timezone.utc)
        )

        # Етап 5. Періодична синхронізація стану.
        state_synchronizer.check_periodic(snapshot)

        # Формування узгодженого набору кожні 0,1 секунди.
        if resampler.is_due():
            emit_resampled_data()

    # Оброблення повідомлень, що залишилися в буфері.
    for item in message_buffer.get_ready(force=True):
        process_message(item)

    # Фінальний зріз після завершення приймання.
    emit_resampled_data()


def main():
    parser = argparse.ArgumentParser(
        description="Конвеєр оброблення телеметрії електроскутера"
    )

    parser.add_argument(
        "--measure",
        action="store_true",
        help="Увімкнути вимірювання продуктивності"
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=60,
        help="Тривалість роботи конвеєра у секундах"
    )

    args = parser.parse_args()

    if args.duration <= 0:
        parser.error("--duration має бути більшим за 0")

    measure_performance = args.measure

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

    print("Запуск конвеєра оброблення телеметрії...", flush=True)
    print("Режим: вимірювання продуктивності." if measure_performance
          else "Режим: звичайна робота.", flush=True)
    print(f"Тривалість роботи: {args.duration:.1f} с", flush=True)

    try:
        # Підключення до InfluxDB.
        database_manager.connect()

        # Реєстрація підписників.
        state_manager.subscribe(
            "interface",
            interface_subscriber
        )

        state_manager.subscribe(
            "analytics",
            analytics_subscriber
        )

        # Реєстрація підписника бази даних.
        state_manager.subscribe(
            "database",
            database_manager.write_twin_state
        )

        worker.start()

        client.connect(
            MQTT_HOST,
            MQTT_PORT,
            keepalive=MQTT_KEEPALIVE
        )

        client.loop_start()

        # Очікування успішного підключення та підписки.
        if not connected_event.wait(timeout=15):
            raise TimeoutError(
                "Не вдалося підключитися до MQTT-брокера за 15 секунд"
            )

        # Початок вимірювання після підключення.
        if measure_performance:
            performance_monitor.start()

        print(
            f"Приймання телеметрії протягом "
            f"{args.duration:.1f} с розпочато.",
            flush=True
        )

        # Відлік часу починається після підключення до брокера.
        end_time = time.monotonic() + args.duration

        while time.monotonic() < end_time:
            time.sleep(min(0.1, max(0, end_time - time.monotonic())))

    except KeyboardInterrupt:
        print("\nПриймання телеметрії достроково зупинено.")

    except (OSError, ValueError, TimeoutError) as error:
        print(f"Помилка роботи конвеєра: {error}")

    finally:
        # Спочатку припиняємо приймання нових повідомлень.
        if client.is_connected():
            client.disconnect()

        client.loop_stop()

        # Даємо worker завершити оброблення черги та буфера.
        stop_event.set()

        if worker.is_alive():
            worker.join()

        # Завершення вимірювання після оброблення черги.
        performance_results = None

        if measure_performance:
            performance_results = performance_monitor.stop()

        with received_lock:
            total_received = received_messages

        print("\n" + "=" * 55)
        print("СТАТИСТИКА ПІДПИСНИКА")
        print("=" * 55)
        print(f"Отримано повідомлень: {total_received}")

        if performance_results:
            print(
                "Оброблено повідомлень: "
                f"{performance_results['processed_messages']}"
            )

        print("=" * 55 + "\n")

        logging.info(
            "receiver_results | received=%d",
            total_received
        )

        # Закриття з'єднання з InfluxDB.
        database_manager.close()

        print("Конвеєр оброблення завершив роботу.")


if __name__ == "__main__":
    main()