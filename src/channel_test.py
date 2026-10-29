import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt


MQTT_HOST = "localhost"
MQTT_PORT = 1883
OBJECT_ID = "scooter_01"
TEST_DURATION = 60
TOPIC = f"object/{OBJECT_ID}/sensor/#"

PERIODIC_SENSORS = {
    "SIG_VEL_01": ("Швидкість", 10),
    "SIG_RPM_02": ("Оберти двигуна", 10),
    "SIG_CUR_03": ("Струм батареї", 1),
    "SIG_VOL_04": ("Напруга батареї", 1),
    "SIG_TMP_05": ("Температура двигуна", 0.1),
    "SIG_SOC_06": ("Заряд батареї", 0.1),
}

DISCRETE_SENSORS = {
    "SIG_BRK_07": "Стан гальмування",
    "SIG_LCK_08": "Протиугінне блокування",
}

ALL_SENSORS = set(PERIODIC_SENSORS) | set(DISCRETE_SENSORS)
REQUIRED_FIELDS = {"sensor_id", "name", "value", "unit", "timestamp"}

first_message = threading.Event()
subscription_ready = threading.Event()
start_time = None

counts = dict.fromkeys(ALL_SENSORS, 0)
latencies = []
invalid = 0
unknown = 0


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        client.subscribe(TOPIC, qos=0)
        print("Підключення до MQTT-брокера успішне.")
    else:
        print(f"Помилка підключення: {reason_code}")


def on_subscribe(client, userdata, mid, reason_codes, properties):
    if not any(code.is_failure for code in reason_codes):
        subscription_ready.set()
        print(f"Підписка активна: {TOPIC}")


def on_message(client, userdata, message):
    global start_time, invalid, unknown

    now = time.monotonic()
    if start_time is None:
        start_time = now
        first_message.set()

    if now >= start_time + TEST_DURATION:
        return

    try:
        data = json.loads(message.payload.decode("utf-8"))
        if not isinstance(data, dict) or not REQUIRED_FIELDS <= data.keys():
            raise ValueError("Некоректна структура повідомлення")

        sensor_id = data["sensor_id"]
        if sensor_id not in ALL_SENSORS:
            unknown += 1
            return

        if message.topic != f"object/{OBJECT_ID}/sensor/{sensor_id}":
            raise ValueError("Невідповідність MQTT-топіку")

        value = data["value"]
        if sensor_id in DISCRETE_SENSORS:
            valid_value = isinstance(value, bool)
        else:
            valid_value = (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
            )

        if not valid_value:
            raise ValueError("Некоректний тип значення сенсора")

        timestamp = datetime.fromisoformat(
            data["timestamp"].replace("Z", "+00:00")
        )
        if timestamp.tzinfo is None:
            raise ValueError("Відсутній часовий пояс")

        latency = (
            datetime.now(timestamp.tzinfo) - timestamp
        ).total_seconds() * 1000

        if latency < -1000:
            raise ValueError("Некоректна часова мітка")

        latencies.append(max(0, latency))
        counts[sensor_id] += 1

    except (ValueError, TypeError, KeyError, UnicodeDecodeError) as error:
        invalid += 1
        print(f"[ПОМИЛКА] {error}")


def print_results():
    expected = sum(round(freq * TEST_DURATION)
                   for _, freq in PERIODIC_SENSORS.values())
    actual = sum(counts[sid] for sid in PERIODIC_SENSORS)
    missing = max(0, expected - actual)

    print("\n========== РЕЗУЛЬТАТИ ТЕСТУ ==========")
    print(f"Тривалість тесту: {TEST_DURATION} с")
    print(f"Очікувані періодичні повідомлення: {expected}")
    print(f"Отримані періодичні повідомлення: {actual}")
    print(f"Оцінка нестачі повідомлень: {missing}")
    print(f"Оцінка втрат: {missing / expected * 100:.3f}%")
    print(f"Некоректні повідомлення: {invalid}")
    print(f"Невідомі сенсори: {unknown}")

    if latencies:
        ordered = sorted(latencies)
        print(f"Середня затримка: {sum(latencies) / len(latencies):.3f} мс")
        print(f"Максимальна затримка: {max(latencies):.3f} мс")
        print(f"95-й процентиль: {ordered[round((len(ordered)-1)*0.95)]:.3f} мс")

    print("\nПовідомлення за сенсорами:")
    for sid, (name, freq) in PERIODIC_SENSORS.items():
        print(f"{sid} ({name}): {counts[sid]}/{round(freq * TEST_DURATION)}")

    for sid, name in DISCRETE_SENSORS.items():
        print(f"{sid} ({name}): {counts[sid]} повідомлень")

    print("\nПримітка: оцінка втрат приблизна; MQTT QoS 0 не підтверджує доставку.")


def main():
    global start_time

    folder = os.path.dirname(os.path.abspath(__file__))
    publisher_path = os.path.join(folder, "emulator_publisher.py")

    if not os.path.isfile(publisher_path):
        print("Не знайдено emulator_publisher.py.")
        return

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="scooter_channel_test",
    )
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message

    publisher = None
    completed = False

    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()

        if not subscription_ready.wait(10):
            raise TimeoutError("Не вдалося оформити підписку")

        publisher = subprocess.Popen(
            [sys.executable, publisher_path],
            cwd=folder,
            stdout=subprocess.DEVNULL,
        )

        print(f"Очікування першого повідомлення. Тест: {TEST_DURATION} с.")
        if not first_message.wait(15):
            raise TimeoutError("Телеметрію не отримано")

        deadline = start_time + TEST_DURATION
        while time.monotonic() < deadline:
            if publisher.poll() is not None:
                raise RuntimeError("Емулятор передчасно завершився")
            time.sleep(0.1)

        completed = True

    except (OSError, TimeoutError, RuntimeError) as error:
        print(f"[ПОМИЛКА ТЕСТУ] {error}")

    finally:
        if publisher is not None:
            if publisher.poll() is None:
                publisher.terminate()
            try:
                publisher.wait(timeout=5)
            except subprocess.TimeoutExpired:
                publisher.kill()
                publisher.wait()

        if client.is_connected():
            client.disconnect()
        client.loop_stop()

    if any(counts.values()):
        print_results()

    print("Тест завершено." if completed else "Тест завершився передчасно.")


if __name__ == "__main__":
    main()