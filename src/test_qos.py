import json
import re
import subprocess
import sys
import time
from pathlib import Path


# Файли проєкту
BASE_DIR = Path(__file__).resolve().parent
PUBLISHER_FILE = BASE_DIR / "emulator_publisher.py"
SUBSCRIBER_FILE = BASE_DIR / "subscriber.py"
LOG_FILE = BASE_DIR / "telemetry.log"
RESULT_FILE = BASE_DIR / "qos_results.json"

# Параметри тестування
TEST_DURATION = 30
QOS_LEVELS = [0, 1, 2]


def set_publisher_qos(qos):
    """Змінює MQTT_QOS у файлі публікатора."""
    source = PUBLISHER_FILE.read_text(encoding="utf-8")

    updated, count = re.subn(
        r"^MQTT_QOS\s*=\s*\d+\s*$",
        f"MQTT_QOS = {qos}",
        source,
        count=1,
        flags=re.MULTILINE
    )

    if count != 1:
        raise RuntimeError(
            "Не вдалося знайти MQTT_QOS у emulator_publisher.py"
        )

    PUBLISHER_FILE.write_text(updated, encoding="utf-8")


def count_received_messages():
    """Рахує валідні повідомлення в журналі підписника."""
    if not LOG_FILE.exists():
        return 0

    text = LOG_FILE.read_text(encoding="utf-8", errors="replace")

    return len(re.findall(r"\|\s*topic=object/scooter_01/sensor/", text))


def run_test(qos):
    print(f"\n{'=' * 45}")
    print(f"ТЕСТ QoS {qos}")
    print(f"{'=' * 45}")

    # Очищення журналу попереднього запуску
    LOG_FILE.write_text("", encoding="utf-8")

    set_publisher_qos(qos)

    subscriber = subprocess.Popen(
        [sys.executable, "-u", str(SUBSCRIBER_FILE)],
        cwd=BASE_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT
    )

    publisher = None

    try:
        # Час на запуск підписника та підключення до брокера
        time.sleep(3)

        if subscriber.poll() is not None:
            raise RuntimeError(
                "Підписник завершився. Перевір Mosquitto та subscriber.py."
            )

        publisher = subprocess.Popen(
            [sys.executable, "-u", str(PUBLISHER_FILE)],
            cwd=BASE_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT
        )

        print(f"Публікація телеметрії протягом {TEST_DURATION} с...")

        time.sleep(TEST_DURATION)

        # Зупиняємо публікатор
        publisher.terminate()

        try:
            publisher.wait(timeout=5)
        except subprocess.TimeoutExpired:
            publisher.kill()
            publisher.wait()

        # Даємо підписнику час обробити останні повідомлення
        time.sleep(3)

        received = count_received_messages()

        result = {
            "qos": qos,
            "duration_seconds": TEST_DURATION,
            "received_messages": received
        }

        print(f"QoS: {qos}")
        print(f"Отримано валідних повідомлень: {received}")

        return result

    finally:
        if publisher is not None and publisher.poll() is None:
            publisher.terminate()
            try:
                publisher.wait(timeout=5)
            except subprocess.TimeoutExpired:
                publisher.kill()
                publisher.wait()

        if subscriber.poll() is None:
            subscriber.terminate()
            try:
                subscriber.wait(timeout=5)
            except subprocess.TimeoutExpired:
                subscriber.kill()
                subscriber.wait()


def main():
    original_source = PUBLISHER_FILE.read_text(encoding="utf-8")
    results = []

    try:
        for qos in QOS_LEVELS:
            results.append(run_test(qos))

    finally:
        # Повертаємо початковий код публікатора
        PUBLISHER_FILE.write_text(original_source, encoding="utf-8")

    RESULT_FILE.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"\n{'=' * 45}")
    print("ПІДСУМКИ")
    print(f"{'=' * 45}")

    for result in results:
        print(
            f"QoS {result['qos']}: "
            f"{result['received_messages']} отриманих повідомлень"
        )

    print(f"\nРезультати збережено у {RESULT_FILE.name}")
    print("Початковий код публікатора відновлено.")


if __name__ == "__main__":
    main()