import argparse
import json
import time

import paho.mqtt.client as mqtt

from emulator import ScooterSensorEmulator


# Параметри MQTT-брокера
MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60

# Ідентифікатор електроскутера
OBJECT_ID = "scooter_01"

# Рівень гарантованої доставки MQTT
MQTT_QOS = 0

# Тема для повідомлень про стан джерела даних
STATUS_TOPIC = f"object/{OBJECT_ID}/status"

# Рівень QoS для повідомлень про стан
STATUS_QOS = 1


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Емулятор телеметрії електроскутера"
    )

    parser.add_argument(
        "--scenario",
        choices=["normal", "burst", "disconnect"],
        default="normal",
        help="Сценарій роботи publisher"
    )

    parser.add_argument(
        "--scenario-start",
        type=float,
        default=10.0,
        help="Час початку сценарію в секундах"
    )

    parser.add_argument(
        "--scenario-duration",
        type=float,
        default=5.0,
        help="Тривалість сценарію в секундах"
    )

    parser.add_argument(
        "--burst-factor",
        type=float,
        default=5.0,
        help="У скільки разів збільшити інтенсивність даних"
    )

    args = parser.parse_args()

    if args.scenario_start < 0:
        parser.error("--scenario-start не може бути від'ємним")

    if args.scenario_duration <= 0:
        parser.error("--scenario-duration має бути більшим за 0")

    if args.burst_factor <= 1:
        parser.error("--burst-factor має бути більшим за 1")

    return args


def publish_status(client, status):
    info = client.publish(
        STATUS_TOPIC,
        payload=status,
        qos=STATUS_QOS,
        retain=True
    )

    info.wait_for_publish(timeout=5)


def main():
    args = parse_arguments()

    emulator = ScooterSensorEmulator()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="scooter_publisher"
    )

    # Налаштування Last Will для нештатного від'єднання
    client.will_set(
        STATUS_TOPIC,
        payload="offline",
        qos=STATUS_QOS,
        retain=True
    )

    loop_started = False
    disconnected = False
    disconnect_until = None
    scenario_executed = False

    print("Підключення до MQTT-брокера...")

    try:
        client.connect(
            MQTT_HOST,
            MQTT_PORT,
            keepalive=MQTT_KEEPALIVE
        )

        client.loop_start()
        loop_started = True

        # Повідомлення про успішне підключення
        publish_status(client, "online")

        print("З'єднання з MQTT-брокером встановлено.")
        print(f"Сценарій: {args.scenario}")

        if args.scenario != "normal":
            print(
                f"Початок сценарію: {args.scenario_start} с"
            )
            print(
                f"Тривалість сценарію: "
                f"{args.scenario_duration} с"
            )

        if args.scenario == "burst":
            print(
                f"Збільшення інтенсивності: "
                f"{args.burst_factor} разів"
            )

        print("Публікація телеметрії електроскутера розпочалася.")
        print("Для завершення натисніть Ctrl+C.\n")

        start_time = time.monotonic()
        next_tick = start_time

        while True:
            current_time = time.monotonic()
            elapsed_time = current_time - start_time

            # Перевірка початку сценарію
            if (
                args.scenario != "normal"
                and not scenario_executed
                and elapsed_time >= args.scenario_start
            ):
                scenario_executed = True

                if args.scenario == "burst":
                    print(
                        "\n!!! ПОЧАТОК СПЛЕСКУ ІНТЕНСИВНОСТІ "
                        f"(у {args.burst_factor} разів)"
                    )

                elif args.scenario == "disconnect":
                    print(
                        "\n!!! ТИМЧАСОВЕ ВІДКЛЮЧЕННЯ "
                        "ВІД MQTT-БРОКЕРА"
                    )

                    # Повідомляємо підписника про відключення
                    publish_status(client, "offline")

                    client.disconnect()
                    client.loop_stop()
                    loop_started = False

                    disconnected = True
                    disconnect_until = (
                        time.monotonic()
                        + args.scenario_duration
                    )

            # Перевірка завершення тимчасового відключення
            if (
                disconnected
                and time.monotonic() >= disconnect_until
            ):
                print(
                    "\n!!! ВІДНОВЛЕННЯ З'ЄДНАННЯ "
                    "З MQTT-БРОКЕРОМ"
                )

                client.reconnect()
                client.loop_start()
                loop_started = True

                publish_status(client, "online")

                disconnected = False
                disconnect_until = None

                print("З'єднання з MQTT-брокером відновлено.")

                # Скидання таймера циклу
                next_tick = time.monotonic()

            # Оновлення фізичної моделі електроскутера
            current, voltage = emulator.update_physics(
                ScooterSensorEmulator.TICK
            )

            messages = emulator.generate_group(
                "10Hz", current, voltage
            )

            # Формування повідомлень із частотою 1 Гц
            if emulator.tick_counter % 10 == 0:
                messages.extend(
                    emulator.generate_group(
                        "1Hz", current, voltage
                    )
                )

            # Формування повідомлень із частотою 0,1 Гц
            if emulator.tick_counter % 100 == 0:
                messages.extend(
                    emulator.generate_group(
                        "0.1Hz", current, voltage
                    )
                )

            # Повідомлення про зміну дискретних станів
            messages.extend(
                emulator.generate_event_messages()
            )

            # Публікація повідомлень, якщо є з'єднання
            if not disconnected and client.is_connected():

                for message in messages:
                    topic = (
                        f"object/{OBJECT_ID}/sensor/"
                        f"{message['sensor_id']}"
                    )

                    payload = json.dumps(
                        message,
                        ensure_ascii=False
                    )

                    info = client.publish(
                        topic,
                        payload,
                        qos=MQTT_QOS
                    )

                    if info.rc == mqtt.MQTT_ERR_SUCCESS:
                        print(f"[MQTT] {topic} {payload}")
                    else:
                        print(
                            f"[ПОМИЛКА] Не вдалося опублікувати "
                            f"повідомлення: {topic}"
                        )

            # Відображення активної аномалії
            if emulator.anomaly_type:
                print(
                    f"!!! АНОМАЛІЯ: {emulator.anomaly_type} "
                    f"(t={emulator.elapsed:.1f} с)"
                )

            # Визначення інтервалу циклу
            tick_interval = ScooterSensorEmulator.TICK

            # Прискорення публікації під час сплеску
            if (
                args.scenario == "burst"
                and scenario_executed
                and elapsed_time < (
                    args.scenario_start
                    + args.scenario_duration
                )
            ):
                tick_interval = (
                    ScooterSensorEmulator.TICK
                    / args.burst_factor
                )

            # Підтримання заданого інтервалу циклу
            next_tick += tick_interval

            delay = next_tick - time.monotonic()

            if delay > 0:
                time.sleep(delay)
            else:
                next_tick = time.monotonic()

            # Повідомлення про завершення сплеску
            if (
                args.scenario == "burst"
                and scenario_executed
                and elapsed_time >= (
                    args.scenario_start
                    + args.scenario_duration
                )
            ):
                print(
                    "\n!!! СПЛЕСК ІНТЕНСИВНОСТІ ЗАВЕРШЕНО. "
                    "Повернення до звичайного режиму."
                )

                # Позначаємо сценарій як завершений
                args.scenario = "normal"

    except KeyboardInterrupt:
        print("\nПублікацію телеметрії зупинено.")

    except (OSError, ValueError) as error:
        print(f"Помилка MQTT-з'єднання: {error}")

    finally:
        if client.is_connected():
            try:
                publish_status(client, "offline")
                client.disconnect()
            except (OSError, ValueError):
                pass

        if loop_started:
            client.loop_stop()

        print("З'єднання з MQTT-брокером завершено.")


if __name__ == "__main__":
    main()