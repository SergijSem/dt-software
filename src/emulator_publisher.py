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

def main():
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

    print("Підключення до MQTT-брокера...")

    try:
        client.connect(
            MQTT_HOST,
            MQTT_PORT,
            keepalive=MQTT_KEEPALIVE
        )
        client.loop_start()

        # Повідомлення про успішне підключення джерела даних
        info = client.publish(
            STATUS_TOPIC,
            payload="online",
            qos=STATUS_QOS,
            retain=True
        )
        info.wait_for_publish()

        print("З'єднання з MQTT-брокером встановлено.")
        print("Публікація телеметрії електроскутера розпочалася.")
        print("Для завершення натисніть Ctrl+C.\n")

        next_tick = time.monotonic()

        while True:
            # Оновлення моделі електроскутера
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

            # Публікація кожного повідомлення через MQTT
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

            # Підтримання базового інтервалу 0,1 с
            next_tick += ScooterSensorEmulator.TICK
            delay = next_tick - time.monotonic()

            if delay > 0:
                time.sleep(delay)
            else:
                # Не накопичуємо відставання циклу
                next_tick = time.monotonic()

    except KeyboardInterrupt:
        print("\nПублікацію телеметрії зупинено.")

    except (OSError, ValueError) as error:
        print(f"Помилка MQTT-з'єднання: {error}")

    finally:
        if client.is_connected():
            client.disconnect()

        client.loop_stop()
        print("З'єднання з MQTT-брокером завершено.")


if __name__ == "__main__":
    main()