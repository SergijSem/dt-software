from datetime import datetime, timezone
import math
import random
import time
import json


class ScooterSensorEmulator:
    """Емулятор телеметрії електроскутера."""

    TICK = 0.1                  # Базовий крок: 10 Гц
    ANOMALY_PERIOD = 60.0       # Період повторення аномалій, с
    ANOMALY_DURATION = 8.0      # Тривалість аномалії, с
    BATTERY_CAPACITY_AH = 20.0  # Умовна ємність батареї, А·год

    def __init__(self):
        self.elapsed = 0.0
        self.state_elapsed = 0.0
        self.trip_state = "idle"

        self.speed = 0.0
        self.motor_temp = 25.0
        self.soc = 90.0

        self.brake = False
        self.anti_theft = False

        self.previous_brake = self.brake
        self.previous_anti_theft = self.anti_theft

        self.tick_counter = 0
        self.anomaly_type = None

    def timestamp(self):
        return datetime.now(timezone.utc).isoformat()

    def ambient_temperature(self):
        """Добовий цикл температури навколишнього середовища."""
        now = datetime.now()
        hour = now.hour + now.minute / 60 + now.second / 3600

        # Мінімум близько 02:00, максимум близько 14:00.
        return 18.0 + 8.0 * math.cos(
            2 * math.pi * (hour - 14.0) / 24.0
        )

    def update_trip_state(self, dt):
        """Модель фаз поїздки."""
        self.state_elapsed += dt

        durations = {
            "idle": 5.0,          # Простій
            "accelerating": 8.0,  # Розгін
            "cruise": 15.0,       # Рух із сталою швидкістю
            "braking": 6.0,       # Гальмування
        }

        if self.state_elapsed >= durations[self.trip_state]:
            self.state_elapsed = 0.0

            transitions = {
                "idle": "accelerating",
                "accelerating": "cruise",
                "cruise": "braking",
                "braking": (
                    "idle" if random.random() < 0.5
                    else "accelerating"
                ),
            }
            self.trip_state = transitions[self.trip_state]

    def update_physics(self, dt):
        self.elapsed += dt
        self.tick_counter += 1

        self.update_trip_state(dt)

        # Швидкість: розгін, рух, гальмування або стоянка.
        if self.trip_state == "accelerating":
            self.speed = min(
                50.0, self.speed + random.uniform(1.5, 3.0) * dt
            )
        elif self.trip_state == "cruise":
            self.speed += random.uniform(-0.5, 0.5) * dt
            self.speed = max(25.0, min(50.0, self.speed))
        elif self.trip_state == "braking":
            self.speed = max(
                0.0, self.speed - random.uniform(3.0, 6.0) * dt
            )
        else:
            self.speed = 0.0

        self.brake = self.trip_state == "braking"

        # Протиугінне блокування активується під час стоянки.
        self.anti_theft = (
            self.trip_state == "idle"
            and self.state_elapsed >= 3.0
        )

        # Аномалії повторюються за заданим часовим сценарієм.
        phase = self.elapsed % self.ANOMALY_PERIOD

        if phase < self.ANOMALY_DURATION:
            anomaly_number = int(
                self.elapsed // self.ANOMALY_PERIOD
            ) % 3
            self.anomaly_type = [
               "overheat",      # Перегрів двигуна
                "overcurrent",   # Надмірний струм акумулятора
                "voltage_drop",  # Різке падіння напруги акумулятора
            ][anomaly_number]
        else:
            self.anomaly_type = None

        # Струм залежить від режиму руху.
        current = {
            "idle": 0.2,
            "accelerating": 22.0,
            "cruise": 9.0,
            "braking": 0.5,
        }[self.trip_state]

        current += random.uniform(-0.3, 0.3)
        current = max(0.0, current)

        # Температура залежить від середовища та навантаження.
        ambient = self.ambient_temperature()
        target_temp = (
            ambient
            + 5.0
            + self.speed * 0.65
            + current * 0.25
        )

        # Теплова інерція двигуна.
        self.motor_temp += (
            (target_temp - self.motor_temp) * 0.015 * dt
        )

        # Розряд батареї за струмом та умовною ємністю.
        self.soc = max(
            0.0,
            self.soc
            - current / (self.BATTERY_CAPACITY_AH * 3600) * 100 * dt,
        )

        # Напруга залежить від заряду та навантаження.
        voltage = (
            36.0
            + 16.0 * self.soc / 100.0
            - current * 0.06
        )
        voltage = max(36.0, min(54.6, voltage))

        # Окремі сценарії аномалій.
        if self.anomaly_type == "overheat":
            self.motor_temp = min(120.0, self.motor_temp + 35.0)

        elif self.anomaly_type == "overcurrent":
            current = min(50.0, current + 28.0)

        elif self.anomaly_type == "voltage_drop":
            voltage = max(36.0, voltage - 8.0)

        return current, voltage

    def make_message(self, sensor_id, name, value, unit):
        return {
            "sensor_id": sensor_id,
            "name": name,
            "value": value,
            "unit": unit,
            "timestamp": self.timestamp(),
        }

    def generate_group(self, group, current, voltage):
        """Формування повідомлень із заданою частотою."""
        speed = max(
            0.0, self.speed + random.uniform(-0.1, 0.1)
        )
        rpm = speed * 55.0 + random.uniform(-2.0, 2.0)
        rpm = max(0.0, rpm)

        if group == "10Hz":
            return [
                self.make_message(
                    "SIG_VEL_01", "Швидкість",
                    round(speed, 2), "км/год"
                ),
                self.make_message(
                    "SIG_RPM_02", "Оберти двигуна",
                    round(rpm, 1), "об/хв"
                ),
            ]

        if group == "1Hz":
            return [
                self.make_message(
                    "SIG_CUR_03", "Струм батареї",
                    round(current, 2), "А"
                ),
                self.make_message(
                    "SIG_VOL_04", "Напруга батареї",
                    round(voltage, 2), "В"
                ),
            ]

        if group == "0.1Hz":
            temp = self.motor_temp + random.uniform(-0.2, 0.2)
            return [
                self.make_message(
                    "SIG_TMP_05", "Температура двигуна",
                    round(max(-20.0, min(120.0, temp)), 1), "°C"
                ),
                self.make_message(
                    "SIG_SOC_06", "Заряд батареї",
                    round(self.soc, 2), "%"
                ),
            ]

        return []

    def generate_event_messages(self):
        """Повідомлення лише при зміні дискретного стану."""
        messages = []

        if self.brake != self.previous_brake:
            messages.append(
                self.make_message(
                    "SIG_BRK_07", "Стан гальмування",
                    self.brake, "bool"
                )
            )
            self.previous_brake = self.brake

        if self.anti_theft != self.previous_anti_theft:
            messages.append(
                self.make_message(
                    "SIG_LCK_08", "Протиугінне блокування",
                    self.anti_theft, "bool"
                )
            )
            self.previous_anti_theft = self.anti_theft

        return messages


def main():
    emulator = ScooterSensorEmulator()

    print("Емулятор сенсорів електроскутера запущено.")
    print("Частоти: 10 Гц, 1 Гц, 0.1 Гц та подієві сигнали.")
    print("Для завершення натисніть Ctrl+C.\n")

    next_tick = time.monotonic()

    try:
        while True:
            current, voltage = emulator.update_physics(
                ScooterSensorEmulator.TICK
            )

            messages = emulator.generate_group(
                "10Hz", current, voltage
            )

            if emulator.tick_counter % 10 == 0:
                messages.extend(
                    emulator.generate_group("1Hz", current, voltage)
                )

            if emulator.tick_counter % 100 == 0:
                messages.extend(
                    emulator.generate_group(
                        "0.1Hz", current, voltage
                    )
                )

            messages.extend(emulator.generate_event_messages())

            for message in messages:
                print(json.dumps(message, ensure_ascii=False))

            if emulator.anomaly_type:
                print(
                    f"!!! АНОМАЛІЯ: {emulator.anomaly_type} "
                    f"(t={emulator.elapsed:.1f} с)"
                )

            next_tick += ScooterSensorEmulator.TICK
            delay = next_tick - time.monotonic()

            if delay > 0:
                time.sleep(delay)
            else:
                # Не намагаємося наздоганяти пропущені такти
                # нескінченним циклом.
                next_tick = time.monotonic()

    except KeyboardInterrupt:
        print("\nЕмуляцію зупинено.")


if __name__ == "__main__":
    main()