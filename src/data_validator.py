import math
from datetime import datetime


SENSORS = {
    "SIG_VEL_01": {
        "name": "Швидкість",
        "unit": "км/год",
        "type": "number",
        "min": 0.0,
        "max": 50.0
    },
    "SIG_RPM_02": {
        "name": "Оберти двигуна",
        "unit": "об/хв",
        "type": "number",
        "min": 0.0,
        "max": 2752.0
    },
    "SIG_CUR_03": {
        "name": "Струм батареї",
        "unit": "А",
        "type": "number",
        "min": 0.0,
        "max": 50.0
    },
    "SIG_VOL_04": {
        "name": "Напруга батареї",
        "unit": "В",
        "type": "number",
        "min": 36.0,
        "max": 54.6
    },
    "SIG_TMP_05": {
        "name": "Температура двигуна",
        "unit": "°C",
        "type": "number",
        "min": -20.0,
        "max": 120.0
    },
    "SIG_SOC_06": {
        "name": "Заряд батареї",
        "unit": "%",
        "type": "number",
        "min": 0.0,
        "max": 100.0
    },
    "SIG_BRK_07": {
        "name": "Стан гальмування",
        "unit": "bool",
        "type": "boolean"
    },
    "SIG_LCK_08": {
        "name": "Протиугінне блокування",
        "unit": "bool",
        "type": "boolean"
    }
}


REQUIRED_FIELDS = {
    "sensor_id",
    "name",
    "value",
    "unit",
    "timestamp"
}


class DataValidator:
    """Перевірка коректності телеметричних повідомлень."""

    def validate(self, data, topic=None):
        if not isinstance(data, dict):
            return False, "Повідомлення має бути JSON-об'єктом"

        missing = REQUIRED_FIELDS - data.keys()

        if missing:
            return False, f"Відсутні поля: {sorted(missing)}"

        sensor_id = data["sensor_id"]

        if not isinstance(sensor_id, str):
            return False, "sensor_id має бути рядком"

        if sensor_id not in SENSORS:
            return False, f"Невідомий сенсор: {sensor_id}"

        # Перевірка відповідності сенсора MQTT-топіку.
        if topic is not None:
            expected_suffix = f"/sensor/{sensor_id}"

            if not topic.endswith(expected_suffix):
                return False, "Ідентифікатор сенсора не відповідає топіку"

        config = SENSORS[sensor_id]

        if data["name"] != config["name"]:
            return False, "Невідповідна назва сенсора"

        if data["unit"] != config["unit"]:
            return False, "Невідповідна одиниця вимірювання"

        value = data["value"]

        if config["type"] == "number":
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
            ):
                return False, "Значення сенсора має бути числом"

            if not math.isfinite(value):
                return False, "Значення не є скінченним числом"

            if not config["min"] <= value <= config["max"]:
                return False, (
                    f"Значення {value} поза допустимим діапазоном "
                    f"[{config['min']}; {config['max']}]"
                )

        elif config["type"] == "boolean":
            if not isinstance(value, bool):
                return False, "Значення сенсора має бути bool"

        if not isinstance(data["timestamp"], str):
            return False, "timestamp має бути рядком"

        try:
            timestamp = datetime.fromisoformat(
                data["timestamp"].replace("Z", "+00:00")
            )

            if timestamp.tzinfo is None:
                return False, "timestamp повинен містити часовий пояс"

        except ValueError:
            return False, "Некоректний формат timestamp"

        return True, "OK"