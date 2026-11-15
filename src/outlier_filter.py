class OutlierFilter:
    """Виявлення різких змін телеметричних показників."""

    MAX_CHANGE_RATE = {
        "SIG_VEL_01": 12.0,   # км/год за секунду
        "SIG_RPM_02": 700.0,  # об/хв за секунду
        "SIG_CUR_03": 40.0,
        "SIG_VOL_04": 15.0,
        "SIG_TMP_05": 8.0,
        "SIG_SOC_06": 2.0
    }

    CONFIRMATION_COUNT = 3

    def __init__(self):
        self.previous = {}
        self.candidates = {}

    def check(self, data, timestamp):
        sensor_id = data["sensor_id"]
        value = data["value"]

        if sensor_id not in self.MAX_CHANGE_RATE:
            return True, "OK"

        previous = self.previous.get(sensor_id)

        if previous is None:
            self.previous[sensor_id] = (value, timestamp)
            return True, "OK"

        previous_value, previous_timestamp = previous
        elapsed = (timestamp - previous_timestamp).total_seconds()

        if elapsed <= 0:
            return False, "Неактуальна або повторна часова мітка"

        max_rate = self.MAX_CHANGE_RATE[sensor_id]
        allowed_change = max_rate * elapsed
        change = abs(value - previous_value)

        # Вимірювання відповідає допустимій швидкості зміни.
        if change <= allowed_change:
            self.previous[sensor_id] = (value, timestamp)
            self.candidates.pop(sensor_id, None)
            return True, "OK"

        # Перевірка послідовності кандидатів на новий рівень.
        candidate = self.candidates.get(sensor_id)

        if candidate is None:
            self.candidates[sensor_id] = {
                "value": value,
                "timestamp": timestamp,
                "count": 1
            }
            return False, (
                f"Надмірна зміна значення: {change:.2f}; "
                f"допустима: {allowed_change:.2f}; "
                "очікується підтвердження"
            )

        candidate_elapsed = (
            timestamp - candidate["timestamp"]
        ).total_seconds()

        if candidate_elapsed <= 0:
            self.candidates.pop(sensor_id, None)
            return False, "Неактуальна часова мітка кандидата"

        candidate_allowed = max_rate * candidate_elapsed
        candidate_change = abs(value - candidate["value"])

        if candidate_change <= candidate_allowed:
            candidate["value"] = value
            candidate["timestamp"] = timestamp
            candidate["count"] += 1

            if candidate["count"] >= self.CONFIRMATION_COUNT:
                self.previous[sensor_id] = (value, timestamp)
                self.candidates.pop(sensor_id, None)
                return True, "Новий рівень підтверджено"

            return False, (
                f"Очікується підтвердження нового рівня: "
                f"{candidate['count']}/{self.CONFIRMATION_COUNT}"
            )

        # Послідовність не підтвердилася — починаємо її заново.
        self.candidates[sensor_id] = {
            "value": value,
            "timestamp": timestamp,
            "count": 1
        }

        return False, (
            f"Непідтверджена зміна: {change:.2f}; "
            f"допустима: {allowed_change:.2f}"
        )