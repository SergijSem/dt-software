class OutlierFilter:
    """Виявлення неправдоподібних змін телеметричних показників."""

    # Максимальна допустима швидкість зміни параметра за секунду.
    MAX_CHANGE_RATE = {
        "SIG_VEL_01": 12.0,   # км/год за секунду
        "SIG_RPM_02": 700.0,  # об/хв за секунду
        "SIG_CUR_03": 40.0,   # А за секунду
        "SIG_VOL_04": 15.0,   # В за секунду
        "SIG_TMP_05": 8.0,    # °C за секунду
        "SIG_SOC_06": 2.0     # % за секунду
    }

    def __init__(self):
        self.previous = {}

    def check(self, data, timestamp):
        sensor_id = data["sensor_id"]
        value = data["value"]

        # Подієві сигнали не проходять числову фільтрацію.
        if sensor_id not in self.MAX_CHANGE_RATE:
            return True, "OK"

        previous = self.previous.get(sensor_id)

        if previous is None:
            self.previous[sensor_id] = (value, timestamp)
            return True, "OK"

        previous_value, previous_timestamp = previous

        elapsed = (
            timestamp - previous_timestamp
        ).total_seconds()

        if elapsed <= 0:
            return False, "Неактуальна або повторна часова мітка"

        change = abs(value - previous_value)
        allowed_change = (
            self.MAX_CHANGE_RATE[sensor_id] * elapsed
        )

        if change > allowed_change:
            return False, (
                f"Надмірна зміна значення: {change:.2f}; "
                f"допустима: {allowed_change:.2f}"
            )

        self.previous[sensor_id] = (value, timestamp)

        return True, "OK"