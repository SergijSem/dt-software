from datetime import datetime, timezone

class MissingDataHandler:
    """Утримання останніх значень та контроль їх актуальності."""

    FRESHNESS_LIMITS = {
        "SIG_VEL_01": 0.5,
        "SIG_RPM_02": 0.5,
        "SIG_CUR_03": 3.0,
        "SIG_VOL_04": 3.0,
        "SIG_TMP_05": 30.0,
        "SIG_SOC_06": 30.0
    }

    EVENT_SENSORS = {
        "SIG_BRK_07",
        "SIG_LCK_08"
    }

    def __init__(self):
        self.values = {}

    def update(self, data, timestamp):
        sensor_id = data["sensor_id"]

        self.values[sensor_id] = {
            "value": data["value"],
            "timestamp": timestamp
        }

    def get_snapshot(self, now=None):
        if now is None:
            now = datetime.now(timezone.utc)

        snapshot = {}

        all_sensors = set(self.FRESHNESS_LIMITS) | self.EVENT_SENSORS

        for sensor_id in all_sensors:
            item = self.values.get(sensor_id)

            if item is None:
                snapshot[sensor_id] = {
                    "value": None,
                    "timestamp": None,
                    "age_seconds": None,
                    "valid": False
                }
                continue

            age = max(
                0.0,
                (now - item["timestamp"]).total_seconds()
            )

            if sensor_id in self.EVENT_SENSORS:
                valid = True
            else:
                valid = (
                    age <= self.FRESHNESS_LIMITS[sensor_id]
                )

            snapshot[sensor_id] = {
                "value": item["value"],
                "timestamp": item["timestamp"].isoformat(),
                "age_seconds": round(age, 3),
                "valid": valid
            }

        return snapshot