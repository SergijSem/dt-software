import time
from datetime import datetime, timezone

class Resampler:
    """Формування узгоджених наборів даних із частотою 10 Гц."""

    INTERVAL = 0.1

    def __init__(self):
        self.next_tick = time.monotonic()

    def is_due(self):
        now = time.monotonic()

        if now < self.next_tick:
            return False

        # Не накопичуємо відставання, якщо оброблення затрималося.
        self.next_tick = now + self.INTERVAL

        return True

    def resample(self, missing_data_handler):
        now = datetime.now(timezone.utc)

        return {
            "timestamp": now.isoformat(),
            "frequency_hz": 10,
            "telemetry": missing_data_handler.get_snapshot(now)
        }