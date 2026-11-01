import json
import logging
import time

from datetime import datetime, timezone


class StateSynchronizer:
    """Керування правилами синхронізації стану цифрового двійника."""

    # Інтервал періодичної синхронізації, секунди
    PERIODIC_INTERVAL = 1.0

    # Пороги зміни числових показників
    CHANGE_THRESHOLDS = {
        "SIG_VEL_01": 2.0,    # Швидкість, км/год
        "SIG_RPM_02": 100.0,  # Оберти двигуна, об/хв
        "SIG_CUR_03": 2.0,    # Струм батареї, А
        "SIG_VOL_04": 0.5,    # Напруга батареї, В
        "SIG_TMP_05": 1.0,    # Температура двигуна, °C
        "SIG_SOC_06": 1.0     # Заряд батареї, %
    }

    # Подієві сигнали
    EVENT_SENSORS = {
        "SIG_BRK_07",
        "SIG_LCK_08"
    }

    def __init__(self, state_manager):

        self.state_manager = state_manager

        self.last_periodic_sync = time.monotonic()

        # Значення показників під час останньої синхронізації
        self.last_synced_values = {}

        # Останні відомі стани подієвих сигналів
        self.last_event_values = {}

        # Кількість виконаних синхронізацій
        self.sync_count = 0

        # Останній синхронізований стан цифрового двійника
        self.current_state = None

    def _sync(self, snapshot, reason):
        """Виконання синхронізації стану."""

        state = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "sync_reason": reason,
            "telemetry": snapshot
        }

        # Збереження поточного синхронізованого стану
        self.current_state = state

        # Передавання нового стану до менеджера
        self.state_manager.update_state(state)

        # Запам'ятовування значень, використаних для синхронізації
        for sensor_id, measurement in snapshot.items():

            if measurement["valid"]:
                self.last_synced_values[sensor_id] = (
                    measurement["value"]
                )

        self.sync_count += 1

        print(
            f"\n[СИНХРОНІЗАЦІЯ #{self.sync_count}] "
            f"Причина: {reason}"
        )

        logging.info(
            "digital_twin_sync=%s",
            json.dumps(state, ensure_ascii=False, default=str)
        )

        return state

    def check_message(self, data, snapshot):
        """Перевірка необхідності синхронізації після нового повідомлення."""

        sensor_id = data["sensor_id"]
        value = data["value"]

        # Правило 1. Синхронізація за подією
        if sensor_id in self.EVENT_SENSORS:

            previous_value = self.last_event_values.get(
                sensor_id
            )

            self.last_event_values[sensor_id] = value

            # Перша отримана подія або зміна стану
            if previous_value is None or previous_value != value:

                self._sync(
                    snapshot,
                    reason=f"event:{sensor_id}"
                )

            return

        # Правило 2. Синхронізація за порогом зміни
        threshold = self.CHANGE_THRESHOLDS.get(sensor_id)

        if threshold is None:
            return

        previous_value = self.last_synced_values.get(
            sensor_id
        )

        # Якщо показник ще не брав участі в синхронізації,
        # перевірка порога не виконується.
        if previous_value is None:
            return

        change = abs(value - previous_value)

        if change >= threshold:

            self._sync(
                snapshot,
                reason=f"threshold:{sensor_id}"
            )

    def check_periodic(self, snapshot):
        """Перевірка необхідності періодичної синхронізації."""

        current_time = time.monotonic()

        if (
            current_time - self.last_periodic_sync
            >= self.PERIODIC_INTERVAL
        ):

            self._sync(
                snapshot,
                reason="periodic"
            )

            self.last_periodic_sync = current_time