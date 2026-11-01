import copy
import logging
import threading

from datetime import datetime, timezone


class StateManager:
    """Керування актуальним станом цифрового двійника."""

    def __init__(self, performance_monitor=None):
        self.current_state = None
        self.subscribers = {}
        self.lock = threading.RLock()

        self.performance_monitor = performance_monitor

    def subscribe(self, name, callback):
        """Реєстрація підписника."""

        with self.lock:
            self.subscribers[name] = callback

        logging.info(
            "Зареєстровано підписника: %s",
            name
        )

    def unsubscribe(self, name):
        """Видалення підписника."""

        with self.lock:
            if name in self.subscribers:
                del self.subscribers[name]

                logging.info(
                    "Видалено підписника: %s",
                    name
                )

    def update_state(self, new_state):
        """Оновлення стану та сповіщення підписників."""

        with self.lock:
            self.current_state = copy.deepcopy(new_state)

            # Фіксація фактичного моменту оновлення стану.
            update_time = datetime.now(timezone.utc)

            updated_state = copy.deepcopy(self.current_state)
            subscribers = list(self.subscribers.items())

        # Вимірювання затримки оновлення стану.
        if self.performance_monitor is not None:
            self.performance_monitor.record_state_update(
                updated_state,
                update_time
            )

        logging.info(
            "Актуальний стан цифрового двійника оновлено."
        )

        # Сповіщення зареєстрованих підписників.
        for name, callback in subscribers:
            try:
                callback(copy.deepcopy(updated_state))

            except Exception as error:
                logging.exception(
                    "Помилка сповіщення підписника %s: %s",
                    name,
                    error
                )

    def get_state(self):
        """Отримання останнього збереженого стану."""

        with self.lock:
            return copy.deepcopy(self.current_state)