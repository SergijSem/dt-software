import base64
import json
import logging
import os
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


class DittoAdapter:
    """Адаптер передавання обробленої телеметрії до Eclipse Ditto."""

    SENSOR_MAP = {
        "SIG_VEL_01": ("org.pzks:running-gear-1", "speed"),
        "SIG_RPM_02": ("org.pzks:motor-1", "rpm"),
        "SIG_TMP_05": ("org.pzks:motor-1", "temperature"),
        "SIG_CUR_03": ("org.pzks:battery-1", "current"),
        "SIG_VOL_04": ("org.pzks:battery-1", "voltage"),
        "SIG_SOC_06": ("org.pzks:battery-1", "stateOfCharge"),
        "SIG_BRK_07": ("org.pzks:brakes-1", "active"),
        "SIG_LCK_08": ("org.pzks:security-1", "locked")
    }

    def __init__(self):
        self.base_url = os.getenv(
            "DITTO_BASE_URL",
            "http://localhost:8082/api/2"
        ).rstrip("/")
        self.username = os.getenv("DITTO_USERNAME", "ditto")
        self.password = os.getenv("DITTO_PASSWORD", "ditto")
        self.timeout = float(os.getenv("DITTO_TIMEOUT", "5"))

        self.tasks = queue.Queue(maxsize=1)
        self.queue_lock = threading.Lock()
        self.stopping = False
        self.worker = None

    def start(self):
        if self.worker is not None and self.worker.is_alive():
            return

        self.worker = threading.Thread(
            target=self._run,
            name="ditto-adapter",
            daemon=True
        )
        self.worker.start()
        logging.info("Адаптер Eclipse Ditto запущено.")

    def on_state_update(self, state):
        if not isinstance(state, dict):
            logging.warning("Адаптер отримав некоректний стан.")
            return

        if not isinstance(state.get("telemetry"), dict):
            logging.warning("У стані відсутній блок telemetry.")
            return

        with self.queue_lock:
            if self.stopping:
                return

            try:
                self.tasks.put_nowait(state)
            except queue.Full:
                try:
                    self.tasks.get_nowait()
                    self.tasks.task_done()
                except queue.Empty:
                    pass

                try:
                    self.tasks.put_nowait(state)
                except queue.Full:
                    logging.warning(
                        "Не вдалося додати найновіший стан до черги."
                    )

    def _update_properties(self, thing_id, properties):
        encoded_id = quote(thing_id, safe=":")

        url = f"{self.base_url}/things/{encoded_id}"

        credentials = base64.b64encode(
            f"{self.username}:{self.password}".encode("utf-8")
        ).decode("ascii")

        body = json.dumps(
            {
                "features": {
                    "state": {
                        "properties": properties
                    }
                }
            },
            ensure_ascii=False
        ).encode("utf-8")

        request = Request(
            url,
            data=body,
            method="PATCH",
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/merge-patch+json"
            }
        )

        with urlopen(request, timeout=self.timeout) as response:
            if response.status not in (200, 201, 204):
                raise RuntimeError(
                    f"Неочікуваний HTTP-статус: {response.status}"
                )

        logging.info(
            "Ditto Thing updated: thing=%s properties=%s",
            thing_id,
            list(properties.keys())
        )

    def _sync_state(self, state):
        telemetry = state["telemetry"]
        updates = {}

        for sensor_id, measurement in telemetry.items():
            if sensor_id not in self.SENSOR_MAP:
                continue

            if not isinstance(measurement, dict):
                continue

            if not measurement.get("valid", False):
                continue

            value = measurement.get("value")
            if value is None:
                continue

            thing_id, property_name = self.SENSOR_MAP[sensor_id]

            if thing_id not in updates:
                updates[thing_id] = {}

            updates[thing_id][property_name] = value

        if not updates:
            return

        with ThreadPoolExecutor(
            max_workers=len(updates),
            thread_name_prefix="ditto-http"
        ) as executor:
            futures = {
                executor.submit(
                    self._update_properties,
                    thing_id,
                    properties
                ): thing_id
                for thing_id, properties in updates.items()
            }

            for future, thing_id in futures.items():
                try:
                    future.result()
                except (
                    HTTPError,
                    URLError,
                    OSError,
                    ValueError,
                    RuntimeError
                ) as error:
                    logging.error(
                        "Помилка оновлення Ditto: thing=%s error=%s",
                        thing_id,
                        error
                    )

    def _run(self):
        while True:
            state = self.tasks.get()

            try:
                if state is None:
                    return

                self._sync_state(state)

            except Exception:
                logging.exception(
                    "Непередбачена помилка адаптера Ditto."
                )

            finally:
                self.tasks.task_done()

    def close(self):
        with self.queue_lock:
            self.stopping = True

            if self.worker is None or not self.worker.is_alive():
                return

            try:
                self.tasks.put_nowait(None)
            except queue.Full:
                try:
                    self.tasks.get_nowait()
                    self.tasks.task_done()
                except queue.Empty:
                    pass

                self.tasks.put_nowait(None)

        self.worker.join(timeout=30)

        if self.worker.is_alive():
            logging.warning(
                "Потік адаптера Ditto не завершився вчасно."
            )
        else:
            logging.info("Адаптер Eclipse Ditto зупинено.")