import json
import logging
import os

from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# Завантаження параметрів із кореневого файлу .env
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(ENV_PATH)


class DatabaseManager:
    """Керування збереженням даних цифрового двійника в InfluxDB."""

    def __init__(self):
        self.url = os.getenv(
            "INFLUXDB_URL",
            "http://localhost:8086"
        )

        self.token = os.getenv("INFLUXDB_TOKEN")
        self.org = os.getenv("INFLUXDB_ORG")
        self.bucket = os.getenv("INFLUXDB_BUCKET")

        self.client = None
        self.write_api = None

    def connect(self):
        """Встановлення з'єднання з InfluxDB."""

        if not all([self.token, self.org, self.bucket]):
            raise ValueError(
                "Не задано INFLUXDB_TOKEN, INFLUXDB_ORG або INFLUXDB_BUCKET"
            )

        self.client = InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org
        )

        self.write_api = self.client.write_api(
            write_options=SYNCHRONOUS
        )

        # Перевірка доступності сховища.
        self.client.health()

        logging.info("З'єднання з InfluxDB встановлено.")
        print("З'єднання з InfluxDB встановлено.")

    def write_telemetry(self, data, timestamp):
        """Збереження прийнятого вимірювання датчика."""

        try:
            point = (
                Point("telemetry")
                .tag("object_id", "scooter_01")
                .tag("sensor_id", data["sensor_id"])
                .tag("name", data["name"])
                .tag("unit", data["unit"])
            )

            value = data["value"]

            if isinstance(value, bool):
                point = point.field("value_bool", value)
            else:
                point = point.field("value", float(value))

            point = point.time(
                timestamp,
                WritePrecision.NS
            )

            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point
            )

            logging.info(
                "Телеметрію збережено: sensor=%s",
                data["sensor_id"]
            )

        except Exception:
            logging.exception(
                "Помилка збереження телеметрії."
            )

    def write_twin_state(self, state):
        """Збереження знімка стану цифрового двійника."""

        try:
            timestamp = datetime.fromisoformat(
                state["updated_at"].replace("Z", "+00:00")
            )

            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(
                    tzinfo=timezone.utc
                )

            point = (
                Point("twin_state")
                .tag("object_id", "scooter_01")
                .tag(
                    "sync_reason",
                    str(state.get("sync_reason", "unknown"))
                )
                .field(
                    "state_json",
                    json.dumps(state, ensure_ascii=False)
                )
                .time(
                    timestamp,
                    WritePrecision.NS
                )
            )

            self.write_api.write(
                bucket=self.bucket,
                org=self.org,
                record=point
            )

            logging.info(
                "Стан цифрового двійника збережено."
            )

        except Exception:
            logging.exception(
                "Помилка збереження стану цифрового двійника."
            )

    def close(self):
        """Закриття з'єднання зі сховищем."""

        if self.write_api is not None:
            self.write_api.close()

        if self.client is not None:
            self.client.close()

        logging.info("З'єднання з InfluxDB закрито.")


# Створення екземпляра компонента
database_manager = DatabaseManager()