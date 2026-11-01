import logging
import threading
import time

from datetime import datetime, timezone


class PerformanceMonitor:
    """Збір і розрахунок показників продуктивності конвеєра."""

    def __init__(self):
        self.lock = threading.Lock()

        self.started = False
        self.start_time = None
        self.end_time = None

        self.processed_messages = 0
        self.latencies_ms = []

        # Ідентифікатори вимірювань, які вже враховані.
        self.seen_measurements = set()

    def start(self):
        """Початок вимірювання продуктивності."""

        with self.lock:
            self.started = True
            self.start_time = time.monotonic()
            self.end_time = None

            self.processed_messages = 0
            self.latencies_ms.clear()
            self.seen_measurements.clear()

        logging.info("performance_measurement_started")

        print("\nВимірювання продуктивності розпочато.")

    def record_processed_message(self):
        """Реєстрація успішно обробленого повідомлення."""

        with self.lock:
            if self.started and self.end_time is None:
                self.processed_messages += 1

    @staticmethod
    def parse_timestamp(value):
        """Перетворення часової мітки на datetime."""

        if isinstance(value, datetime):
            timestamp = value
        elif isinstance(value, str):
            timestamp = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        else:
            return None

        if timestamp.tzinfo is None:
            return None

        return timestamp.astimezone(timezone.utc)

    def record_state_update(self, state, update_time):
        """
        Реєстрація затримки для нових вимірювань,
        які увійшли до оновленого стану двійника.
        """

        telemetry = state.get("telemetry", {})

        with self.lock:
            if not self.started or self.end_time is not None:
                return

            for sensor_id, measurement in telemetry.items():

                if not measurement.get("valid", False):
                    continue

                generated_at = self.parse_timestamp(
                    measurement.get("timestamp")
                )

                if generated_at is None:
                    continue

                measurement_id = (
                    sensor_id,
                    generated_at.isoformat()
                )

                # Не враховуємо те саме вимірювання повторно.
                if measurement_id in self.seen_measurements:
                    continue

                self.seen_measurements.add(measurement_id)

                latency = (
                    update_time - generated_at
                ).total_seconds() * 1000

                # Від'ємна затримка означає проблему з часовими мітками.
                if latency < 0:
                    logging.warning(
                        "negative_latency: sensor=%s | latency=%.3f ms",
                        sensor_id,
                        latency
                    )
                    continue

                self.latencies_ms.append(latency)

    def stop(self):
        """Завершення вимірювання та виведення результатів."""

        with self.lock:
            if not self.started or self.end_time is not None:
                return

            self.end_time = time.monotonic()

            duration = self.end_time - self.start_time

            count = self.processed_messages
            latencies = list(self.latencies_ms)

            self.started = False

        throughput = (
            count / duration if duration > 0 else 0.0
        )

        average_latency = (
            sum(latencies) / len(latencies)
            if latencies else 0.0
        )

        maximum_latency = (
            max(latencies) if latencies else 0.0
        )

        print("\n" + "=" * 55)
        print("РЕЗУЛЬТАТИ ВИМІРЮВАННЯ ПРОДУКТИВНОСТІ")
        print("=" * 55)

        print(f"Тривалість тесту: {duration:.2f} с")
        print(f"Оброблено повідомлень: {count}")
        print(
            f"Пропускна здатність: "
            f"{throughput:.2f} повідомлень/с"
        )

        print(
            f"Кількість вимірювань із розрахованою затримкою: "
            f"{len(latencies)}"
        )

        if latencies:
            print(
                f"Середня затримка: {average_latency:.3f} мс"
            )
            print(
                f"Максимальна затримка: {maximum_latency:.3f} мс"
            )
        else:
            print("Затримку не вдалося розрахувати.")

        print("=" * 55 + "\n")

        logging.info(
            "performance_results | duration=%.3f s | "
            "processed=%d | throughput=%.3f msg/s | "
            "latency_samples=%d | average_latency=%.3f ms | "
            "maximum_latency=%.3f ms",
            duration,
            count,
            throughput,
            len(latencies),
            average_latency,
            maximum_latency
        )

        return {
            "duration_seconds": duration,
            "processed_messages": count,
            "throughput_messages_per_second": throughput,
            "latency_samples": len(latencies),
            "average_latency_ms": average_latency,
            "maximum_latency_ms": maximum_latency
        }