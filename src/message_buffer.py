import heapq
import itertools
import time
from datetime import timedelta


class MessageBuffer:
    def __init__(self, reorder_window=0.5):
        self.reorder_window = reorder_window
        self.heap = []
        self.sequence = itertools.count()
        self.max_timestamp = None
        self.last_emitted_timestamp = None

    def add(self, item):
        timestamp = item["parsed_timestamp"]

        heapq.heappush(
            self.heap,
            (timestamp, next(self.sequence), item)
        )

        if self.max_timestamp is None or timestamp > self.max_timestamp:
            self.max_timestamp = timestamp

    def get_ready(self, force=False):
        ready = []
        current_time = time.monotonic()

        while self.heap:
            timestamp, _, item = self.heap[0]

            watermark = (
                self.max_timestamp is not None
                and timestamp <= self.max_timestamp
                - timedelta(seconds=self.reorder_window)
            )

            timeout = (
                current_time - item["received_monotonic"]
                >= self.reorder_window
            )

            if not (force or watermark or timeout):
                break

            heapq.heappop(self.heap)

            item["late"] = (
                self.last_emitted_timestamp is not None
                and timestamp < self.last_emitted_timestamp
            )

            if (
                self.last_emitted_timestamp is None
                or timestamp > self.last_emitted_timestamp
            ):
                self.last_emitted_timestamp = timestamp

            ready.append(item)

        return ready