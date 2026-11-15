import base64
import json
import os

import websocket


DITTO_WS_URL = os.getenv("DITTO_WS_URL", "ws://localhost:8082/ws/2")
DITTO_USERNAME = os.getenv("DITTO_USERNAME", "ditto")
DITTO_PASSWORD = os.getenv("DITTO_PASSWORD", "ditto")

TARGET_THING = "org.pzks:motor-1"


def get_thing_id(topic):
    parts = topic.split("/")
    if len(parts) < 2:
        return None

    return f"{parts[0]}:{parts[1]}"


def main():
    credentials = base64.b64encode(
        f"{DITTO_USERNAME}:{DITTO_PASSWORD}".encode("utf-8")
    ).decode("ascii")

    headers = [f"Authorization: Basic {credentials}"]

    print("Підключення до Eclipse Ditto...")
    print(f"WebSocket: {DITTO_WS_URL}")

    ws = websocket.create_connection(
        DITTO_WS_URL,
        header=headers,
        timeout=10
    )

    try:
        print("З'єднання встановлено.")

        ws.send("START-SEND-EVENTS")
        print("Надіслано запит на підписку на події.")

        while True:
            response = ws.recv()

            if response == "START-SEND-EVENTS:ACK":
                print("Підписку на події успішно активовано.")
                break

            if isinstance(response, str) and response.startswith("ERROR"):
                raise RuntimeError(response)

            print(f"Відповідь Ditto: {response}")

        print(f"Очікування змін цифрового двійника: {TARGET_THING}")
        print("Для завершення натисніть Ctrl+C.\n")

        ws.settimeout(None)

        while True:
            message = ws.recv()

            try:
                event = json.loads(message)
            except (json.JSONDecodeError, TypeError):
                continue

            if not isinstance(event, dict):
                continue

            topic = event.get("topic", "")
            thing_id = get_thing_id(topic)

            if thing_id != TARGET_THING:
                continue

            event_type = topic.split("/")[-1]
            path = event.get("path", "")

            print("[DITTO EVENT]")
            print(f"Thing: {thing_id}")
            print(f"Event: {event_type}")
            print(f"Property path: {path}")
            print(
                "New value:",
                json.dumps(event.get("value"), ensure_ascii=False)
            )
            print("-" * 45)

    except KeyboardInterrupt:
        print("\nОтримання подій зупинено.")

    finally:
        ws.close()
        print("WebSocket-з'єднання закрито.")


if __name__ == "__main__":
    main()