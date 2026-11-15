import base64
import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


class DittoGraphQueries:
    """Запити до графа цифрових двійників Eclipse Ditto."""

    THING_IDS = {
        "scooter": "org.pzks:scooter-1",
        "motor": "org.pzks:motor-1",
        "battery": "org.pzks:battery-1",
        "running_gear": "org.pzks:running-gear-1",
        "brakes": "org.pzks:brakes-1",
        "security": "org.pzks:security-1",
    }

    def __init__(self):
        self.base_url = os.getenv(
            "DITTO_BASE_URL",
            "http://localhost:8082/api/2"
        ).rstrip("/")

        username = os.getenv("DITTO_USERNAME", "ditto")
        password = os.getenv("DITTO_PASSWORD", "ditto")

        credentials = base64.b64encode(
            f"{username}:{password}".encode("utf-8")
        ).decode("ascii")

        self.headers = {
            "Authorization": f"Basic {credentials}",
            "Accept": "application/json",
        }

    def request_json(self, path):
        """Виконує GET-запит до HTTP API Ditto."""
        url = f"{self.base_url}/{path.lstrip('/')}"

        request = Request(
            url,
            headers=self.headers,
            method="GET"
        )

        try:
            with urlopen(request, timeout=10) as response:
                data = response.read().decode("utf-8")
                return json.loads(data) if data else None

        except HTTPError as error:
            details = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"HTTP {error.code}: {details}"
            ) from error

        except URLError as error:
            raise RuntimeError(
                f"Не вдалося підключитися до Ditto: {error}"
            ) from error

    def get_thing(self, thing_id):
        """Отримує повний поточний стан одного Thing."""
        encoded_id = quote(thing_id, safe=":")
        return self.request_json(f"things/{encoded_id}")

    def get_current_states(self):
        """Отримує поточний стан усіх шести двійників."""
        results = {}

        for name, thing_id in self.THING_IDS.items():
            results[thing_id] = self.get_thing(thing_id)

        return results

    def search_things(self, filter_expression):
        """Шукає цифрові двійники за RQL-умовою."""
        query = urlencode({"filter": filter_expression})
        return self.request_json(f"search/things?{query}")

    def traverse_from_root(self):
        """Обходить зв'язки від головного Thing до підсистем."""
        root_id = self.THING_IDS["scooter"]
        root = self.get_thing(root_id)

        child_ids = root.get("attributes", {}).get("hasPart", [])
        result = {
            "root": {
                "thingId": root_id,
                "data": root,
            },
            "children": [],
        }

        for child_id in child_ids:
            child = self.get_thing(child_id)
            result["children"].append({
                "thingId": child_id,
                "partOf": child.get("attributes", {}).get("partOf"),
                "data": child,
            })

        return result

    def traverse_to_root(self, child_id):
        """Переходить від дочірнього Thing до головного."""
        child = self.get_thing(child_id)
        parent_id = child.get("attributes", {}).get("partOf")

        if not parent_id:
            raise ValueError(
                f"Для {child_id} не визначено зв'язок partOf"
            )

        parent = self.get_thing(parent_id)

        return {
            "child": child,
            "parent": parent,
        }


def print_json(title, data):
    print(f"\n{'=' * 70}")
    print(title)
    print("=" * 70)
    print(json.dumps(data, ensure_ascii=False, indent=2))


def main():
    ditto = DittoGraphQueries()

    print("\nКРОК 5.1. ОТРИМАННЯ ПОТОЧНОГО СТАНУ")
    states = ditto.get_current_states()
    print_json("Поточний стан цифрових двійників", states)

    print("\nКРОК 5.2. ПОШУК ЗА ЗНАЧЕННЯМ ВЛАСТИВОСТІ")

    searches = {
        "Двигуни з температурою понад 50 °C":
            "gt(features/state/properties/temperature,50)",

        "Акумулятори із зарядом менше 30%":
            "lt(features/state/properties/stateOfCharge,30)",

        "Ходова частина зі швидкістю понад 25 км/год":
            "gt(features/state/properties/speed,25)",
    }

    for description, expression in searches.items():
        result = ditto.search_things(expression)
        print_json(
            f"{description}\nRQL: {expression}",
            result
        )

    print("\nКРОК 5.3. ОБХІД ЗВ'ЯЗКІВ ГРАФА")

    graph = ditto.traverse_from_root()
    print_json(
        "Головний двійник та його дочірні підсистеми",
        graph
    )

    reverse = ditto.traverse_to_root(
        DittoGraphQueries.THING_IDS["motor"]
    )

    print_json(
        "Перехід від двійника двигуна до головного двійника",
        reverse
    )


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, KeyError) as error:
        print(f"\nПОМИЛКА: {error}")
        raise SystemExit(1)