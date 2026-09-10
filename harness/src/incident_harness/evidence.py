import json
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx


ALERT_NAME = "Product worker processing failures with consumer lag"


def _find_alert(alerts: list[dict]) -> dict | None:
    return next(
        (item for item in alerts if item.get("labels", {}).get("alertname") == ALERT_NAME),
        None,
    )


def wait_for_grafana_alert(output_directory: Path, timeout: int = 90) -> dict:
    output_directory.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    latest = None
    observations = output_directory / "alert-observations.jsonl"
    with httpx.Client(auth=("admin", "admin"), trust_env=False, timeout=5) as client:
        while (remaining := deadline - time.monotonic()) > 0:
            observation = {"observed_at": datetime.now(UTC).isoformat()}
            try:
                response = client.get(
                    "http://127.0.0.1:3000/api/alertmanager/grafana/api/v2/alerts",
                    params={"active": "true"},
                    timeout=min(5, remaining),
                )
                observation["status_code"] = response.status_code
                response.raise_for_status()
                latest = response.json()
                observation["response"] = latest
                alert = _find_alert(latest)
                with observations.open("a") as output:
                    output.write(json.dumps(observation) + "\n")
                if alert:
                    result = {
                        "schema_version": 1,
                        "source": "grafana",
                        "alert": alert,
                    }
                    (output_directory / "alert.json").write_text(json.dumps(result, indent=2) + "\n")
                    return result
            except (httpx.HTTPError, ValueError) as error:
                observation["error"] = f"{type(error).__name__}: {error}"
                with observations.open("a") as output:
                    output.write(json.dumps(observation) + "\n")
            if (remaining := deadline - time.monotonic()) > 0:
                time.sleep(min(2, remaining))

    (output_directory / "alert-timeout.json").write_text(json.dumps({"last_response": latest}, indent=2) + "\n")
    raise TimeoutError("Grafana alert did not fire")
