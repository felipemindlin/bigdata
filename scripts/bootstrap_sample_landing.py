import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
LANDING = BASE / "datalake" / "landing"
STREAM_DIR = LANDING / "usage_events_stream"


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(header)]
    for row in rows:
        lines.append(",".join(str(x) if x is not None else "" for x in row))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_jsonl(path: Path, events: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e, ensure_ascii=True) + "\n")


def main() -> None:
    LANDING.mkdir(parents=True, exist_ok=True)
    STREAM_DIR.mkdir(parents=True, exist_ok=True)

    write_csv(
        LANDING / "customers_orgs.csv",
        ["org_id", "org_name", "industry", "region", "plan", "nps_score"],
        [
            ["org_001", "Acme Cloud", "retail", "us-east", "enterprise", 42.0],
            ["org_002", "Beta Labs", "health", "eu-west", "pro", 65.0],
            ["org_003", "Gamma AI", "tech", "sa-east", "starter", 15.0],
        ],
    )

    write_csv(
        LANDING / "users.csv",
        ["user_id", "org_id", "role", "is_active"],
        [
            ["u_001", "org_001", "admin", "true"],
            ["u_002", "org_001", "analyst", "true"],
            ["u_003", "org_002", "engineer", "false"],
            ["u_004", "org_003", "owner", "true"],
        ],
    )

    write_csv(
        LANDING / "billing_monthly.csv",
        ["org_id", "month", "revenue_usd", "credits_usd", "taxes_usd", "currency"],
        [
            ["org_001", "2026-03", 12000.0, 300.0, 1700.0, "USD"],
            ["org_002", "2026-03", 7400.0, 120.0, 860.0, "USD"],
            ["org_003", "2026-03", 1800.0, 0.0, 220.0, "USD"],
        ],
    )

    now = datetime.now(timezone.utc)

    events_a = [
        {
            "event_id": "e_001",
            "org_id": "org_001",
            "user_id": "u_001",
            "resource_id": "r_001",
            "service": "compute",
            "event_ts": (now - timedelta(minutes=15)).isoformat(),
            "schema_version": 2,
            "value": "12.5",
            "unit": "cpu_hours",
            "cost_usd_increment": 8.4,
            "requests": 120,
            "cpu_hours": 12.5,
            "storage_gb_hours": 0.0,
            "genai_tokens": 0,
            "carbon_kg": 1.6,
        },
        {
            "event_id": "e_002",
            "org_id": "org_001",
            "user_id": "u_002",
            "resource_id": "r_002",
            "service": "genai",
            "event_ts": (now - timedelta(minutes=12)).isoformat(),
            "schema_version": 2,
            "value": "5000",
            "unit": "tokens",
            "cost_usd_increment": 4.1,
            "requests": 25,
            "cpu_hours": 0.0,
            "storage_gb_hours": 0.0,
            "genai_tokens": 5000,
            "carbon_kg": 0.3,
        },
        {
            "event_id": "e_003",
            "org_id": "org_002",
            "user_id": "u_003",
            "resource_id": "r_003",
            "service": "storage",
            "event_ts": (now - timedelta(minutes=10)).isoformat(),
            "schema_version": 1,
            "value": "15",
            "unit": "gb_hours",
            "cost_usd_increment": -0.5,
            "requests": 8,
            "cpu_hours": 0.0,
            "storage_gb_hours": 15.0,
        },
    ]

    events_b = [
        {
            "event_id": "e_001",
            "org_id": "org_001",
            "user_id": "u_001",
            "resource_id": "r_001",
            "service": "compute",
            "event_ts": (now - timedelta(minutes=15)).isoformat(),
            "schema_version": 2,
            "value": "12.5",
            "unit": "cpu_hours",
            "cost_usd_increment": 8.4,
            "requests": 120,
            "cpu_hours": 12.5,
            "storage_gb_hours": 0.0,
            "genai_tokens": 0,
            "carbon_kg": 1.6,
        },
        {
            "event_id": "e_004",
            "org_id": "org_003",
            "user_id": "u_004",
            "resource_id": "r_004",
            "service": "database",
            "event_ts": (now - timedelta(minutes=8)).isoformat(),
            "schema_version": 1,
            "value": "6",
            "unit": "requests",
            "cost_usd_increment": 1.8,
            "requests": 200,
            "cpu_hours": 0.0,
            "storage_gb_hours": 0.0,
        },
        {
            "event_id": "e_005",
            "org_id": "org_002",
            "user_id": "u_003",
            "resource_id": "r_003",
            "service": "storage",
            "event_ts": (now - timedelta(hours=3)).isoformat(),
            "schema_version": 2,
            "value": "9",
            "unit": "gb_hours",
            "cost_usd_increment": 0.7,
            "requests": 3,
            "cpu_hours": 0.0,
            "storage_gb_hours": 9.0,
            "genai_tokens": 0,
            "carbon_kg": 0.2,
        },
        {
            "event_id": "e_006",
            "org_id": "org_003",
            "user_id": "u_004",
            "resource_id": "r_005",
            "service": "compute",
            "event_ts": (now - timedelta(minutes=5)).isoformat(),
            "schema_version": 2,
            "value": "22",
            "unit": None,
            "cost_usd_increment": 3.4,
            "requests": 44,
            "cpu_hours": 22.0,
            "storage_gb_hours": 0.0,
            "genai_tokens": 0,
            "carbon_kg": 0.9,
        },
    ]

    write_jsonl(STREAM_DIR / "part-0001.jsonl", events_a)
    write_jsonl(STREAM_DIR / "part-0002.jsonl", events_b)

    print(f"Sample landing data generated in: {LANDING}")


if __name__ == "__main__":
    main()
