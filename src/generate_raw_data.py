import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "2_data" / "raw"

RAW_DIR.mkdir(parents=True, exist_ok=True)

def generate_services():
    data = [
        ["S001", "Workplace Services", "IT100", "End User Computing"],
        ["S002", "Network Services", "IT200", "Infrastructure"],
        ["S003", "Cloud Infrastructure", "IT300", "Cloud Team"],
        ["S004", "ERP Application Support", "IT400", "Application Management"],
        ["S005", "Identity and Access Management", "IT500", "Security Team"]
    ]

    df = pd.DataFrame(data, columns=[
        "service_id",
        "service_name",
        "cost_center",
        "service_owner"
    ])

    df.to_csv(RAW_DIR / "services.csv", index=False)


def generate_costs():
    data = [
        ["2025-01-01","S001","Hardware","Dell",25000],
        ["2025-01-01","S002","Network Equipment","Cisco",18000],
        ["2025-01-01","S003","Cloud Services","AWS",32000],
        ["2025-01-01","S004","Software License","SAP",27000],
        ["2025-01-01","S005","Security Software","Microsoft",15000],
        ["2025-02-01","S001","Hardware","Dell",22000],
        ["2025-02-01","S002","Network Equipment","Cisco",17000],
        ["2025-02-01","S003","Cloud Services","AWS",35000],
        ["2025-02-01","S004","Software License","SAP",26000],
        ["2025-02-01","S005","Security Software","Microsoft",14000],
    ]

    df = pd.DataFrame(data, columns=[
        "date",
        "service_id",
        "cost_type",
        "vendor",
        "amount_chf"
    ])

    df.to_csv(RAW_DIR / "costs.csv", index=False)


def generate_usage_drivers():
    data = [
        ["2025-01-01","S001",1200,1100,0,240],
        ["2025-01-01","S002",1200,950,0,120],
        ["2025-01-01","S003",0,0,320,40],
        ["2025-01-01","S004",650,0,0,80],
        ["2025-01-01","S005",1200,0,0,60],
        ["2025-02-01","S001",1220,1120,0,210],
        ["2025-02-01","S002",1220,970,0,110],
        ["2025-02-01","S003",0,0,350,45],
        ["2025-02-01","S004",660,0,0,70],
        ["2025-02-01","S005",1220,0,0,55],
    ]

    df = pd.DataFrame(data, columns=[
        "date",
        "service_id",
        "users",
        "devices",
        "virtual_machines",
        "tickets"
    ])

    df.to_csv(RAW_DIR / "usage_drivers.csv", index=False)


def main():
    generate_services()
    generate_costs()
    generate_usage_drivers()

    print("Raw IT controlling data generated successfully.")


if __name__ == "__main__":
    main()