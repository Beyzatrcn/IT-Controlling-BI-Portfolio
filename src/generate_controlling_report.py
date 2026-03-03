import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "2_data" / "raw"
OUT_DIR = BASE_DIR / "8_results" / "sample_reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_csv(name: str) -> pd.DataFrame:
    path = RAW_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")

    # Robust CSV read: handles UTF-8 BOM and auto-detects separator (comma vs semicolon)
    df = pd.read_csv(path, encoding="utf-8-sig", sep=None, engine="python")

    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    """
    NA-safe division:
    - casts to numeric
    - replaces 0 denom with NA
    - replaces inf/-inf with NA
    - uses pandas nullable Float64 dtype so .round works with NA
    """
    numer = pd.to_numeric(numer, errors="coerce")
    denom = pd.to_numeric(denom, errors="coerce").replace(0, pd.NA)

    result = numer / denom
    result = result.replace([float("inf"), -float("inf")], pd.NA)

    return result.astype("Float64").round(2)


def main() -> None:
    services = load_csv("services.csv")
    costs = load_csv("costs.csv")
    drivers = load_csv("usage_drivers.csv")

    # Validate expected columns (clear error messages)
    required_service_cols = {"service_id"}
    required_cost_cols = {"date", "service_id", "amount_chf"}
    required_driver_cols = {"date", "service_id", "users", "devices", "virtual_machines", "tickets"}

    missing_services = required_service_cols - set(services.columns)
    missing_cost = required_cost_cols - set(costs.columns)
    missing_driver = required_driver_cols - set(drivers.columns)

    if missing_services:
        raise ValueError(f"services.csv missing columns: {sorted(missing_services)}. Found: {list(services.columns)}")
    if missing_cost:
        raise ValueError(f"costs.csv missing columns: {sorted(missing_cost)}. Found: {list(costs.columns)}")
    if missing_driver:
        raise ValueError(
            f"usage_drivers.csv missing columns: {sorted(missing_driver)}. Found: {list(drivers.columns)}"
        )

    # Parse dates
    costs["date"] = pd.to_datetime(costs["date"], errors="coerce")
    drivers["date"] = pd.to_datetime(drivers["date"], errors="coerce")

    if costs["date"].isna().any():
        raise ValueError("costs.csv contains invalid date values.")
    if drivers["date"].isna().any():
        raise ValueError("usage_drivers.csv contains invalid date values.")

    # Ensure numeric columns are numeric
    costs["amount_chf"] = pd.to_numeric(costs["amount_chf"], errors="coerce")
    for c in ["users", "devices", "virtual_machines", "tickets"]:
        drivers[c] = pd.to_numeric(drivers[c], errors="coerce")

    # Aggregate costs per month & service
    monthly_cost = (
        costs.groupby([pd.Grouper(key="date", freq="MS"), "service_id"], as_index=False)["amount_chf"]
        .sum()
        .rename(columns={"amount_chf": "total_cost_chf"})
    )

    # Aggregate drivers per month & service
    monthly_drivers = (
        drivers.groupby([pd.Grouper(key="date", freq="MS"), "service_id"], as_index=False)[
            ["users", "devices", "virtual_machines", "tickets"]
        ]
        .sum()
    )

    # Merge + enrich
    report = (
        monthly_cost.merge(monthly_drivers, on=["date", "service_id"], how="left")
        .merge(services, on="service_id", how="left")
        .sort_values(["date", "service_id"])
    )

    # KPI calculations
    report["cost_per_user_chf"] = safe_div(report["total_cost_chf"], report["users"])
    report["cost_per_device_chf"] = safe_div(report["total_cost_chf"], report["devices"])
    report["cost_per_vm_chf"] = safe_div(report["total_cost_chf"], report["virtual_machines"])
    report["cost_per_ticket_chf"] = safe_div(report["total_cost_chf"], report["tickets"])

    out_path = OUT_DIR / "monthly_service_cost_report.csv"
    report.to_csv(out_path, index=False)

    print(f"Report generated: {out_path}")
    print(report.head(10).to_string(index=False))


if __name__ == "__main__":
    main()