from pathlib import Path
import subprocess
import sys

BASE_DIR = Path(__file__).resolve().parents[1]

def run(cmd: list[str]) -> None:
    print("\n>> " + " ".join(cmd))
    result = subprocess.run(cmd, cwd=BASE_DIR, text=True)
    if result.returncode != 0:
        raise SystemExit(result.returncode)

def main() -> None:
    # Always use the current Python interpreter
    py = sys.executable

    print("Running IT Controlling mini-pipeline...")

    # 1) Generate raw data
    run([py, "src/generate_raw_data.py"])

    # 2) Generate monthly controlling report
    run([py, "src/generate_controlling_report.py"])

    report_path = BASE_DIR / "8_results" / "sample_reports" / "monthly_service_cost_report.csv"
    if report_path.exists():
        print(f"\nDONE ✅ Report ready: {report_path}")
    else:
        print("\nDONE (but report file not found) ⚠️ Please check logs above.")

if __name__ == "__main__":
    main()