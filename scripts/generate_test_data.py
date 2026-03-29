"""Generate synthetic test Parquet file with known edge cases.

Creates tests/fixtures/test_accounts.parquet with deterministic data
for unit and integration testing.
"""

from datetime import date, datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROWS = [
    # test_001: At Risk Oct-Nov-Dec-Jan (4 consecutive) -> Duration: 4
    ("test_001", "Account 001", "AMER", date(2025, 10, 1), "At Risk", date(2026, 3, 1), "owner01@example.com", 50000, datetime(2025, 10, 5, 12, 0, 0)),
    ("test_001", "Account 001", "AMER", date(2025, 11, 1), "At Risk", date(2026, 3, 1), "owner01@example.com", 50000, datetime(2025, 11, 5, 12, 0, 0)),
    ("test_001", "Account 001", "AMER", date(2025, 12, 1), "At Risk", date(2026, 3, 1), "owner01@example.com", 50000, datetime(2025, 12, 5, 12, 0, 0)),
    ("test_001", "Account 001", "AMER", date(2026, 1, 1), "At Risk", date(2026, 3, 1), "owner01@example.com", 50000, datetime(2026, 1, 5, 12, 0, 0)),

    # test_002: At Risk Oct-Nov, Healthy Dec, At Risk Jan -> Duration: 1
    ("test_002", "Account 002", "EMEA", date(2025, 10, 1), "At Risk", date(2026, 5, 1), "owner02@example.com", 75000, datetime(2025, 10, 5, 12, 0, 0)),
    ("test_002", "Account 002", "EMEA", date(2025, 11, 1), "At Risk", date(2026, 5, 1), "owner02@example.com", 75000, datetime(2025, 11, 5, 12, 0, 0)),
    ("test_002", "Account 002", "EMEA", date(2025, 12, 1), "Healthy", date(2026, 5, 1), "owner02@example.com", 75000, datetime(2025, 12, 5, 12, 0, 0)),
    ("test_002", "Account 002", "EMEA", date(2026, 1, 1), "At Risk", date(2026, 5, 1), "owner02@example.com", 75000, datetime(2026, 1, 5, 12, 0, 0)),

    # test_003: At Risk Jan only (no prior history) -> Duration: 1
    ("test_003", "Account 003", "APAC", date(2026, 1, 1), "At Risk", date(2026, 6, 1), "owner03@example.com", 30000, datetime(2026, 1, 5, 12, 0, 0)),

    # test_004: At Risk Jan, null region -> unroutable (unknown_region)
    ("test_004", "Account 004", None, date(2026, 1, 1), "At Risk", date(2026, 4, 1), "owner04@example.com", 40000, datetime(2026, 1, 5, 12, 0, 0)),

    # test_005: At Risk Jan, region="LATAM" (unmapped) -> unroutable
    ("test_005", "Account 005", "LATAM", date(2026, 1, 1), "At Risk", date(2026, 7, 1), "owner05@example.com", 60000, datetime(2026, 1, 5, 12, 0, 0)),

    # test_006: At Risk Jan, ARR=5000 (below default threshold) -> filtered
    ("test_006", "Account 006", "AMER", date(2026, 1, 1), "At Risk", date(2026, 8, 1), "owner06@example.com", 5000, datetime(2026, 1, 5, 12, 0, 0)),

    # test_007: At Risk Jan, ARR=0 -> filtered
    ("test_007", "Account 007", "AMER", date(2026, 1, 1), "At Risk", date(2026, 2, 1), "owner07@example.com", 0, datetime(2026, 1, 5, 12, 0, 0)),

    # test_008: Healthy Jan -> not in results
    ("test_008", "Account 008", "AMER", date(2026, 1, 1), "Healthy", date(2026, 9, 1), "owner08@example.com", 80000, datetime(2026, 1, 5, 12, 0, 0)),

    # test_009: At Risk Jan, two rows same month (duplicate) -> keep latest
    ("test_009", "Account 009", "EMEA", date(2026, 1, 1), "At Risk", date(2026, 10, 1), "owner09@example.com", 45000, datetime(2026, 1, 3, 8, 0, 0)),
    ("test_009", "Account 009", "EMEA", date(2026, 1, 1), "At Risk", date(2026, 10, 1), "owner09@example.com", 45000, datetime(2026, 1, 5, 16, 0, 0)),

    # test_010: At Risk Jan, null renewal_date + null owner
    ("test_010", "Account 010", "APAC", date(2026, 1, 1), "At Risk", None, None, 55000, datetime(2026, 1, 5, 12, 0, 0)),

    # test_011: At Risk Oct-Nov, Churned Dec, At Risk Jan -> Duration: 1
    ("test_011", "Account 011", "AMER", date(2025, 10, 1), "At Risk", date(2026, 4, 1), "owner11@example.com", 35000, datetime(2025, 10, 5, 12, 0, 0)),
    ("test_011", "Account 011", "AMER", date(2025, 11, 1), "At Risk", date(2026, 4, 1), "owner11@example.com", 35000, datetime(2025, 11, 5, 12, 0, 0)),
    ("test_011", "Account 011", "AMER", date(2025, 12, 1), "Churned", date(2026, 4, 1), "owner11@example.com", 35000, datetime(2025, 12, 5, 12, 0, 0)),
    ("test_011", "Account 011", "AMER", date(2026, 1, 1), "At Risk", date(2026, 4, 1), "owner11@example.com", 35000, datetime(2026, 1, 5, 12, 0, 0)),

    # test_012: At Risk Dec-Jan spanning year boundary -> Duration: 2
    ("test_012", "Account 012", "EMEA", date(2025, 12, 1), "At Risk", date(2026, 5, 1), "owner12@example.com", 65000, datetime(2025, 12, 5, 12, 0, 0)),
    ("test_012", "Account 012", "EMEA", date(2026, 1, 1), "At Risk", date(2026, 5, 1), "owner12@example.com", 65000, datetime(2026, 1, 5, 12, 0, 0)),
]

COLUMNS = [
    "account_id", "account_name", "account_region", "month",
    "status", "renewal_date", "account_owner", "arr", "updated_at",
]


def main():
    df = pd.DataFrame(ROWS, columns=COLUMNS)

    # Match types from the real parquet file
    df["month"] = pd.to_datetime(df["month"]).dt.date
    df["renewal_date"] = pd.to_datetime(df["renewal_date"]).dt.date
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    df["arr"] = df["arr"].astype("int64")

    out_path = "tests/fixtures/test_accounts.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, out_path)
    print(f"Written {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
