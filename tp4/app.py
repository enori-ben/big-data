
from pathlib import Path
import pandas as pd

DATA_DIR = Path("/app/data")
CHUNK_SIZE = 100_000


def main():
    csv_files = list(DATA_DIR.glob("*.csv"))

    if not csv_files:
        print("No CSV dataset found in /app/data.")
        return

    for file_path in csv_files:
        print(f"\n{'=' * 60}")
        print(f"Dataset: {file_path.name}")
        print(f"Chunk size: {CHUNK_SIZE:,}")
        print("Processing the complete CSV file...")
        print("=" * 60)

        total_rows = 0
        columns = None
        dtypes = None
        missing_values = None
        numeric_sums = None
        numeric_counts = None
        numeric_min = None
        numeric_max = None

        try:
            for chunk_number, chunk in enumerate(
                pd.read_csv(
                    file_path,
                    chunksize=CHUNK_SIZE,
                    low_memory=False
                ),
                start=1
            ):
                total_rows += len(chunk)

                if columns is None:
                    columns = list(chunk.columns)
                    dtypes = chunk.dtypes.astype(str)
                    missing_values = pd.Series(
                        0, index=columns, dtype="int64"
                    )
                    numeric_columns = chunk.select_dtypes(
                        include="number"
                    ).columns

                    numeric_sums = pd.Series(
                        0.0, index=numeric_columns
                    )
                    numeric_counts = pd.Series(
                        0, index=numeric_columns, dtype="int64"
                    )
                    numeric_min = pd.Series(
                        float("inf"), index=numeric_columns
                    )
                    numeric_max = pd.Series(
                        float("-inf"), index=numeric_columns
                    )

                missing_values = missing_values.add(
                    chunk.isna().sum(), fill_value=0
                )

                numeric = chunk.select_dtypes(include="number")

                if len(numeric.columns):
                    numeric_sums = numeric_sums.add(
                        numeric.sum(), fill_value=0
                    )
                    numeric_counts = numeric_counts.add(
                        numeric.count(), fill_value=0
                    )
                    numeric_min = pd.concat(
                        [numeric_min, numeric.min()], axis=1
                    ).min(axis=1)
                    numeric_max = pd.concat(
                        [numeric_max, numeric.max()], axis=1
                    ).max(axis=1)

                if chunk_number % 10 == 0:
                    print(
                        f"Processed {total_rows:,} rows so far..."
                    )

            print("\nPROCESSING COMPLETED")
            print(f"Total rows: {total_rows:,}")
            print(f"Total columns: {len(columns)}")

            print("\nCOLUMN DATA TYPES")
            print(dtypes.to_string())

            print("\nMISSING VALUES (ALL ROWS)")
            print(missing_values.astype("int64").to_string())

            if len(numeric_sums.index):
                means = numeric_sums / numeric_counts.replace(0, float("nan"))
                summary = pd.DataFrame({
                    "mean": means,
                    "min": numeric_min,
                    "max": numeric_max,
                    "non_null_count": numeric_counts,
                })

                print("\nNUMERIC SUMMARY (ALL ROWS)")
                print(summary.to_string())

            print("\nAll CSV rows have been processed.")

        except Exception as exc:
            print(f"Error processing {file_path.name}: {exc}")
            raise


if __name__ == "__main__":
    main()
