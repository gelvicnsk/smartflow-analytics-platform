from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import input_file_name
from src.config import RAW_DIR, BRONZE_DIR


def list_clean_csv_files(folder: Path):
    return [
        str(p)
        for p in folder.glob("*.csv")
        if ":Zone.Identifier" not in p.name
    ]


def read_csv_folder(spark, folder_name: str) -> DataFrame:
    folder = RAW_DIR / folder_name
    files = list_clean_csv_files(folder)

    if not files:
        raise FileNotFoundError(f"Aucun CSV valide trouvé dans {folder}")

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(files)
        .withColumn("source_file", input_file_name())
    )


def load_all_raw_data(spark):
    dataframes = {}

    folders = [
        "Velos",
        "Stations",
        "Villes",
        "Cyclistes",
        "Prestataires",
        "Reparateurs",
    ]

    for folder in folders:
        path = RAW_DIR / folder
        if path.exists():
            dataframes[folder.lower()] = read_csv_folder(spark, folder)

    return dataframes


def save_bronze(dataframes: dict):
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    for name, df in dataframes.items():
        (
            df.write
            .mode("overwrite")
            .parquet(str(BRONZE_DIR / name))
        )
        print(f"[BRONZE] {name} sauvegardé.")
