from pyspark.sql.functions import col, to_timestamp, trim
from src.config import SILVER_DIR


def clean_velos(df):
    return (
        df
        .filter(col("velo_id").isNotNull())
        .filter(col("performance").isNotNull())
        .filter(col("timestamp").isNotNull())
        .withColumn("velo_id", trim(col("velo_id")))
        .withColumn("performance", col("performance").cast("double"))
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .filter(col("timestamp").isNotNull())
        .filter((col("performance") >= 0) & (col("performance") <= 1))
        .dropDuplicates(["velo_id", "timestamp"])
    )


def clean_stations(df):
    cleaned = (
        df
        .filter(col("timestamp").isNotNull())
        .filter(col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2}"))
        .filter(col("station_id").isNotNull())
        .filter(col("station_id").rlike(r"^\d+(\.0)?$"))
    )

    cleaned = (
        cleaned
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("station_id", col("station_id").cast("double"))
    )

    if "cycliste_id" in cleaned.columns:
        cleaned = cleaned.withColumn("cycliste_id", col("cycliste_id").cast("double"))

    if "velo_id" in cleaned.columns:
        cleaned = cleaned.withColumn("velo_id", trim(col("velo_id")))

    if "velo_performance" in cleaned.columns:
        cleaned = cleaned.withColumn(
            "velo_performance",
            col("velo_performance").cast("double")
        )

    if "action" in cleaned.columns:
        cleaned = cleaned.withColumn("action", trim(col("action")))

    return (
        cleaned
        .filter(col("timestamp").isNotNull())
        .filter(col("station_id").isNotNull())
        .dropDuplicates()
    )


def clean_villes(df):
    cleaned = df.dropDuplicates()

    numeric_cols = [
        "id",
        "vitesse_a_pied",
        "vitesse_a_velo",
        "salaire",
        "age",
        "sportivite",
        "velo_perf_minimale",
    ]

    for c in numeric_cols:
        if c in cleaned.columns:
            cleaned = cleaned.withColumn(c, col(c).cast("double"))

    if "home" in cleaned.columns:
        cleaned = cleaned.withColumn("home", trim(col("home")))

    if "travail" in cleaned.columns:
        cleaned = cleaned.withColumn("travail", trim(col("travail")))

    if "statut" in cleaned.columns:
        cleaned = cleaned.withColumn("statut", trim(col("statut")))

    if "sexe" in cleaned.columns:
        cleaned = cleaned.withColumn("sexe", trim(col("sexe")))

    return cleaned


def clean_cyclistes(df):
    cleaned = df.dropDuplicates()

    if "timestamp" in cleaned.columns:
        cleaned = (
            cleaned
            .filter(col("timestamp").isNotNull())
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .filter(col("timestamp").isNotNull())
        )

    if "id" in cleaned.columns:
        cleaned = cleaned.withColumn("id", col("id").cast("double"))

    if "vitesse" in cleaned.columns:
        cleaned = cleaned.withColumn("vitesse", col("vitesse").cast("double"))

    if "velo" in cleaned.columns:
        cleaned = cleaned.withColumn("velo", trim(col("velo")))

    if "position" in cleaned.columns:
        cleaned = cleaned.withColumn("position", trim(col("position")))

    if "destination_finale" in cleaned.columns:
        cleaned = cleaned.withColumn("destination_finale", trim(col("destination_finale")))

    return cleaned


def clean_reparateurs(df):
    cleaned = df.dropDuplicates()

    if "timestamp" in cleaned.columns:
        cleaned = (
            cleaned
            .filter(col("timestamp").isNotNull())
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .filter(col("timestamp").isNotNull())
        )

    if "id" in cleaned.columns:
        cleaned = cleaned.withColumn("id", col("id").cast("double"))

    if "vitesse" in cleaned.columns:
        cleaned = cleaned.withColumn("vitesse", col("vitesse").cast("double"))

    if "trajet" in cleaned.columns:
        cleaned = cleaned.withColumn("trajet", trim(col("trajet")))

    if "position" in cleaned.columns:
        cleaned = cleaned.withColumn("position", trim(col("position")))

    return cleaned


def clean_prestataires(df):
    cleaned = df.dropDuplicates()

    if "id" in cleaned.columns:
        cleaned = cleaned.withColumn("id", trim(col("id")))

    if "largeur" in cleaned.columns:
        cleaned = cleaned.withColumn("largeur", col("largeur").cast("double"))

    if "hauteur" in cleaned.columns:
        cleaned = cleaned.withColumn("hauteur", col("hauteur").cast("double"))

    return cleaned


def clean_generic(df):
    return df.dropDuplicates()


def clean_all(dataframes: dict):
    cleaned = {}

    for name, df in dataframes.items():
        if name == "velos":
            cleaned[name] = clean_velos(df)

        elif name == "stations":
            cleaned[name] = clean_stations(df)

        elif name == "villes":
            cleaned[name] = clean_villes(df)

        elif name == "cyclistes":
            cleaned[name] = clean_cyclistes(df)

        elif name == "reparateurs":
            cleaned[name] = clean_reparateurs(df)

        elif name == "prestataires":
            cleaned[name] = clean_prestataires(df)

        else:
            cleaned[name] = clean_generic(df)

    return cleaned


def save_silver(dataframes: dict):
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    for name, df in dataframes.items():
        (
            df.write
            .mode("overwrite")
            .parquet(str(SILVER_DIR / name))
        )
        print(f"[SILVER] {name} sauvegardé.")