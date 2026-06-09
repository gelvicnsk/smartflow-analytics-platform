from pyspark.sql.functions import col, count, avg, min, max, hour, date_format
from src.config import GOLD_DIR


def show_basic_infos(dataframes: dict):
    for name, df in dataframes.items():
        print(f"\n========== {name.upper()} ==========")
        df.printSchema()
        print("Nombre de lignes :", df.count())
        df.show(5, truncate=False)


def run_velos_kpis(velos_df):
    print("\n========== KPI VELOS ==========")

    print("Nombre total d'enregistrements vélos :", velos_df.count())
    print("Nombre de vélos distincts :", velos_df.select("velo_id").distinct().count())

    print("\nPerformance moyenne par vélo :")
    velos_df.groupBy("velo_id") \
        .agg(
            avg("performance").alias("performance_moyenne"),
            min("performance").alias("performance_min"),
            max("performance").alias("performance_max"),
            count("*").alias("nb_mesures")
        ) \
        .orderBy(col("performance_moyenne").asc()) \
        .show(20, truncate=False)

    print("\nVélos à faible performance :")
    velos_df.filter(col("performance") < 0.5).show(20, truncate=False)

    print("\nActivité par heure :")
    velos_df.withColumn("heure", hour("timestamp")) \
        .groupBy("heure") \
        .count() \
        .orderBy("heure") \
        .show(24)


def run_stations_kpis(stations_df):
    print("\n========== KPI STATIONS ==========")

    print("Nombre total d'événements stations :", stations_df.count())

    if "station_id" in stations_df.columns:
        print("Nombre de stations distinctes :", stations_df.select("station_id").distinct().count())

    if "action" in stations_df.columns:
        print("\nRépartition des actions :")
        stations_df.groupBy("action").count().orderBy(col("count").desc()).show()

    if "station_id" in stations_df.columns:
        print("\nTop stations les plus actives :")
        stations_df.groupBy("station_id").count().orderBy(col("count").desc()).show(10)

    if "timestamp" in stations_df.columns:
        print("\nActivité par heure :")
        stations_df.withColumn("heure", hour("timestamp")) \
            .groupBy("heure") \
            .count() \
            .orderBy("heure") \
            .show(24)


def build_gold_tables(cleaned: dict):
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    if "velos" in cleaned:
        velos_gold = (
            cleaned["velos"]
            .groupBy("velo_id")
            .agg(
                avg("performance").alias("performance_moyenne"),
                min("performance").alias("performance_min"),
                max("performance").alias("performance_max"),
                count("*").alias("nb_mesures")
            )
        )

        velos_gold.write.mode("overwrite").parquet(str(GOLD_DIR / "kpi_velos"))
        print("[GOLD] kpi_velos sauvegardé.")

    if "stations" in cleaned:
        stations_df = cleaned["stations"]

        if "station_id" in stations_df.columns:
            stations_gold = (
                stations_df
                .groupBy("station_id")
                .agg(count("*").alias("nb_evenements"))
            )

            stations_gold.write.mode("overwrite").parquet(str(GOLD_DIR / "kpi_stations"))
            print("[GOLD] kpi_stations sauvegardé.")
