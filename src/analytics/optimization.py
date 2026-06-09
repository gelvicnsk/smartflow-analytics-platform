import time
from src.config import PARQUET_DIR


def measure_time(label, function):
    start = time.time()
    result = function()
    end = time.time()
    print(f"{label} : {round(end - start, 4)} secondes")
    return result


def test_partitions(df, name):
    print(f"\n========== PARTITIONS {name.upper()} ==========")

    print("Partitions initiales :", df.rdd.getNumPartitions())

    repartitioned = df.repartition(4)
    print("Après repartition(4) :", repartitioned.rdd.getNumPartitions())

    coalesced = repartitioned.coalesce(2)
    print("Après coalesce(2) :", coalesced.rdd.getNumPartitions())


def test_cache(df, name):
    print(f"\n========== CACHE {name.upper()} ==========")

    measure_time("Premier count sans cache", lambda: df.count())

    cached_df = df.cache()
    measure_time("Premier count avec cache", lambda: cached_df.count())
    measure_time("Deuxième count avec cache", lambda: cached_df.count())

    cached_df.unpersist()


def save_parquet_for_comparison(dataframes: dict):
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    for name, df in dataframes.items():
        path = PARQUET_DIR / name
        measure_time(
            f"Sauvegarde Parquet {name}",
            lambda df=df, path=path: df.write.mode("overwrite").parquet(str(path))
        )
