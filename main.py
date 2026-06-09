from src.spark_session import get_spark
from src.ingestion.load_data import load_all_raw_data, save_bronze
from src.cleaning.clean_data import clean_all, save_silver
from src.analytics.kpi_analysis import (
    show_basic_infos,
    run_velos_kpis,
    run_stations_kpis,
    build_gold_tables,
)
from src.analytics.spark_sql_analysis import create_sql_views, run_sql_queries
from src.analytics.optimization import (
    test_partitions,
    test_cache,
    save_parquet_for_comparison,
)
from src.ml.incident_prediction import train_models


def main():
    spark = get_spark()

    print("\n========== CHARGEMENT RAW ==========")
    raw_data = load_all_raw_data(spark)
    show_basic_infos(raw_data)

    print("\n========== SAUVEGARDE BRONZE ==========")
    save_bronze(raw_data)

    print("\n========== NETTOYAGE SILVER ==========")
    cleaned_data = clean_all(raw_data)
    save_silver(cleaned_data)

    print("\n========== ANALYTICS ==========")
    if "velos" in cleaned_data:
        run_velos_kpis(cleaned_data["velos"])

    if "stations" in cleaned_data:
        run_stations_kpis(cleaned_data["stations"])

    print("\n========== SQL ==========")
    create_sql_views(cleaned_data)
    run_sql_queries(spark)

    print("\n========== GOLD ==========")
    build_gold_tables(cleaned_data)

    print("\n========== OPTIMISATION ==========")
    for name, df in cleaned_data.items():
        test_partitions(df, name)
        test_cache(df, name)

    save_parquet_for_comparison(cleaned_data)

    print("\n========== MACHINE LEARNING ==========")
    if "velos" in cleaned_data:
        train_models(cleaned_data["velos"])

    spark.stop()


if __name__ == "__main__":
    main()
