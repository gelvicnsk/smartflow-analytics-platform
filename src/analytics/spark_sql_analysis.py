def create_sql_views(dataframes: dict):
    """
    Création des vues temporaires Spark SQL
    """

    for name, df in dataframes.items():
        df.createOrReplaceTempView(name)
        print(f"Vue SQL créée : {name}")


def run_sql_queries(spark):

    print("\n========== SQL ANALYSIS ==========")

    tables = [row.name for row in spark.catalog.listTables()]

    print("Tables disponibles :")
    print(tables)

    # ---------------------------------------------------
    # VELOS
    # ---------------------------------------------------

    if "velos" in tables:

        print("\n--- Statistiques globales vélos ---")

        spark.sql("""
            SELECT
                COUNT(*) AS total_mesures,
                COUNT(DISTINCT velo_id) AS total_velos,
                ROUND(AVG(performance),4) AS performance_moyenne
            FROM velos
        """).show()

        print("\n--- Top 10 vélos les moins performants ---")

        spark.sql("""
            SELECT
                velo_id,
                ROUND(AVG(performance),4) AS performance_moyenne,
                COUNT(*) AS nb_mesures
            FROM velos
            GROUP BY velo_id
            ORDER BY performance_moyenne ASC
            LIMIT 10
        """).show()

    # ---------------------------------------------------
    # STATIONS
    # ---------------------------------------------------

    if "stations" in tables:

        print("\n--- Stations les plus actives ---")

        spark.sql("""
            SELECT
                station_id,
                COUNT(*) AS nb_evenements
            FROM stations
            GROUP BY station_id
            ORDER BY nb_evenements DESC
            LIMIT 10
        """).show()

        print("\n--- Répartition des actions ---")

        spark.sql("""
            SELECT
                action,
                COUNT(*) AS total
            FROM stations
            GROUP BY action
            ORDER BY total DESC
        """).show()

    # ---------------------------------------------------
    # CYCLISTES
    # ---------------------------------------------------

    if "cyclistes" in tables:

        print("\n--- Cyclistes les plus rapides ---")

        spark.sql("""
            SELECT
                id,
                ROUND(MAX(vitesse),4) AS vitesse_max
            FROM cyclistes
            GROUP BY id
            ORDER BY vitesse_max DESC
            LIMIT 10
        """).show()

    # ---------------------------------------------------
    # REPARATEURS
    # ---------------------------------------------------

    if "reparateurs" in tables:

        print("\n--- Réparateurs les plus mobiles ---")

        spark.sql("""
            SELECT
                id,
                ROUND(MAX(vitesse),4) AS vitesse_max
            FROM reparateurs
            GROUP BY id
            ORDER BY vitesse_max DESC
            LIMIT 10
        """).show()

    print("\n========== FIN SQL ==========")