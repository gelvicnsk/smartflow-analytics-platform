from pyspark.sql.functions import col, when, hour
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from src.config import MODELS_DIR


def prepare_ml_dataset(velos_df):
    """
    Objectif : prédire un risque d'incident.
    Ici, on crée une variable label :
    - 1 = risque si performance < 0.5
    - 0 = normal sinon
    """

    ml_df = (
        velos_df
        .filter(col("performance").isNotNull())
        .filter(col("timestamp").isNotNull())
        .withColumn("event_hour", hour("timestamp"))
        .withColumn(
            "label",
            when(col("performance") < 0.5, 1).otherwise(0)
        )
        .select("performance", "event_hour", "label")
        .dropna()
    )

    assembler = VectorAssembler(
        inputCols=["performance", "event_hour"],
        outputCol="features"
    )

    return assembler.transform(ml_df).select("features", "label")


def evaluate_model(predictions, model_name):
    evaluator_accuracy = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )

    evaluator_precision = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedPrecision"
    )

    evaluator_recall = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="weightedRecall"
    )

    accuracy = evaluator_accuracy.evaluate(predictions)
    precision = evaluator_precision.evaluate(predictions)
    recall = evaluator_recall.evaluate(predictions)

    print(f"\n========== {model_name} ==========")
    print("Accuracy :", round(accuracy, 4))
    print("Precision :", round(precision, 4))
    print("Recall :", round(recall, 4))

    return {
        "model": model_name,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall
    }


def train_models(velos_df):
    ml_dataset = prepare_ml_dataset(velos_df)

    train_df, test_df = ml_dataset.randomSplit([0.8, 0.2], seed=42)

    models = {
        "Logistic Regression": LogisticRegression(featuresCol="features", labelCol="label"),
        "Random Forest": RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=20),
        "Decision Tree": DecisionTreeClassifier(featuresCol="features", labelCol="label"),
    }

    results = []

    for name, model in models.items():
        fitted_model = model.fit(train_df)
        predictions = fitted_model.transform(test_df)
        results.append(evaluate_model(predictions, name))

        model_path = MODELS_DIR / name.lower().replace(" ", "_")
        fitted_model.write().overwrite().save(str(model_path))
        print(f"Modèle sauvegardé : {model_path}")

    return results
