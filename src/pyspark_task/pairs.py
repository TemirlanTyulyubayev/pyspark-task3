from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def get_product_category_pairs(products: DataFrame, categories: DataFrame, product_category: DataFrame) -> DataFrame:
    """
    Возвращает все пары «Имя продукта – Имя категории» и продукты без категорий.
    """
    result = (
        products.alias("p")
        .join(product_category.alias("pc"), F.col("p.id") == F.col("pc.product_id"), how="left")
        .join(categories.alias("c"), F.col("pc.category_id") == F.col("c.id"), how="left")
        .select(
            F.col("p.name").alias("product_name"),
            F.col("c.name").alias("category_name")
        )
    )
    return result