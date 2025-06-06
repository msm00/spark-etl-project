from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lower, concat_ws, regexp_replace


def transform_df(df: DataFrame) -> DataFrame:
    """Apply ETL transformations to the input DataFrame."""
    return df.select(
        "first_name",
        "last_name",
        "email",
        lower(
            regexp_replace(
                concat_ws("_", col("first_name"), col("last_name")), "\\s+", "_"
            )
        ).alias("username"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
    )

def main() -> None:
    """Run the ETL process."""
    spark = (
        SparkSession.builder.appName("CSV to PostgreSQL ETL")
        .config("spark.driver.extraClassPath", "/app/postgresql-42.2.5.jar")
        .getOrCreate()
    )

    try:
        # Načtení CSV souboru do DataFrame
        df = spark.read.csv("/app/data/users.csv", header=True, inferSchema=True)

        # Aplikace transformací
        transformed_df = transform_df(df)

        # Definice parametrů pro připojení k PostgreSQL
        db_properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver",
        }
        db_url = "jdbc:postgresql://postgres:5432/etl_db"

        # Uložení transformovaných dat do PostgreSQL
        transformed_df.write.jdbc(
            url=db_url, table="users", mode="append", properties=db_properties
        )

        print("Data byla úspěšně načtena do tabulky 'users' v PostgreSQL.")
    except Exception as e:  # pragma: no cover - logging
        print(f"Chyba při zpracování dat: {str(e)}")
        raise e
    finally:
        # Ukončení SparkSession
        spark.stop()


if __name__ == "__main__":
    main()
