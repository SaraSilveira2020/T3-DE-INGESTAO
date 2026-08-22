import argparse
import datetime as dt
import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import bson
from pymongo import MongoClient
from pymongo.collection import Collection
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

try:
    import yaml
except ImportError as exc:
    raise ImportError("PyYAML is required to read config/pipeline_config.yaml") from exc


RAW_SCHEMA = StructType(
    [
        StructField("_source_id", StringType(), False),
        StructField("_raw_document", StringType(), True),
    ]
)

WATERMARK_SCHEMA = StructType(
    [
        StructField("collection", StringType(), False),
        StructField("watermark_field", StringType(), True),
        StructField("watermark_value", StringType(), True),
        StructField("updated_at", TimestampType(), False),
        StructField("_ingestion_id", StringType(), False),
    ]
)

LOG_SCHEMA = StructType(
    [
        StructField("_ingestion_id", StringType(), False),
        StructField("collection", StringType(), False),
        StructField("load_type", StringType(), False),
        StructField("watermark_inicial", StringType(), True),
        StructField("watermark_final", StringType(), True),
        StructField("qtd_lida_origem", LongType(), False),
        StructField("qtd_gravada_destino", LongType(), False),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), False),
        StructField("duracao_seg", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("mensagem_erro", StringType(), True),
    ]
)

BRONZE_SCHEMA = StructType(
    [
        StructField("_source_id", StringType(), False),
        StructField("_raw_document", StringType(), True),
        StructField("_ingestion_id", StringType(), False),
        StructField("_ingestion_timestamp", TimestampType(), False),
        StructField("_source_path", StringType(), False),
        StructField("_load_type", StringType(), False),
        StructField("_ingestion_date", StringType(), False),
        StructField("_rescued_data", StringType(), True),
    ]
)


@dataclass(frozen=True)
class CollectionConfig:
    name: str
    load_type: str
    watermark_field: str | None
    watermark_type: str | None
    source_id_field: str
    projection: dict[str, int]
    destination: str


class JsonEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, bson.ObjectId):
            return str(obj)
        if isinstance(obj, (dt.datetime, dt.date)):
            return obj.isoformat()
        if isinstance(obj, bson.Decimal128):
            return str(obj)
        if isinstance(obj, bytes):
            return obj.hex()
        return str(obj)


def load_yaml(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def parse_collection_config(raw: dict[str, Any]) -> CollectionConfig:
    return CollectionConfig(
        name=raw["name"],
        load_type=raw["load_type"],
        watermark_field=raw.get("watermark_field"),
        watermark_type=raw.get("watermark_type"),
        source_id_field=raw.get("source_id_field", "_id"),
        projection=raw.get("projection") or {},
        destination=raw["destination"],
    )


class SecretResolver:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get(self, env_var: str, scope: str, key: str) -> str:
        value = os.getenv(env_var)
        if value:
            return value

        try:
            import IPython

            dbutils = IPython.get_ipython().user_ns["dbutils"]
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception as exc:
            raise RuntimeError(
                f"Secret {scope}/{key} not found in Databricks secrets or {env_var} env var."
            ) from exc


class ControlRepository:
    def __init__(
        self,
        spark: SparkSession,
        control_config: dict[str, Any],
        bronze_config: dict[str, Any],
    ):
        self.spark = spark
        self.catalog = bronze_config["catalog"]
        self.schema = bronze_config["schema"]
        self.log_table = control_config["table_name"]
        self.watermark_table = control_config["watermark_table_name"]

    def ensure_tables(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.log_table} (
                _ingestion_id STRING,
                collection STRING,
                load_type STRING,
                watermark_inicial STRING,
                watermark_final STRING,
                qtd_lida_origem BIGINT,
                qtd_gravada_destino BIGINT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duracao_seg DOUBLE,
                status STRING,
                mensagem_erro STRING
            )
            USING DELTA
            """
        )
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.watermark_table} (
                collection STRING,
                watermark_field STRING,
                watermark_value STRING,
                updated_at TIMESTAMP,
                _ingestion_id STRING
            )
            USING DELTA
            """
        )

    def get_watermark(self, collection: str) -> str | None:
        if not self.spark.catalog.tableExists(self.watermark_table):
            return None

        rows = (
            self.spark.table(self.watermark_table)
            .where(F.col("collection") == collection)
            .orderBy(F.col("updated_at").desc())
            .limit(1)
            .collect()
        )
        return rows[0]["watermark_value"] if rows else None

    def write_watermark(
        self,
        collection: str,
        watermark_field: str,
        watermark_value: str | None,
        ingestion_id: str,
    ) -> None:
        if watermark_value is None:
            return

        row = [(collection, watermark_field, watermark_value, dt.datetime.utcnow(), ingestion_id)]
        df = self.spark.createDataFrame(row, WATERMARK_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self.watermark_table)

    def write_log(self, log: dict[str, Any]) -> None:
        row = [
            (
                log["_ingestion_id"],
                log["collection"],
                log["load_type"],
                log["watermark_inicial"],
                log["watermark_final"],
                int(log["qtd_lida_origem"]),
                int(log["qtd_gravada_destino"]),
                log["start_time"],
                log["end_time"],
                float(log["duracao_seg"]),
                log["status"],
                log["mensagem_erro"],
            )
        ]
        df = self.spark.createDataFrame(row, LOG_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(self.log_table)


class MongoExtractor:
    def __init__(self, uri: str, database: str, batch_size: int, retry_config: dict[str, Any]):
        self.database = database
        self.batch_size = batch_size
        self.retry_config = retry_config
        self.client = MongoClient(
            uri,
            serverSelectionTimeoutMS=15_000,
            socketTimeoutMS=300_000,
            appName="sample-mflix-bronze-ingestion",
        )

    def collection(self, collection_name: str) -> Collection:
        return self.client[self.database][collection_name]

    def parse_watermark(self, config: CollectionConfig, watermark: str) -> Any:
        if config.watermark_type == "datetime":
            normalized = watermark.replace("Z", "+00:00")
            parsed = dt.datetime.fromisoformat(normalized)
            if parsed.tzinfo:
                return parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
            return parsed
        return watermark

    def build_filter(self, config: CollectionConfig, watermark: str | None) -> dict[str, Any]:
        if config.load_type != "incremental" or not config.watermark_field or watermark is None:
            return {}
        return {config.watermark_field: {"$gt": self.parse_watermark(config, watermark)}}

    def count(self, config: CollectionConfig, watermark: str | None) -> int:
        return self.collection(config.name).count_documents(self.build_filter(config, watermark))

    def max_watermark(self, config: CollectionConfig, query_filter: dict[str, Any]) -> str | None:
        if not config.watermark_field:
            return None

        pipeline = [
            {"$match": query_filter},
            {"$group": {"_id": None, "max_watermark": {"$max": f"${config.watermark_field}"}}},
        ]
        result = next(self.collection(config.name).aggregate(pipeline), None)
        if not result:
            return None
        value = result.get("max_watermark")
        if isinstance(value, (dt.datetime, dt.date)):
            return value.isoformat()
        return str(value) if value is not None else None

    def iter_batches(
        self, config: CollectionConfig, watermark: str | None
    ) -> Iterable[list[tuple[str, str]]]:
        query_filter = self.build_filter(config, watermark)
        cursor = self.collection(config.name).find(
            filter=query_filter,
            projection=config.projection,
            batch_size=self.batch_size,
        )

        batch: list[tuple[str, str]] = []
        for document in cursor:
            source_id = str(document.get(config.source_id_field, ""))
            raw_document = json.dumps(document, cls=JsonEncoder, ensure_ascii=False)
            batch.append((source_id, raw_document))

            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def close(self) -> None:
        self.client.close()


class BronzeLoader:
    def __init__(self, spark: SparkSession, pipeline_config: dict[str, Any]):
        self.spark = spark
        self.source_system = pipeline_config["pipeline"]["source_system"]
        self.partition_column = pipeline_config["bronze"]["partition_column"]

    def build_bronze_df(
        self,
        rows: list[tuple[str, str]],
        ingestion_id: str,
        load_type: str,
    ) -> DataFrame:
        ingestion_timestamp = dt.datetime.utcnow()
        ingestion_date = ingestion_timestamp.date().isoformat()

        return (
            self.spark.createDataFrame(rows, schema=RAW_SCHEMA)
            .withColumn("_ingestion_id", F.lit(ingestion_id))
            .withColumn("_ingestion_timestamp", F.lit(ingestion_timestamp).cast("timestamp"))
            .withColumn("_source_path", F.lit(self.source_system))
            .withColumn("_load_type", F.lit(load_type))
            .withColumn("_ingestion_date", F.lit(ingestion_date))
            .withColumn("_rescued_data", F.lit(None).cast("string"))
        )

    def ensure_table(self, destination: str) -> None:
        if self.spark.catalog.tableExists(destination):
            return

        empty_df = self.spark.createDataFrame([], BRONZE_SCHEMA)
        (
            empty_df.write.format("delta")
            .mode("append")
            .partitionBy(self.partition_column)
            .saveAsTable(destination)
        )

    def write(self, df: DataFrame, destination: str) -> int:
        rows_written = df.count()
        (
            df.write.format("delta")
            .mode("append")
            .partitionBy(self.partition_column)
            .saveAsTable(destination)
        )
        return rows_written


class QualityValidator:
    def __init__(self, quality_config: dict[str, Any]):
        self.quality_config = quality_config

    def validate_batch(self, df: DataFrame) -> None:
        null_ids = df.where(F.col("_source_id").isNull() | (F.col("_source_id") == "")).count()
        if self.quality_config["fail_on_null_source_id"] and null_ids > 0:
            raise ValueError(f"Batch has {null_ids} records with null _source_id.")

        duplicate_ids = (
            df.groupBy("_source_id")
            .count()
            .where(F.col("count") > 1)
            .limit(1)
            .count()
        )
        if self.quality_config["fail_on_duplicate_source_id_in_batch"] and duplicate_ids > 0:
            raise ValueError("Batch has duplicated _source_id values.")


class IngestionJob:
    def __init__(self, pipeline_config_path: str, collections_config_path: str):
        self.spark = SparkSession.builder.getOrCreate()
        self.pipeline_config = load_yaml(pipeline_config_path)
        self.collections_config = load_json(collections_config_path)

        mongodb_config = self.pipeline_config["mongodb"]
        uri = SecretResolver(self.spark).get(
            env_var=mongodb_config["connection_env_var"],
            scope=mongodb_config["connection_secret_scope"],
            key=mongodb_config["connection_secret_key"],
        )
        self.control = ControlRepository(
            self.spark,
            self.pipeline_config["control"],
            self.pipeline_config["bronze"],
        )
        self.extractor = MongoExtractor(
            uri=uri,
            database=self.collections_config["database"],
            batch_size=self.pipeline_config["mongodb"]["default_batch_size"],
            retry_config=self.pipeline_config["mongodb"]["retry"],
        )
        self.loader = BronzeLoader(self.spark, self.pipeline_config)
        self.validator = QualityValidator(self.pipeline_config["quality"])

    def run(self, selected_collection: str | None = None) -> None:
        self.control.ensure_tables()
        collections = [
            parse_collection_config(raw) for raw in self.collections_config["collections"]
        ]

        for collection_config in collections:
            if selected_collection and collection_config.name != selected_collection:
                continue
            self.run_collection(collection_config)

        self.extractor.close()

    def run_collection(self, config: CollectionConfig) -> None:
        ingestion_id = str(uuid.uuid4())
        start_time = dt.datetime.utcnow()
        start_monotonic = time.monotonic()
        initial_watermark = self.control.get_watermark(config.name)
        query_filter = self.extractor.build_filter(config, initial_watermark)

        rows_read = 0
        rows_written = 0
        status = "SUCCESS"
        error_message = None
        final_watermark = None

        try:
            self.loader.ensure_table(config.destination)
            source_count = self.extractor.count(config, initial_watermark)
            final_watermark = self.extractor.max_watermark(config, query_filter)

            for batch in self.extractor.iter_batches(config, initial_watermark):
                rows_read += len(batch)
                df = self.loader.build_bronze_df(batch, ingestion_id, config.load_type)
                self.validator.validate_batch(df)
                rows_written += self.loader.write(df, config.destination)

            if rows_read != source_count:
                status = "PARTIAL"
                error_message = (
                    f"Read {rows_read} records but source count was {source_count}."
                )

            if status == "SUCCESS":
                self.control.write_watermark(
                    config.name,
                    config.watermark_field or "",
                    final_watermark,
                    ingestion_id,
                )
        except Exception as exc:
            status = "FAILED"
            error_message = str(exc)
            raise
        finally:
            end_time = dt.datetime.utcnow()
            self.control.write_log(
                {
                    "_ingestion_id": ingestion_id,
                    "collection": config.name,
                    "load_type": config.load_type,
                    "watermark_inicial": initial_watermark,
                    "watermark_final": final_watermark,
                    "qtd_lida_origem": rows_read,
                    "qtd_gravada_destino": rows_written,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duracao_seg": round(time.monotonic() - start_monotonic, 3),
                    "status": status,
                    "mensagem_erro": error_message,
                }
            )


def default_path(relative_path: str) -> str:
    project_root = Path(__file__).resolve().parents[1]
    return str(project_root / relative_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest sample_mflix collections to Bronze.")
    parser.add_argument(
        "--pipeline-config",
        default=default_path("config/pipeline_config.yaml"),
        help="Path to the pipeline YAML config.",
    )
    parser.add_argument(
        "--collections-config",
        default=default_path("config/collections.json"),
        help="Path to the collections JSON config.",
    )
    parser.add_argument(
        "--collection",
        default=None,
        help="Optional collection name. If omitted, all configured collections are processed.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    IngestionJob(args.pipeline_config, args.collections_config).run(args.collection)
