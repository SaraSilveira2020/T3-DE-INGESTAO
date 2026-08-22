# Databricks notebook source
# MAGIC %pip install pymongo pyyaml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = "/Workspace" + notebook_path.rsplit("/", 2)[0]

sys.path.append(repo_root)

from jobs.ingestion_job import IngestionJob

job = IngestionJob(
    f"{repo_root}/config/pipeline_config.yaml",
    f"{repo_root}/config/collections.json",
)

job.run("comments")

# COMMAND ----------

spark.table("workspace.bronze.comments").display()
spark.table("workspace.bronze.control_ingestion_log").display()
