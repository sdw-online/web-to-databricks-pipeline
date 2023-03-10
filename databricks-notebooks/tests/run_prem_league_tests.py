# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Run Premier League Data Quality Tests
# MAGIC 
# MAGIC This notebook is for executing the tests for the Premier League table created in the CDC pipeline

# COMMAND ----------

import pytest
import os
import sys

# COMMAND ----------

repo_name = "tests"

# Get the path to this notebook, for example "/Workspace/Repos/{username}/{repo-name}".
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Get the repo's root directory name.
repo_root = os.path.dirname(os.path.dirname(notebook_path))

# Prepare to run pytest from the repo.
os.chdir(f"/Workspace/{repo_root}/{repo_name}")
print(os.getcwd())

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."
