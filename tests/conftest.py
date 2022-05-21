import pathlib
from typing import NamedTuple

import pytest
import shutil
from pyspark.sql import SparkSession

DELTA_JAR = (pathlib.Path(__file__).parent / "delta-core_2.12-1.2.1.jar").absolute()

class TestingContext(NamedTuple):
    data_dir: str


class SparkTestingContext(NamedTuple):
    ctx: TestingContext
    spark: SparkSession


@pytest.fixture(scope='session', autouse=True)
def testing_context(tmp_path_factory) -> TestingContext:
    data_dir = tmp_path_factory.mktemp('data_dir')

    yield TestingContext(str(data_dir))

    shutil.rmtree(data_dir)


@pytest.fixture(scope='session')
def spark_testing_context(testing_context, tmp_path_factory):

    spark = (SparkSession.builder
             .master('local[1]')
             .config('spark.driver.extraJavaOptions',
                     " ".join([
                         "-Ddelta.log.cacheSize=3",
                         "-XX:+CMSClassUnloadingEnabled",
                         "-XX:+UseCompressedOops"
                     ]))
             .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
             .config('spark.sql.catalog.spark_catalog',
                     'org.apache.spark.sql.delta.catalog.DeltaCatalog')
             .config("spark.sql.shuffle.partitions", "1")
             .config('spark.databricks.delta.snapshotPartitions', '2')
             .config('spark.ui.showConsoleProgress', 'false')
             .config('spark.ui.enabled', 'false')
             .config('spark.ui.dagGraph.retainedRootRDDs', '1')
             .config('spark.ui.retainedJobs', '1')
             .config('spark.ui.retainedStages', '1')
             .config('spark.ui.retainedTasks', '1')
             .config('spark.sql.ui.retainedExecutions', '1')
             .config('spark.worker.ui.retainedExecutors', '1')
             .config('spark.worker.ui.retainedDrivers', '1')
             .config('spark.driver.memory', '2g')
             .config('spark.sql.warehouse.dir',
                     str(tmp_path_factory.mktemp('warehouse').absolute()))
             .config('spark.jars', f'file://{DELTA_JAR}')
             .getOrCreate()
             )

    spark.sparkContext.setCheckpointDir(
        str(tmp_path_factory.mktemp('checkpoints').absolute())
    )

    yield SparkTestingContext(
        ctx=testing_context,
        spark=spark
    )

    spark.stop()
