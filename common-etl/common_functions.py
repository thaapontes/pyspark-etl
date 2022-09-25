import logging
from json import dumps, loads

from pyspark.sql import DataFrame


def get_number_of_records(df: DataFrame):
    print('Total Records = {}'.format(df.count()))


def get_number_of_partitions(df: DataFrame):
    partitions = df.rdd.getNumPartitions()
    print("partitions count:" + str(partitions))


def get_number_of_partitions_after_repartition(df: DataFrame, number_of_partitions: int):
    repartitioned = df.repartition(number_of_partitions).rdd.getNumPartitions()
    print("partitions count after repartition:" + str(repartitioned))


def get_schema(df: DataFrame, schema_type: str):
    if schema_type == 'tree':
        logging.warning("*** Schema as a tree:")
        df.printSchema()
    elif schema_type == 'string':
        logging.warning("*** Schema as string: {}".format(df.schema))
    else:
        schema_as_json = df.schema.json()
        parsed_schema = loads(schema_as_json)
        logging.warning("*** Schema as JSON: {}".format(dumps(parsed_schema, indent=2)))


def get_query_plan(df: DataFrame):
    df.explain()


def query_on_df(df: DataFrame, column: str):
    df.createOrReplaceTempView("dfs")
    my_query = spark.sql('SELECT ' + column + ' FROM dfs LIMIT 3')
    my_query.show()


def load_df(df: DataFrame, df_name: str):
    df.coalesce(1).write.format('json').save(df_name + '.json')


def persist_mode(mode: str, df: DataFrame):
    """
    :param df: the dataframe will wish to persist
    :param mode: cache can be used for datasets that will be repeatedly computed and are not too large checkpoint can
    be used for datasets that takes a long time to run, or when the computing chain is too long or depends on too
    many datasets
    cache save in memory/disk and remember lineage whilst checkpoint saves to an HDFS and forgets the lineage
    resource: https://mallikarjuna_g.gitbooks.io/sparkinternals/content/cache-and-checkpoint.html
    """
    if mode == 'cache':
        df.cache()
    elif mode == 'checkpoint':
        df.checkpoint()
