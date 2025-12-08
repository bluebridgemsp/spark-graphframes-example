def get_vertex_df(spark):
    df = spark.read.csv(
        "data/vertices.csv",
        header=True,
        inferSchema=True
    )
    return df


def get_edge_df(spark):
    df = spark.read.csv(
        "data/edges.csv",
        header=True,
        inferSchema=True
    )
    return df