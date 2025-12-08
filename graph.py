from graphframes import GraphFrame
from graphframes.lib import Pregel
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col, array, when, coalesce, array
from data import data_set1, data_set2, data_set3


def main():
    spark = (
        SparkSession.builder.appName("MalRankApp")
        .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3")
        .config("spark.executor.instances", "1")
        .config("spark.checkpoint.dir", "./.data/checkpoints/")
        .getOrCreate()
    )

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

    experiments = [
        # max_iter,k, vertices, edges
        # Data Set 1
        # (5, 0.5, data_set1.get_vertex_df(spark), data_set1.get_edge_df(spark)),
        # Data Set 2
        # (3, 0.5, data_set2.get_vertex_df(spark), data_set2.get_edge_df(spark)),
        # (4, 0.5, data_set2.get_vertex_df(spark), data_set2.get_edge_df(spark)),
        # (5, 0.5, data_set2.get_vertex_df(spark), data_set2.get_edge_df(spark)),
        # (5, 0.65, data_set2.get_vertex_df(spark), data_set2.get_edge_df(spark)),
        # (5, 0.8, data_set2.get_vertex_df(spark), data_set2.get_edge_df(spark)),
        # Data Set 3
        (3, 0.5, data_set3.get_vertex_df(spark), data_set3.get_edge_df(spark)),
        (5, 0.5, data_set3.get_vertex_df(spark), data_set3.get_edge_df(spark)),
        (10, 0.5, data_set3.get_vertex_df(spark), data_set3.get_edge_df(spark)),
        (3, 0.8, data_set3.get_vertex_df(spark), data_set3.get_edge_df(spark)),
        (5, 0.8, data_set3.get_vertex_df(spark), data_set3.get_edge_df(spark)),
        (10, 0.8, data_set3.get_vertex_df(spark), data_set3.get_edge_df(spark)),
    ]

    for i, (max_iter, k, v, e) in enumerate(experiments):

        print("=" * 40)
        print(f"Experiment {i + 1}")
        print("=" * 40)
        print()
        print(f"k value: {k}\n")
        print(f"Max Iterations: {max_iter}\n")
    
        g = GraphFrame(v, e)
        # print("Vertices:")
        # g.vertices.show(v.count(), truncate=False)
        # print("")
        # print("Edges:")
        # g.edges.show(e.count(), truncate=False)

        backward_message_expr = (
            when(
                Pregel.edge("backward_weight") == 0.0,
                None
            ).otherwise(
                array(
                    Pregel.dst("mal_rank_score"),
                    k * Pregel.dst("mal_rank_score") + (1 - k) * Pregel.edge("backward_weight")
                )
            )
        )

        forward_message_expr = (
            when(
                Pregel.edge("forward_weight") == 0.0,
                None
            ).otherwise(
                array(
                Pregel.src("mal_rank_score"),
                k * Pregel.src("mal_rank_score") + (1 - k) * Pregel.edge("forward_weight") 
                )
            )
        )

        message_agg_expr = sum(Pregel.msg().getItem(0) * Pregel.msg().getItem(1)) / sum(Pregel.msg().getItem(1))

        initial_value_expr = col("is_malicious") * col("confidence_score")

        update_expr = col("is_malicious") * col("confidence_score") + (1 - col("confidence_score")) * coalesce(Pregel.msg(), lit(0.0))

        result_df = (
            g.pregel
                .sendMsgToSrc(backward_message_expr)
                .sendMsgToDst(forward_message_expr)
                .aggMsgs(message_agg_expr)
                .withVertexColumn(
                    "mal_rank_score",
                    initial_value_expr,
                    update_expr
                )
                .setMaxIter(max_iter)
                .run()
        )

        print("Result: Vertices with MalRank results:")
        result_df.show(result_df.count(), truncate=False)
        result_df.unpersist()

    spark.stop()

if __name__ == "__main__":
    main()