from graphframes import GraphFrame
from graphframes.lib import Pregel
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col, array, when, coalesce, array, array_append, collect_list, create_map, element_at

REPORT_PATH = "./.data/graph_output.md"

def write_text_to_file(content: str) -> None:
    with open(REPORT_PATH, "a") as f:
        f.write(content + "\n")


def write_heading_to_file(level: int, content: str) -> None:
    heading = "#" * level + " " + content + "\n\n"
    write_text_to_file(heading)

def new_file() -> None:
    with open(REPORT_PATH, "w") as f:
        f.write("")
    
spark = (
    SparkSession.builder.appName("MalRankApp")
    .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3")
    .config("spark.executor.instances", "1")
    .config("spark.checkpoint.dir", "./.data/checkpoints/")
    .getOrCreate()
)

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

v1_df = spark.createDataFrame(
    [
        ("1", "ip_address", "192.168.101.1", 0, 0.1),
        ("2", "ip_address", "192.168.101.2", 0, 0.1),
        ("3", "ip_address", "192.168.101.3", 0, 1.0),
        ("4", "ip_address", "212.59.0.1", 1, 0.9),
        ("5", "ip_address", "212.59.0.2", 1, 0.6),
    ],
    ["id", "label", "name", "is_malicious", "confidence_score"],
)

e1_df = spark.createDataFrame(
    [
        (1, 4, "connects_to", 0.0, 1.0),
        (1, 5, "connects_to", 0.0, 1.0),
        (2, 4, "connects_to", 0.0, 1.0),
        (3, 4, "connects_to", 0.0, 1.0),
    ],
    ["src", "dst", "label", "forward_weight", "backward_weight"],
)

experiments = [
    # max_iter,k, vertices, edges
    (5, 0.5, v1_df, e1_df),
    (5, 0.8, v1_df, e1_df),
    (10, 0.5, v1_df, e1_df),
    (10, 0.8, v1_df, e1_df),
]

for i, (max_iter, k, v, e) in enumerate(experiments):

    print("=" * 40)
    print(f"Experiment {i + 1}")
    print("=" * 40)
    print()
    print(f"k value: {k}\n")
    print(f"Max Iterations: {max_iter}\n")
   
    g = GraphFrame(v, e)
    print("Vertices:")
    g.vertices.show()
    print("")
    print("Edges:")
    g.edges.show()

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
    result_df.show(truncate=False)
    result_df.unpersist()