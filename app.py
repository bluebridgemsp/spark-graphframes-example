from pyspark.sql import SparkSession
from graphframes import GraphFrame

def main():

    spark = ( SparkSession.builder
        .appName("SimpleApp")
        # Add GraphFrames package to SparkSession
        .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.9.3")
        # Number of executors for parallel processing
        .config("spark.executor.instances", "3")
        .getOrCreate()
    )

    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

    # Create a Vertex DataFrame with "id" column
    v = spark.createDataFrame([
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
    ], ["id", "name", "age"])

    # Create an Edge DataFrame with "src" and "dst" columns
    e = spark.createDataFrame([
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow"),
    ], ["src", "dst", "relationship"])

    e.show()
    v.show()

    # Create a GraphFrame
    g = GraphFrame(v, e)

    # Query: Get in-degree of each vertex.
    g.inDegrees.show()

    # Query: Count the number of "follow" connections in the graph.
    count = g.edges.filter("relationship = 'follow'").count()
    print("Number of follow connections: " + str(count))

    # Run PageRank algorithm, and show results.
    results = g.pageRank(resetProbability=0.01, maxIter=20)
    results.vertices.select("id", "pagerank").show()
    
    spark.stop()

if __name__ == "__main__":
    main()
