def get_vertex_df(spark):
    df = spark.createDataFrame(
        [
            (1, "ip_address", "192.168.101.1", 0, 0.1),
            (2, "ip_address", "192.168.101.2", 0, 0.1),
            (3, "ip_address", "192.168.101.3", 0, 1.0),
            (4, "ip_address", "212.59.0.1", 1, 0.9),
            (5, "ip_address", "212.59.0.2", 1, 0.6),
        ],
        ["id", "label", "name", "is_malicious", "confidence_score"]
    )
    return df

def get_edge_df(spark):
    df = spark.createDataFrame(
        [
            (1, 4, "connects_to", 0.0, 1.0),
            (1, 5, "connects_to", 0.0, 1.0),
            (2, 4, "connects_to", 0.0, 1.0),
            (3, 4, "connects_to", 0.0, 1.0),
        ],
        ["src", "dst", "label", "forward_weight", "backward_weight"],
    )
    return df