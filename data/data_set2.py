def get_vertex_df(spark):
    df = spark.createDataFrame(
        [
            (1,  "ip_address", "192.168.101.1",  1.0,  1.0),
            (2,  "ip_address", "192.168.101.2",  0.2,  0.9),
            (3,  "ip_address", "192.168.101.3",  0.0,  0.5),
            (4,  "ip_address", "192.168.101.4",  0.1,  0.8),
            (5,  "ip_address", "192.168.101.5",  0.05, 0.9),
            (6,  "ip_address", "192.168.101.6",  0.9,  0.3),
            (7,  "ip_address", "192.168.101.7",  1.0,  1.0),
            (8,  "ip_address", "192.168.101.8",  0.5,  0.1),
            (9,  "ip_address", "192.168.101.9",  0.2,  0.8),
            (10, "ip_address", "192.168.101.10", 0.3,  0.75),
            (11, "ip_address", "192.168.101.11", 0.8,  0.7),
        ],
        ["id", "label", "name", "is_malicious", "confidence_score"]
    )
    return df

def get_edge_df(spark):
    df = spark.createDataFrame(
        [
            (1,  2, "connects_to",  1.0, 0.0),
            (1,  3, "connects_to",  1.0, 0.0),
            (2,  4, "connects_to",  0.9, 0.1),
            (2,  5, "connects_to",  0.8, 0.2),
            (3,  6, "connects_to",  0.8, 0.1),
            (3,  7, "connects_to",  0.7, 0.1),
            (4,  1, "connects_to",  0.5, 0.0),
            (4,  6, "connects_to",  1.0, 0.0),
            (5,  3, "connects_to",  1.0, 0.0),
            (5,  8, "connects_to",  0.9, 0.0),
            (6,  5, "connects_to",  0.9, 0.2),
            (6,  9, "connects_to",  0.7, 0.3),
            (7,  1, "connects_to",  0.7, 0.0),
            (7,  8, "connects_to",  1.0, 0.2),
            (8,  4, "connects_to",  0.9, 0.1),
            (8,  9, "connects_to",  1.0, 0.0),
            (9,  2, "connects_to",  1.0, 0.5),
            (9,  7, "connects_to",  0.8, 0.0),
            (10, 1, "connects_to",  1.0, 0.0),
            (10, 2, "connects_to",  1.0, 0.0),
            (11, 3, "connects_to",  1.0, 0.0),
            (11, 4, "connects_to",  1.0, 0.0),
        ],
        ["src", "dst", "label", "forward_weight", "backward_weight"],
    )
    return df