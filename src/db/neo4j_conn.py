from neo4j import GraphDatabase, Driver

def get_driver() -> Driver:
    return GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", "neo4jpassword")
    )
