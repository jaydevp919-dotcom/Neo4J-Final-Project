from db.neo4j_conn import get_driver

QUERIES = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Airport) REQUIRE a.code IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Carrier) REQUIRE c.code IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (f:Flight) REQUIRE f.flight_id IS UNIQUE",
]

def main() -> None:
    driver = get_driver()
    with driver.session() as s:
        for q in QUERIES:
            s.run(q)
    driver.close()
    print("✅ setup_schema ran successfully")

if __name__ == "__main__":
    main()
