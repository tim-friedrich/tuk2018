import json
import partial_replication as pr

tables = pr.TPCH.parse_tables('TPC-H_attribute_sizes.txt')
queries = pr.TPCH.parse_queries(tables)
tpch = pr.TPCH(SF=10, tables=tables, queries=queries)

with open('benchmark/config.json', 'r') as f:
    config = json.load(f)

# memory consumption for each query
print("MEMORY CONSUMPTION PER QUERY")
for q in queries:
    print("\nQuery", q._nr)
    print("Size:   ", sum([column.size() for column in q._columns]))
    print("Columns:", [column._name for column in q._columns])

# full replication
full_sum = 0
for t in tables:
    full_sum += sum([column.size() for column in t._columns])
print("\n\nFULL REPLICATION")
print("Size:", full_sum)

# partial replication
print("\n\nPARTIAL REPLICATION")
for idx, serverConf in enumerate(config['connections']):
    node_columns = set()
    for q in queries:
        if q._nr in serverConf['queries']:
            node_columns.update(q._columns)
    node_sum = sum([column.size() for column in node_columns])
    print()
    print(serverConf['host'])
    print("Size:      ", node_sum)
    print("Percentage:", round(node_sum/full_sum*100, 1))