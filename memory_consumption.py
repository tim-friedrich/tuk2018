import json
import partial_replication as pr

tables = pr.TPCH.parse_tables('TPC-H_attribute_sizes.txt')
queries = pr.TPCH.parse_queries(tables)
tpch = pr.TPCH(SF=10, tables=tables, queries=queries)

with open('benchmark/config.json', 'r') as f:
    config = json.load(f)

# memory consumption for each query
print("Memory consumption per query")
for q in queries:
    print("Query", q._nr, sum([column.size() for column in q._columns]))

# full replication
full_sum = 0
for t in tables:
    full_sum += sum([column.size() for column in t._columns])
print("\nFull replication", full_sum)

# partial replication
print("\nPartial replication")
for idx, serverConf in enumerate(config['connections']):
    node_columns = set()
    node_sum = 0
    for q in queries:
        if q._nr in serverConf['queries']:
            node_columns.update(q._columns)
    print(serverConf['host'], sum([column.size() for column in node_columns]))