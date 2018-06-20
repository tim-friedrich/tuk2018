#!/usr/bin/env python3
import itertools
import time
import math
import re
import sys

from fractions import Fraction
import copy


class Column():
    def __init__(self, identifier, name, size, table):
        self._id = identifier
        self._name = name.upper()
        self._data_type_size = size
        self._table = table

    def size(self):
        return self._data_type_size * self._table.row_count()

    def __str__(self):
        return "C%d %s" % (self._id, self._name)

    @staticmethod
    def size_of_type(s):
        s = s.lower()
        if s == 'integer':
            return 4
        elif s == 'date':
            return 8
        elif s == 'time':
            return 8
        elif s.startswith('decimal'):
            return 8
        elif s.startswith('varchar'):
            return int(s.strip('varchar()'))
        elif s.startswith('char'):
            return int(s.strip('char()'))
        elif s.startswith('nvarchar'):
            return int(s.strip('nvarchar()'))
        elif s.startswith('varbinary'):
            return int(s.strip('varbinary()'))
        else:
            raise Exception("Unknown column type: %s", s)



class Table():
    def __init__(self, name):
        self._name = name.upper()
        self._columns = []
        self._benchmark = None

    def size(self):
        return sum([col.size() for col in self._columns])

    def row_size(self):
        return sum([col._data_type_size for col in self._columns])

    def row_count(self):
        return self._benchmark.row_count(self._name)

    def __unicode__(self):
        return self._name

    def __str__(self):
        return self._name


class Query():
    def __init__(self, nr, columns):
        self._nr = nr
        self._columns = columns
        self._load = None

    def __str__(self):
        return "Q%d" % self._nr



class TPCH():
    '''Describes a benchmark and is used as input for algorithms to calculate replica configurations.

    '''
    SIZE = {'PART': 200*1000, 'SUPPLIER': 10*1000, 'PARTSUPP': 800*1000, 'CUSTOMER': 150*1000, 'ORDERS': 1500*1000, 'LINEITEM': 6000*1000, 'NATION': 25, 'REGION': 5}

    def __init__(self, SF, tables, queries=None, updates=None):
        self._SF = SF
        self._tables = tables
        for t in self._tables:
            t._benchmark = self
        self._queries = queries if queries else []
        self._updates = updates if updates else []


    def row_count(self, table_name):
        if table_name in ['REGION', 'NATION']:
            return self.SIZE[table_name]
        else:
            return self.SIZE[table_name] * self._SF


    @staticmethod
    def parse_tables(table_file):
        with open(table_file) as f:
            table_definitions = f.read().split('\n\n')

        tables = []

        column_identifier = 0
        for table_def in table_definitions:
            tokens = table_def.split('\n')
            table_name = tokens[0]
            table = Table(table_name)

            for attribute in tokens[1:]:
                attribute_name, attribute_size = attribute.split(',')
                attribute_size = int(attribute_size)
                col = Column(column_identifier, attribute_name, attribute_size, table)
                column_identifier += 1
                table._columns.append(col)
            tables.append(table)
            # print(table._name)

        return tables


    @staticmethod
    def parse_queries(tables):
        queries = []
        for q_nr in range(1, 23):
            with open('queries/%d.sql' % q_nr) as f:
                q_text = f.read().upper()
            columns = []
            for t in tables:
                for c in t._columns:
                    if c._name in q_text:
                        columns.append(c)
            queries.append(Query(q_nr, columns))
        return queries


    @staticmethod
    def add_load(queries):
        regex = re.compile('Query: (\\d+)(?:\\.sql){0,1} performed at an average of: ([\\d.]+) seconds')
        with open(sys.argv[1]) as f:
            lines = f.read().split('\n')
            for line in lines:
                match = regex.match(line)
                if match:
                    index = int(match.group(1)) - 1
                    load = int(float(match.group(2))*100)
                    queries[index]._load = load
            sum_load = sum([q._load for q in queries])
            for q in queries:
                q._load = Fraction(q._load, sum_load)




def Benchmark_accessed_columns(benchmark):
    accessed_columns = set()
    for q in benchmark._queries:
        for c in q._columns:
            accessed_columns.add(c)
    return accessed_columns

def Benchmark_accessed_columns_queries(benchmark, query_ids):
    accessed_columns = set()
    for q_id in query_ids:
        for c in benchmark._queries[q_id]._columns:
            accessed_columns.add(c)
    return accessed_columns

def config_get_benchmarks(benchmark, config):
    #print(len(benchmark._queries))
    #print(config)
    benchmarks = []
    for backend in config:
        new_bench = copy.copy(benchmark)
        queries = []
        for q_id in backend.keys():
            #print(q_id)
            q = Query(benchmark._queries[q_id]._nr, benchmark._queries[q_id]._columns)
            q._load = benchmark._queries[q_id]._load * backend[q_id]
            queries.append(q)
        new_bench._queries = queries
        benchmarks.append(new_bench)
    return benchmarks

def config_accessed_size(benchmark, config):
    config_accessed_size = 0
    for backend in config:
        backend_accessed_columns = Benchmark_accessed_columns_queries(benchmark, backend.keys())
        backend_accessed_size = sum([c.size() for c in backend_accessed_columns])
        # print(backend_accessed_size)
        config_accessed_size += backend_accessed_size
    return config_accessed_size



def sigmod_greedy(benchmark, num_backends, load = None):
    '''
    Calculates replica configurations according to Rabl's paper.

    benchmark:
        type: class TPCH
        specifies the workload and the table instances
    num_backends:
        type: int
        specifies the number of nodes/backends to distribute the workload to
    load:
        optional list of fractions with length num_backends
        specifies unequal load distribution among backends

    returns a list of replica configurations
        each list entry is a dictionary and destribes the load for a single replica:
            the keys are the queries which are assign to the replica
            the values are the fractions of a query load which are executed by the replica
    '''
    def updates_for_query(query):
        updates = []
        for update in benchmark._updates:
            for col in update._columns:
                if col in query._columns:
                    updates.append(update)
                    break # test next update
        return updates

    query_weights = []
    query_sizes = []

    # Normalize query load
    sum_of_query_load = sum(query._load for query in benchmark._queries)
    assert type(sum_of_query_load) == Fraction

    for query in benchmark._queries:
        query_weights.append(query._load / sum_of_query_load)
        query_sizes.append(sum([column.size() for column in query._columns]))
    #print(query_sizes)
    #print(query_weights)
    queries = list(range(len(benchmark._queries)))

    rest_weights = list(query_weights)
    def get_key(i):
        query = benchmark._queries[i]
        w = rest_weights[i]
        s = query_sizes[i]
        for update in updates_for_query(query):
            w += update._load
            for col in update._columns:
                if col not in query._columns:
                    s += col.size()
        return w * s, -i
    queries.sort(key=get_key, reverse=True)
    # print(queries)
    current_load = [0 for _ in range(num_backends)]

    if load == None:
        load = [Fraction(1, num_backends) for _ in range(num_backends)]
    else:
        assert(len(load) == num_backends)
    scaled_load = list(load)

    backend_fragments = [[] for _ in range(num_backends)]
    backend_q = [[] for _ in range(num_backends)]
    backend_u = [[] for _ in range(num_backends)]

    backend_q_costs = [{} for _ in range(num_backends)]


    # print(sum(rest_weights))
    # print(scaled_load)
    # print(current_load)
    # print(rest_weights)

    while len(queries) > 0:
        #print('Q-Rest weights: %s' % rest_weights)
        query = benchmark._queries[queries[0]]
        #print('Assign query %d' % queries[0])

        def all_backends_full():
            for i in range(num_backends):
                if current_load < scaled_load:
                    return False
            return True

        if all_backends_full():
            for i in range(num_backends):
                scaled_load[i] = current_load[i] + load[i] * query_weights[queries[0]]
            #print('(%d)New scaled load %s' % (queries[0], scaled_load))
        #print(queries)
        # print(query)
        # print(scaled_load)
        # print(current_load)
        # print(rest_weights)
        # print('-----------')
        #time.sleep(1)

        # find best fitting backend for query
        differences = []
        for b in range(num_backends):
            if current_load[b] == scaled_load[b]:
                differences.append(math.inf)
            elif current_load[b] == 0:
                differences.append(0)
            else:
                difference = 0
                for column in query._columns:
                    if column not in backend_fragments[b]:
                        difference += column.size()
                differences.append(difference)
        backend = differences.index(min(differences))
        #print('Choose backend %d' % backend)
        # assign query to backend
        if queries[0] not in backend_q[backend]:
            backend_q[backend].append(queries[0])
            backend_q_costs[backend][queries[0]] = 0

            # add fragments of query
            for col in query._columns:
                if col not in backend_fragments[backend]:
                    backend_fragments[backend].append(col)
            # add fragments of related updates
            for i, update in enumerate(benchmark._updates):
                if i not in backend_u[backend]:
                    for col in update._columns:
                        if col in backend_fragments[backend]:
                            # update is related to query
                            backend_u[backend].append(i)
                            current_load[backend] += update._load
                            for c in update._columns:
                                if c not in backend_fragments[backend]:
                                    backend_fragments[backend].append(col)
                            break # test next update query
        #print('Fragments for %d: %s' % (queries[0], [col._table._name for col in backend_fragments[backend]]))

        if current_load[backend] >= scaled_load[backend]:
            scaled_load[backend] = current_load[backend] + load[backend] * query_weights[queries[0]]

        if rest_weights[queries[0]] > scaled_load[backend] - current_load[backend]:
            backend_q_costs[backend][queries[0]] += (scaled_load[backend] - current_load[backend])/ query_weights[queries[0]] #new
            rest_weights[queries[0]] -= scaled_load[backend] - current_load[backend]
            current_load[backend] = scaled_load[backend]
            queries.sort(key=get_key, reverse=True)

        else:
            current_load[backend] += rest_weights[queries[0]]
            backend_q_costs[backend][queries[0]] += rest_weights[queries[0]]/ query_weights[queries[0]] #new
            queries = queries[1:]
        #print('Current load %s' % current_load)
        #print('-------------------------------')


    # test for equal load distribution of backends
    if benchmark._updates == None:
        for b_id, b in enumerate(backend_q):
            s = 0
            for q_id in b:
                s += backend_q_costs[b_id][q_id] * query_weights[q_id]
            assert s == Fraction(1, num_backends)

    return backend_q_costs




if __name__ == '__main__':
# TPC-H
    if len(sys.argv) < 2:
        sys.exit('Path to results file missing')
    
    tables = TPCH.parse_tables('TPC-H_attribute_sizes.txt')
    queries = TPCH.parse_queries(tables)
    TPCH.add_load(queries)

    tpch = TPCH(SF=1, tables=tables, queries=queries)

    sum_size = 0
    for q in queries:
        sum_size += sum([column.size() for column in q._columns])

    overall_size = sum([t.size() for t in tables])
    print('overall_size', overall_size)

    # calculates minimum replica sizes to process n queries
    #optimum_configuration_single_replica(tpch, overall_size)

    # for t in tables:
    #     print(t._name, t.size(), "%.3f" % (t.size()/overall_size))
    # print(Column._next_id)
    #rainer(tpch)
    # overall_load = sum([q._load for q in queries])
    # print(overall_load)
    # print(optimum_configuration(tpch, overall_size))

    accessed_columns = Benchmark_accessed_columns(tpch)
    accessed_size = sum([c.size() for c in accessed_columns])
    print("sum_size", sum_size/accessed_size)
    for num_backends in [4]:
        backend_config = sigmod_greedy(tpch, num_backends)
        #print([list(backend.keys()) for backend in backend_config]) 
        size = config_accessed_size(tpch, backend_config)
        print(num_backends, size/accessed_size)
        print(backend_config)


