'''
Graph Database experiments on TPC-H benchmark dataset

Update Propagation
'''


import random # used to split up departments randomly
import math # math module

import xlsxwriter # writing to excel

from neo4j import GraphDatabase

from datetime import datetime

import time # use for benchmarking code and finding bottlenecks





# databse class
class gdbms_test:


    def __init__(self, uri, user, password, database):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        self.driver.close()

    def reset(self):
        with self.driver.session(database = self.database) as session:
            session.run("MATCH (m) DETACH DELETE m")

    def execute_query(self, query):
        with self.driver.session(database = self.database) as session:
            session.run(query)

    def execute_query_with_output(self, query):
        with self.driver.session(database = self.database) as session:
            record = session.run(query)
        return record

    def execute_query_with_output_result(self, query):
        with self.driver.session(database = self.database) as session:
            record = session.run(query)
            return [dict(i) for i in record]



####################################################

# performing queries



# getting db_hits

def sum_db_hits(profile):
    return (profile.get("dbHits", 0) + sum(map(sum_db_hits, profile.get("children", []))))


# getting execution time

def sum_time(profile):
    return (profile.get("time", 0) + sum(map(sum_time, profile.get("children", []))))


def show_query_details(database, query):

    
    new_db = database
    
    result = new_db.execute_query_with_output(query)

    summary = result.consume().profile

    return  (sum_db_hits(summary), sum_time(summary))


#####################################

# write to excel file

def write_to_excel(filename, sheetname, experiment_name, heading: list, content: list):

    # create sheet
    workbook = xlsxwriter.Workbook(filename)
    worksheet = workbook.add_worksheet(sheetname)
    
    # write experiment name
    worksheet.write(0,0, experiment_name)

    
    # write heading (list), give information on variables, etc
    row_start = 2

    for row in range(len(heading)):
            worksheet.write(row_start + row, 0, heading[row])

    # write content (content list of iterables)
    row_start += (2 + len(heading))
    column_start = 0

    for row in range(len(content)):
        for column in range(len(content[row])):
            worksheet.write(row_start + row, column_start + column, content[row][column])

    # close workbook
    workbook.close()



#########################


#################################################################################################
#    
# TPC-H benchmark queries
#
#################################################################################################

# Query 1

query_1_relational = """PROFILE MATCH (l:LINEITEM)
                        WHERE l.l_shipdate <= date('1998-12-01') - duration('P'+toString(toInteger(floor((1+rand())*60)))+'D')
                        WITH l.l_returnflag AS returnflag,
                            l.l_linestatus AS linestatus,
                            sum(l.l_quantity) AS sum_qty,
                            sum(l.l_extendedprice) AS sum_base_price,
                            sum(l.l_extendedprice * (1 - l.l_discount)) AS sum_disc_price,
                            sum(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)) AS sum_charge,
                            avg(l.l_quantity) AS avg_qty,
                            avg(l.l_extendedprice) AS avg_price,
                            avg(l.l_discount) AS avg_disc,
                            COUNT(*) AS count_order
                        RETURN returnflag,
                            linestatus,
                            sum_qty,
                            sum_base_price,
                            sum_disc_price,
                            sum_charge,
                            avg_qty,
                            avg_price,
                            avg_disc,
                            count_order
                        ORDER BY returnflag, linestatus;
                        """

query_1_mixed = query_1_relational

query_1_graph = query_1_relational


# Query 2

query_2_relational = """PROFILE WITH ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST'] as regions, toInteger(floor(5*rand())) AS position,
                    ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER'] as typess, toInteger(floor(5*rand())) AS type_position
                    WITH regions[position] AS selected_region, typess[type_position] AS selected_type, toInteger(floor(50*rand())) AS size 
                    MATCH (ps:PARTSUPP), (p:PART), (s:SUPPLIER), (n:NATION), (r:REGION)
                    WHERE ps.ps_partkey = p.p_partkey
                    AND ps.ps_suppkey = s.s_suppkey
                    AND s.s_nationkey = n.n_nationkey
                    AND n.n_regionkey = r.r_regionkey
                    AND p.p_size = size
                    AND p.p_type ENDS WITH selected_type
                    AND r.r_name = selected_region
                    WITH p.p_partkey AS partkey, MIN(ps.ps_supplycost) AS min_cost, selected_region
                    MATCH (ps:PARTSUPP), (p:PART), (s:SUPPLIER), (n:NATION), (r:REGION)
                    WHERE p.p_partkey = partkey
                    AND ps.ps_partkey = p.p_partkey
                    AND ps.ps_suppkey = s.s_suppkey
                    AND ps.ps_supplycost = min_cost
                    AND s.s_nationkey = n.n_nationkey
                    AND n.n_regionkey = r.r_regionkey
                    AND r.r_name = selected_region
                    RETURN s.s_acctbal AS s_acctbal, 
                        s.s_name AS s_name, 
                        n.n_name AS n_name,
                        p.p_partkey AS p_partkey, 
                        p.p_mfgr AS p_mfgr, 
                        s.s_address AS s_address,
                        s.s_phone AS s_phone, 
                        s.s_comment AS s_comment
                    ORDER BY s_acctbal DESC, n_name, s_name, p_partkey"""

query_2_mixed = """PROFILE WITH ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST'] as regions, toInteger(floor(5*rand())) AS position,
                ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER'] as typess, toInteger(floor(5*rand())) AS type_position
                WITH regions[position] AS selected_region, typess[type_position] AS selected_type, toInteger(floor(50*rand())) AS size 
                MATCH (ps:PARTSUPP), (p:PART), (s:SUPPLIER)-[SUPPLIER_NATION]->(n:NATION)-[:NATION_REGION]->(r:REGION)
                WHERE ps.ps_partkey = p.p_partkey
                AND ps.ps_suppkey = s.s_suppkey
                AND p.p_size = size
                AND p.p_type ENDS WITH selected_type
                AND r.r_name = selected_region
                WITH p.p_partkey AS partkey, MIN(ps.ps_supplycost) AS min_cost, selected_region
                MATCH (ps:PARTSUPP), (p:PART), (s:SUPPLIER)-[SUPPLIER_NATION]->(n:NATION)-[:NATION_REGION]->(r:REGION)
                WHERE p.p_partkey = partkey
                AND ps.ps_partkey = p.p_partkey
                AND ps.ps_suppkey = s.s_suppkey
                AND r.r_name = selected_region
                RETURN s.s_acctbal AS s_acctbal, 
                    s.s_name AS s_name, 
                    n.n_name AS n_name,
                    p.p_partkey AS p_partkey, 
                    p.p_mfgr AS p_mfgr, 
                    s.s_address AS s_address,
                    s.s_phone AS s_phone, 
                    s.s_comment AS s_comment
                ORDER BY s_acctbal DESC, n_name, s_name, p_partkey"""

query_2_graph = """PROFILE WITH ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST'] as regions, toInteger(floor(5*rand())) AS position,
                ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER'] as typess, toInteger(floor(5*rand())) AS type_position
                WITH regions[position] AS selected_region, typess[type_position] AS selected_type, toInteger(floor(50*rand())) AS size 
                MATCH (p:PART)<-[:PARTSUPP_PART]-(ps:PARTSUPP)-[PARTSUPP_SUPPLIER]->(s:SUPPLIER)-[SUPPLIER_NATION]->(n:NATION)-[:NATION_REGION]->(r:REGION)
                WHERE p.p_size = size
                AND p.p_type ENDS WITH selected_type
                AND r.r_name = selected_region
                WITH p.p_partkey AS partkey, MIN(ps.ps_supplycost) AS min_cost, selected_region
                MATCH (p:PART)<-[:PARTSUPP_PART]-(ps:PARTSUPP)-[PARTSUPP_SUPPLIER]->(s:SUPPLIER)-[SUPPLIER_NATION]->(n:NATION)-[:NATION_REGION]->(r:REGION)
                WHERE p.p_partkey = partkey
                AND r.r_name = selected_region
                RETURN s.s_acctbal AS s_acctbal, e
                    s.s_name AS s_name, 
                    n.n_name AS n_name,
                    p.p_partkey AS p_partkey, 
                    p.p_mfgr AS p_mfgr, 
                    s.s_address AS s_address,
                    s.s_phone AS s_phone, 
                    s.s_comment AS s_comment
                ORDER BY s_acctbal DESC, n_name, s_name, p_partkey"""


# Query 3

query_3_relational = """PROFILE WITH ['BUILDING', 'AUTOMOBILE', 'MACHINERY', 'HOUSEHOLD', 'FURNITURE'] AS segments
                        WITH segments[toInteger(floor(5*rand()))] AS segment,
                        date('1995-03-01') + duration('P'+toString(toInteger(floor((rand())*30)))+'D') AS selected_date
                        MATCH (o:ORDERS), (c:CUSTOMER), (l:LINEITEM)
                        WHERE c.c_mktsegment = segment
                            AND o.o_custkey = c.c_custkey
                            AND l.l_orderkey = o.o_orderkey
                            AND o.o_orderdate < selected_date
                            AND l.l_shipdate > selected_date
                        WITH l.l_orderkey AS orderkey, 
                            SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, 
                            o.o_orderdate AS orderdate, 
                            o.o_shippriority AS shippriority
                        RETURN orderkey,
                            orderdate,
                            shippriority,
                            revenue
                        ORDER BY revenue DESC, orderdate"""

query_3_mixed = """PROFILE WITH ['BUILDING', 'AUTOMOBILE', 'MACHINERY', 'HOUSEHOLD', 'FURNITURE'] AS segments
                    WITH segments[toInteger(floor(5*rand()))] AS segment, date('1995-03-01') + duration('P'+toString(toInteger(floor((rand())*30)))+'D') AS selected_date
                    MATCH (o:ORDERS)-[:ORDERS_CUSTOMER]->(c:CUSTOMER), (l:LINEITEM)
                    WHERE 
                    c.c_mktsegment = segment
                    AND l.l_orderkey = o.o_orderkey
                    AND o.o_orderdate < selected_date
                    AND l.l_shipdate > selected_date
                    WITH l.l_orderkey AS orderkey, 
                        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, 
                        o.o_orderdate AS orderdate, 
                        o.o_shippriority AS shippriority
                    RETURN orderkey, orderdate, shippriority, revenue
                    ORDER BY revenue DESC, orderdate"""

query_3_graph = """PROFILE WITH ['BUILDING', 'AUTOMOBILE', 'MACHINERY', 'HOUSEHOLD', 'FURNITURE'] AS segments
                    WITH segments[toInteger(floor(5*rand()))] AS segment, date('1995-03-01') + duration('P'+toString(toInteger(floor((rand())*30)))+'D') AS selected_date
                    MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)-[:ORDERS_CUSTOMER]->(c:CUSTOMER)
                    WHERE 
                    c.c_mktsegment = segment
                    AND o.o_orderdate < selected_date
                    AND l.l_shipdate > selected_date
                    WITH o.o_orderkey AS orderkey, 
                        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue, 
                        o.o_orderdate AS orderdate, 
                        o.o_shippriority AS shippriority
                    RETURN orderkey, orderdate, shippriority, revenue
                    ORDER BY revenue DESC, orderdate"""


# Query 4

query_4_relational = """PROFILE WITH toInteger(1+floor(9*rand())) AS month
                        MATCH (o:ORDERS), (l:LINEITEM)
                        WHERE o.o_orderkey = l.l_orderkey
                        AND o.o_orderdate >= date('1993-' + month + '-01')
                        AND o.o_orderdate < date('1993-' + (month + 3) + '-01')
                        AND l.l_commitdate < l.l_receiptdate
                        WITH o.o_orderpriority AS orderpriority, COUNT(DISTINCT(o)) AS order_count
                        RETURN orderpriority, order_count
                        ORDER BY orderpriority"""

query_4_mixed = query_4_relational

query_4_graph = """PROFILE WITH toInteger(1+floor(9*rand())) AS month
                    MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
                    WHERE o.o_orderdate >= date('1993-' + month + '-01')
                    AND o.o_orderdate < date('1993-' + (month + 3) + '-01')
                    AND l.l_commitdate < l.l_receiptdate
                    WITH o.o_orderpriority AS orderpriority, COUNT(DISTINCT(o)) AS order_count
                    RETURN orderpriority, order_count
                    ORDER BY orderpriority"""


 # Query 5

query_5_relational = """PROFILE WITH ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST'] as regions, toInteger(floor(5*rand())) AS position
                        WITH regions[position] AS selected_region, 1993 + toInteger(floor(4*rand())) AS selected_year
                        MATCH (c:CUSTOMER), (o:ORDERS), (l:LINEITEM), (s:SUPPLIER), (n:NATION), (r:REGION)
                        WHERE c.c_custkey = o.o_custkey
                        AND l.l_orderkey = o.o_orderkey
                        AND l.l_suppkey = s.s_suppkey
                        AND c.c_nationkey = s.s_nationkey
                        AND s.s_nationkey = n.n_nationkey
                        AND n.n_regionkey = r.r_regionkey
                        AND r.r_name = selected_region
                        AND o.o_orderdate >= date(selected_year + '-01-01')
                        AND o.o_orderdate < date((selected_year + 1) + '-01-01')
                        WITH n.n_name AS nation, 
                            SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
                        RETURN nation, revenue
                        ORDER BY revenue DESC"""

query_5_mixed = """PROFILE WITH ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST'] as regions, toInteger(floor(5*rand())) AS position
                    WITH regions[position] AS selected_region, 1993 + toInteger(floor(4*rand())) AS selected_year
                    MATCH (o:ORDERS)-[:ORDERS_CUSTOMER]->(c:CUSTOMER)-[:CUSTOMER_NATION]->(n:NATION)-[:NATION_REGION]->(r:REGION),
                        (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP), (s:SUPPLIER)-[SUPPLIER_NATION]->(n:NATION)
                    WHERE l.l_orderkey = o.o_orderkey
                    AND ps.ps_suppkey = s.s_suppkey
                    AND r.r_name = selected_region
                    AND o.o_orderdate >= date(selected_year + '-01-01')
                    AND o.o_orderdate < date((selected_year + 1) + '-01-01')
                    WITH n.n_name AS nation, 
                        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
                    RETURN nation, revenue
                    ORDER BY revenue DESC"""

query_5_graph = """PROFILE WITH ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST'] as regions, toInteger(floor(5*rand())) AS position
                    WITH regions[position] AS selected_region, 1993 + toInteger(floor(4*rand())) AS selected_year
                    MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)-[:ORDERS_CUSTOMER]->(c:CUSTOMER)-[:CUSTOMER_NATION]->(n:NATION)-[:NATION_REGION]->(r:REGION),
                        (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)-[SUPPLIER_NATION]->(n:NATION)
                    WHERE r.r_name = selected_region
                    AND o.o_orderdate >= date(selected_year + '-01-01')
                    AND o.o_orderdate < date((selected_year + 1) + '-01-01')
                    WITH n.n_name AS nation, 
                        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
                    RETURN nation, revenue
                    ORDER BY revenue DESC"""



# Query 6

query_6_relational = """PROFILE WITH 1993 + toInteger(floor(4*rand())) AS selected_year, toInteger(floor(2 + 7*rand()))/100 AS discount, toInteger(floor(24 + 2*rand())) AS quantity
                        MATCH (l:LINEITEM)
                        WHERE l.l_shipdate >= date(selected_year + '-01-01')
                        AND l.l_shipdate < date((selected_year + 1) + '-01-01')
                        AND l.l_discount >= discount - 0.01
                        AND l.l_discount <= discount + 0.01
                        AND l.l_quantity < quantity
                        WITH SUM(l.l_extendedprice * l.l_discount) AS revenue
                        RETURN revenue;"""

query_6_mixed = query_6_relational

query_6_graph = query_6_relational



# Query 7

query_7_relational = """PROFILE WITH ['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'] AS nations,
                        toInteger(floor(25*rand())) AS position
                        WITH nations[position] AS nation1, nations[(position + 1 + toInteger(floor(23*rand()))) % 25] AS nation2
                        MATCH (s:SUPPLIER), (l:LINEITEM), (o:ORDERS), (c:CUSTOMER), (n1:NATION), (n2:NATION)
                        WHERE s.s_suppkey = l.l_suppkey
                        AND o.o_orderkey = l.l_orderkey
                        AND c.c_custkey = o.o_custkey
                        AND s.s_nationkey = n1.n_nationkey
                        AND c.c_nationkey = n2.n_nationkey
                        AND (
                            (n1.n_name = nation1 AND n2.n_name = nation2) OR
                            (n1.n_name = nation2 AND n2.n_name = nation1)
                        )
                        AND l.l_shipdate >= date('1995-01-01')
                        AND l.l_shipdate <= date('1996-12-31')
                        WITH n1.n_name AS supp_nation, 
                            n2.n_name AS cust_nation, 
                            toString(l.l_shipdate.year) AS l_year,
                            SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
                        RETURN supp_nation, cust_nation, l_year, revenue
                        ORDER BY supp_nation, cust_nation, l_year"""

query_7_mixed = """PROFILE WITH ['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'] AS nations,
                        toInteger(floor(25*rand())) AS position
                        WITH nations[position] AS nation1, nations[(position + 1 + toInteger(floor(23*rand()))) % 25] AS nation2
                        MATCH (n1:NATION)<-[:SUPPLIER_NATION]-(s:SUPPLIER), (ps:PARTSUPP)<-[:LINEITEM_PARTSUPP]-(l:LINEITEM), (o:ORDERS)-[:ORDERS_CUSTOMER]->(c:CUSTOMER)-[:CUSTOMER_NATION]->(n2:NATION)
                        WHERE s.s_suppkey = ps.ps_suppkey
                        AND o.o_orderkey = l.l_orderkey
                        AND (
                            (n1.n_name = nation1 AND n2.n_name = nation2) OR
                            (n1.n_name = nation2 AND n2.n_name = nation1)
                        )
                        AND l.l_shipdate >= date('1995-01-01')
                        AND l.l_shipdate <= date('1996-12-31')
                        WITH n1.n_name AS supp_nation, 
                            n2.n_name AS cust_nation, 
                            toString(l.l_shipdate.year) AS l_year,
                            SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
                        RETURN supp_nation, cust_nation, l_year, revenue
                        ORDER BY supp_nation, cust_nation, l_year"""

query_7_graph = """PROFILE WITH ['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'] AS nations,
                toInteger(floor(25*rand())) AS position
                WITH nations[position] AS nation1, nations[(position + 1 + toInteger(floor(23*rand()))) % 25] AS nation2
                MATCH (n1:NATION)<-[:SUPPLIER_NATION]-(s:SUPPLIER)<-[:PARTSUPP_SUPPLIER]-(ps:PARTSUPP)<-[:LINEITEM_PARTSUPP]-(l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)-[:ORDERS_CUSTOMER]->(c:CUSTOMER)-[:CUSTOMER_NATION]->(n2:NATION)
                WHERE ((n1.n_name = nation1 AND n2.n_name = nation2) OR
                    (n1.n_name = nation2 AND n2.n_name = nation1))
                AND l.l_shipdate >= date('1995-01-01')
                AND l.l_shipdate <= date('1996-12-31')
                WITH n1.n_name AS supp_nation, 
                    n2.n_name AS cust_nation, 
                    toString(l.l_shipdate.year) AS l_year,
                    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
                RETURN supp_nation, cust_nation, l_year, revenue
                ORDER BY supp_nation, cust_nation, l_year"""



# Query 8

query_8_relational = """PROFILE WITH ['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'] AS nations,
                    ['AFRICA', 'AMERICA', 'AMERICA', 'AMERICA', 'AFRICA', 'AFRICA', 'EUROPE', 'EUROPE', 'ASIA', 'ASIA', 'ASIA', 'ASIA', 'ASIA', 'ASIA', 'AFRICA', 'AFRICA', 'AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'ASIA', 'ASIA', 'EUROPE', 'EUROPE', 'AMERICA'] AS regions,
                    toInteger(floor(25*rand())) AS position, ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO'] AS types1, ['ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED'] AS types2, ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER'] AS types3, toInteger(floor(6*rand())) AS pos1, toInteger(floor(5*rand())) AS pos2, toInteger(floor(5*rand())) AS pos3 
                    WITH types1[pos1] + ' ' + types2[pos2] + ' ' + types3[pos3]  AS type, nations[position] AS nation, regions[position] as region
                    MATCH (p:PART), (s:SUPPLIER), (l:LINEITEM), (o:ORDERS), (c:CUSTOMER), (r:REGION), (n1:NATION), (n2:NATION)
                    WHERE p.p_partkey = l.l_partkey
                    AND s.s_suppkey = l.l_suppkey
                    AND l.l_orderkey = o.o_orderkey
                    AND o.o_custkey = c.c_custkey
                    AND l.l_suppkey = s.s_suppkey
                    AND c.c_nationkey = n1.n_nationkey
                    AND n1.n_regionkey = r.r_regionkey
                    AND r.r_name = region
                    AND s.s_nationkey = n2.n_nationkey
                    AND o.o_orderdate >= date('1995-01-01')
                    AND o.o_orderdate < date('1997-01-01')
                    AND p.p_type = type
                    WITH l.l_shipdate.year AS o_year, 
                        l.l_extendedprice * (1 - l.l_discount) AS volume,
                        n2.n_name AS nation
                    WITH o_year, SUM(CASE WHEN nation = nation THEN volume ELSE 0 end)/SUM(volume) AS mkt_share
                    RETURN o_year, mkt_share
                    ORDER BY o_year"""

query_8_mixed = """PROFILE WITH ['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'] AS nations,
                ['AFRICA', 'AMERICA', 'AMERICA', 'AMERICA', 'AFRICA', 'AFRICA', 'EUROPE', 'EUROPE', 'ASIA', 'ASIA', 'ASIA', 'ASIA', 'ASIA', 'ASIA', 'AFRICA', 'AFRICA', 'AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'ASIA', 'ASIA', 'EUROPE', 'EUROPE', 'AMERICA'] AS regions,
                toInteger(floor(25*rand())) AS position, ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO'] AS types1, ['ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED'] AS types2, ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER'] AS types3, toInteger(floor(6*rand())) AS pos1, toInteger(floor(5*rand())) AS pos2, toInteger(floor(5*rand())) AS pos3 
                WITH types1[pos1] + ' ' + types2[pos2] + ' ' + types3[pos3]  AS type, nations[position] AS nation, regions[position] as region
                MATCH (p:PART), (s:SUPPLIER), (l:LINEITEM), (o:ORDERS), (c:CUSTOMER), (r:REGION), (n1:NATION), (n2:NATION)
                MATCH (r:REGION)<-[:NATION_REGION]-(n2:NATION)<-[:SUPPLIER_NATION]-(s:SUPPLIER), (p:PART), (l:LINEITEM), (o:ORDERS)-[:ORDERS_CUSTOMER]->(c:CUSTOMER)-[:CUSTOMER_NATION]->(n1:NATION)
                WHERE p.p_partkey = l.l_partkey
                AND s.s_suppkey = l.l_suppkey
                AND l.l_orderkey = o.o_orderkey
                AND r.r_name = region
                AND o.o_orderdate >= date('1995-01-01')
                AND o.o_orderdate < date('1997-01-01')
                AND p.p_type = type
                WITH l.l_shipdate.year AS o_year, 
                    l.l_extendedprice * (1 - l.l_discount) AS volume,
                    n2.n_name AS nation
                WITH o_year, SUM(CASE WHEN nation = nation THEN volume ELSE 0 end)/SUM(volume) AS mkt_share
                RETURN o_year, mkt_share
                ORDER BY o_year"""

query_8_graph = """PROFILE WITH ['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT', 'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA', 'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA', 'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA', 'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UNITED KINGDOM', 'UNITED STATES'] AS nations,
                ['AFRICA', 'AMERICA', 'AMERICA', 'AMERICA', 'AFRICA', 'AFRICA', 'EUROPE', 'EUROPE', 'ASIA', 'ASIA', 'ASIA', 'ASIA', 'ASIA', 'ASIA', 'AFRICA', 'AFRICA', 'AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'ASIA', 'ASIA', 'EUROPE', 'EUROPE', 'AMERICA'] AS regions,
                toInteger(floor(25*rand())) AS position, ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO'] AS types1, ['ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED'] AS types2, ['TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER'] AS types3, toInteger(floor(6*rand())) AS pos1, toInteger(floor(5*rand())) AS pos2, toInteger(floor(5*rand())) AS pos3 
                WITH types1[pos1] + ' ' + types2[pos2] + ' ' + types3[pos3]  AS type, nations[position] AS nation, regions[position] as region
                MATCH (p:PART), (s:SUPPLIER), (l:LINEITEM), (o:ORDERS), (c:CUSTOMER), (r:REGION), (n1:NATION), (n2:NATION)
                MATCH (r:REGION)<-[:NATION_REGION]-(n2:NATION)<-[:SUPPLIER_NATION]-(s:SUPPLIER)-[:PARTSUPP_SUPPLIER]-(ps:PARTSUPP)-[PARTSUPP_PART]->(p:PART), (ps:PARTSUPP)<-[:LINEITEM_PARTSUPP]-(l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)-[:ORDERS_CUSTOMER]->(c:CUSTOMER)-[:CUSTOMER_NATION]->(n1:NATION)
                WHERE r.r_name = region
                AND o.o_orderdate >= date('1995-01-01')
                AND o.o_orderdate < date('1997-01-01')
                AND p.p_type = type
                WITH l.l_shipdate.year AS o_year, 
                    l.l_extendedprice * (1 - l.l_discount) AS volume,
                    n2.n_name AS nation
                WITH o_year, SUM(CASE WHEN nation = nation THEN volume ELSE 0 end)/SUM(volume) AS mkt_share
                RETURN o_year, mkt_share
                ORDER BY o_year"""



# Query 9

query_9_relational = """PROFILE WITH ['almond', 'antique', 'aquamarine', 'azure', 'beige', 'bisque', 'black', 'blanched', 'blue', 'blush', 'brown', 'burlywood', 'burnished', 'chartreuse', 'chiffon', 'chocolate', 'coral', 'cornflower', 'cornsilk', 'cream', 'cyan', 'dark', 'deep', 'dim', 'dodger', 'drab', 'firebrick', 'floral', 'forest', 'frosted', 'gainsboro', 'ghost', 'goldenrod', 'green', 'grey', 'honeydew', 'hot', 'indian', 'ivory', 'khaki', 'lace', 'lavender', 'lawn', 'lemon', 'light', 'lime', 'linen', 'magenta', 'maroon', 'medium', 'metallic', 'midnight', 'mint', 'misty', 'moccasin', 'navajo', 'navy', 'olive', 'orange', 'orchid', 'pale', 'papaya', 'peach', 'peru', 'pink', 'plum', 'powder', 'puff', 'purple', 'red', 'rose', 'rosy', 'royal', 'saddle', 'salmon', 'sandy', 'seashell', 'sienna', 'sky', 'slate', 'smoke', 'snow', 'spring', 'steel', 'tan', 'thistle', 'tomato', 'turquoise', 'violet', 'wheat', 'white', 'yellow'] AS colors,
                        toInteger(floor(80*rand())) AS position
                        WITH colors[position] AS color
                        MATCH (p:PART), (s:SUPPLIER), (l:LINEITEM), (ps:PARTSUPP), (o:ORDERS), (n:NATION)
                        WHERE s.s_suppkey = l.l_suppkey
                        AND ps.ps_suppkey = l.l_suppkey
                        AND ps.ps_partkey = l.l_partkey
                        AND p.p_partkey = l.l_partkey
                        AND o.o_orderkey = l.l_orderkey
                        AND s.s_nationkey = n.n_nationkey
                        AND p.p_name CONTAINS color
                        WITH n.n_name AS nation, 
                            o.o_orderdate.year AS o_year, 
                            SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit
                        RETURN nation, o_year, sum_profit
                        ORDER BY nation, o_year DESC"""

query_9_mixed = """PROFILE WITH ['almond', 'antique', 'aquamarine', 'azure', 'beige', 'bisque', 'black', 'blanched', 'blue', 'blush', 'brown', 'burlywood', 'burnished', 'chartreuse', 'chiffon', 'chocolate', 'coral', 'cornflower', 'cornsilk', 'cream', 'cyan', 'dark', 'deep', 'dim', 'dodger', 'drab', 'firebrick', 'floral', 'forest', 'frosted', 'gainsboro', 'ghost', 'goldenrod', 'green', 'grey', 'honeydew', 'hot', 'indian', 'ivory', 'khaki', 'lace', 'lavender', 'lawn', 'lemon', 'light', 'lime', 'linen', 'magenta', 'maroon', 'medium', 'metallic', 'midnight', 'mint', 'misty', 'moccasin', 'navajo', 'navy', 'olive', 'orange', 'orchid', 'pale', 'papaya', 'peach', 'peru', 'pink', 'plum', 'powder', 'puff', 'purple', 'red', 'rose', 'rosy', 'royal', 'saddle', 'salmon', 'sandy', 'seashell', 'sienna', 'sky', 'slate', 'smoke', 'snow', 'spring', 'steel', 'tan', 'thistle', 'tomato', 'turquoise', 'violet', 'wheat', 'white', 'yellow'] AS colors,
                    toInteger(floor(80*rand())) AS position
                    WITH colors[position] AS color
                    MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP), (s:SUPPLIER)-[:SUPPLIER_NATION]->(n:NATION),
                        (p:PART), (o:ORDERS)
                    WHERE ps.ps_suppkey = s.s_suppkey
                    AND ps.ps_partkey = p.p_partkey
                    AND l.l_orderkey = o.o_orderkey
                    AND p.p_name CONTAINS color
                    WITH n.n_name AS nation, 
                        toString(o.o_orderdate.year) AS o_year, 
                        SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit
                    RETURN nation, o_year, sum_profit
                    ORDER BY nation, o_year DESC"""

query_9_graph = """PROFILE WITH ['almond', 'antique', 'aquamarine', 'azure', 'beige', 'bisque', 'black', 'blanched', 'blue', 'blush', 'brown', 'burlywood', 'burnished', 'chartreuse', 'chiffon', 'chocolate', 'coral', 'cornflower', 'cornsilk', 'cream', 'cyan', 'dark', 'deep', 'dim', 'dodger', 'drab', 'firebrick', 'floral', 'forest', 'frosted', 'gainsboro', 'ghost', 'goldenrod', 'green', 'grey', 'honeydew', 'hot', 'indian', 'ivory', 'khaki', 'lace', 'lavender', 'lawn', 'lemon', 'light', 'lime', 'linen', 'magenta', 'maroon', 'medium', 'metallic', 'midnight', 'mint', 'misty', 'moccasin', 'navajo', 'navy', 'olive', 'orange', 'orchid', 'pale', 'papaya', 'peach', 'peru', 'pink', 'plum', 'powder', 'puff', 'purple', 'red', 'rose', 'rosy', 'royal', 'saddle', 'salmon', 'sandy', 'seashell', 'sienna', 'sky', 'slate', 'smoke', 'snow', 'spring', 'steel', 'tan', 'thistle', 'tomato', 'turquoise', 'violet', 'wheat', 'white', 'yellow'] AS colors,
                toInteger(floor(80*rand())) AS position
                WITH colors[position] AS color
                MATCH (l:LINEITEM)-[:LINEITEM_PARTSUPP]->(ps:PARTSUPP)-[:PARTSUPP_SUPPLIER]->(s:SUPPLIER)-[:SUPPLIER_NATION]->(n:NATION),
                    (ps:PARTSUPP)-[:PARTSUPP_PART]->(p:PART), (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)
                WHERE p.p_name CONTAINS color
                WITH n.n_name AS nation, 
                    toString(o.o_orderdate.year) AS o_year, 
                    SUM(l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity) AS sum_profit
                RETURN nation, o_year, sum_profit
                ORDER BY nation, o_year DESC"""



# Query 10

query_10_relational = """PROFILE WITH date('1993-02-01') + Duration({months: toInteger(floor(23*rand()))}) AS start_date
                        WITH start_date, start_date + Duration({months: 3}) AS end_date
                        MATCH (c:CUSTOMER), (o:ORDERS), (l:LINEITEM), (n:NATION)
                        WHERE c.c_custkey = o.o_custkey
                        AND l.l_orderkey = o.o_orderkey
                        AND o.o_orderdate >= start_date
                        AND o.o_orderdate < end_date
                        AND l.l_returnflag = 'R'
                        AND c.c_nationkey = n.n_nationkey
                        WITH c.c_custkey AS custkey,
                            c.c_name AS name,
                            SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
                            c.c_acctbal AS acctbal,
                            n.n_name AS nation,
                            c.c_address AS address,
                            c.c_phone AS phone,
                            c.c_comment AS comment
                        RETURN custkey, name, revenue, acctbal, nation, address, phone, comment
                        ORDER BY revenue DESC"""

query_10_mixed = """PROFILE WITH date('1993-02-01') + Duration({months: toInteger(floor(23*rand()))}) AS start_date
                    WITH start_date, start_date + Duration({months: 3}) AS end_date
                    MATCH (l:LINEITEM), (o:ORDERS)-[:ORDERS_CUSTOMER]->(c:CUSTOMER)-[:CUSTOMER_NATION]->(n:NATION)
                    WHERE l.l_orderkey = o.o_orderkey
                    AND o.o_orderdate >= start_date
                    AND o.o_orderdate < end_date
                    AND l.l_returnflag = 'R'
                    WITH c.c_custkey AS custkey,
                        c.c_name AS name,
                        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
                        c.c_acctbal AS acctbal,
                        n.n_name AS nation,
                        c.c_address AS address,
                        c.c_phone AS phone,
                        c.c_comment AS comment
                    RETURN custkey, name, revenue, acctbal, nation, address, phone, comment
                    ORDER BY revenue DESC"""

query_10_graph = """PROFILE WITH date('1993-02-01') + Duration({months: toInteger(floor(23*rand()))}) AS start_date
                    WITH start_date, start_date + Duration({months: 3}) AS end_date
                    MATCH (l:LINEITEM)-[:LINEITEM_ORDERS]->(o:ORDERS)-[:ORDERS_CUSTOMER]->(c:CUSTOMER)-[:CUSTOMER_NATION]->(n:NATION)
                    WHERE o.o_orderdate >= start_date
                    AND o.o_orderdate < end_date
                    AND l.l_returnflag = 'R'
                    WITH c.c_custkey AS custkey,
                        c.c_name AS name,
                        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
                        c.c_acctbal AS acctbal,
                        n.n_name AS nation,
                        c.c_address AS address,
                        c.c_phone AS phone,
                        c.c_comment AS comment
                    RETURN custkey, name, revenue, acctbal, nation, address, phone, comment
                    ORDER BY revenue DESC"""
    
   
# main function

def main():

    

    # Establish connection to Neo4j instance
    local_bolt = <local_bolt>
    local_pw = <password>
    local_user = <local_user>
    active_database = is_relational_model * "relational" + is_semirelational_model * "mixed" + is_graph_model * "graph"


    ###############################
    #                             #
    #     Experiment settings     #
    #                             #
    ###############################   
    
    
    factor = 1 # sclaing factor for TPC-H (small = 0.01 / mdedium = 0.1 / large = 1)
    is_relational_model = True
    is_semirelational_model = False
    is_graph_model = False

    query_number = 1 # required for writing to excel

    query = query_1_relational  # query to be executed / has to align with the semantics chosen above


    precision = 1 # precision for time measured in miliseconds for query execution

    
    # amount of runs for experiments with one dedicated percentage value of updates
    runs = 20

    # amount of bottom and top results based on amount of runs that will be disregarded for average caluclation
    outliers = 5




    

    # Initialise DB
    new_db = gdbms_test(local_bolt, local_user, local_pw, active_database)

    # Initialise db_hits and time update propagation
    db_hits = []
    times = []
    
    # current date and time
    today = datetime.now().strftime("%Y_%m_%d")
    current = datetime.now().strftime("%H_%M_%S")





    for i in range(0, runs):
        
            
        # shows if experiment is still running
        print()
        print(f"{runs-i} more experiments to go...")
        print()

        current_db_hits = 0
        current_time = 0

        ## execute query
        
        current_db_hits_time = show_query_details(new_db, query)
        current_db_hits = current_db_hits_time[0]
        current_time = current_db_hits_time[1]


        db_hits.append(current_db_hits)
        times.append(current_time)

        



    if outliers > 0:
        db_hits = sorted(db_hits)[outliers:-outliers]
        times = [round(time_of_run / 1000000,precision) for time_of_run in sorted(times)[outliers:-outliers]]
    else:
        db_hits = sorted(db_hits)
        times = [round(time_of_run / 1000000,precision) for time_of_run in sorted(times)]


    average_db_hits = round(sum(db_hits)/(runs - 2*outliers))
    average_time = round(sum(times)/(runs - 2*outliers),precision)

    # Show results
    

    # Experiment results

    filename = "Benchmark_query_results_" + "query_" + str(query_number) + "___" + str(factor) + "_" + (is_relational_model * "relational" + is_semirelational_model * "mixed" + is_graph_model * "graph") + "_" + str(today) + "---" + str(current) + ".xlsx"
    sheetname = "Experiment"
    experiment_name = f"TPC-H benchmark queries with scaling factor {factor}"

    # can probably be deleted
    experiment_details = []
    
    # create content for Excel
    content = [["Benchmark query number " + str(query_number) + " : " + query ],[is_relational_model * "Relational semantics modelling" + is_semirelational_model * "Mixed semantics modelling" + is_graph_model * "Graph semantics modelling"], [],[]]
    content.append(["DbHits: "] + db_hits + [" ", "Average DbHits: "] + [average_db_hits])
    content.append(["Time (in ms): "] + times + [" ", "Average time (in ms): "] + [average_time])
    content.append([])

    # writing to file
    write_to_excel(filename, sheetname, experiment_name, experiment_details, content)


    # closing db
    new_db.close()


main()






