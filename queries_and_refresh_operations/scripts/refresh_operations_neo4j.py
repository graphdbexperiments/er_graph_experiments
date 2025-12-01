'''
Graph Database experiments on TPC-H benchmark dataset

Refresh Queries
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

    #print(summary)

    return  (sum_db_hits(summary), sum_time(summary))


########################

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

    
    # amount of runs for experiments with one dedicated percentage value of updates
    runs = 20

    # amount of bottom and top results based on amount of runs that will be disregarded for average caluclation
    outliers = 5

    percentage = 100 # adjustable to create / delete more than regular amound in RF1 / RF2
    





    
    # Initialise DB
    new_db = gdbms_test(local_bolt, local_user, local_pw, active_database)

    # Initialise db_hits and time for RF1 and RF2
    db_hits = [[],[]]
    times = [[], []]

    # current date and time
    today = datetime.now().strftime("%Y_%m_%d")
    current = datetime.now().strftime("%H_%M_%S")



    for i in range(0, runs):
        
            
        # shows if experiment is still running
        print()
        print(f"{runs-i} more experiments to go...")
        print()

        ## execute query


        # get SF*1,500 random ORDERS and respective CUSTOMER

        limit = int(percentage / 100 * factor * 1500)
        customer_pool = int(factor * 150000)
        partsupp_pool = int(factor * 800000)

        get_max_order_key = "PROFILE MATCH (o:ORDERS) RETURN o.o_orderkey AS keys ORDER BY keys DESC LIMIT 1"
        get_max_order_key_result = new_db.execute_query_with_output_result(get_max_order_key)
        max_order_key = max([max_order_key_dict['keys'] for max_order_key_dict in get_max_order_key_result])

        
        get_cust_keys = "PROFILE MATCH(c:CUSTOMER) RETURN c.c_custkey AS customer_key"
        customer_keys_query_result = new_db.execute_query_with_output_result(get_cust_keys)
        cust_keys = [cust_key_dict['customer_key'] for cust_key_dict in customer_keys_query_result]

        # create new order keys that do not exist yet

        new_order_keys = [(max_order_key + 2 + increment) for increment in range(limit)]

        # get random PARTSUPP (ps_partkey, ps_suppkey) for new LINEITEMS

        if is_relational_model or is_semirelational_model:
            get_partsupp_keys = "PROFILE MATCH (ps:PARTSUPP) RETURN ps.ps_partkey AS partkey, ps.ps_suppkey AS suppkey"
        else:
            get_partsupp_keys = "PROFILE MATCH (p:PART)-[]-(ps:PARTSUPP)-[]-(s:SUPPLIER) RETURN p.p_partkey AS partkey, s.s_suppkey AS suppkey"

        partsupp_keys_query_result = new_db.execute_query_with_output_result(get_partsupp_keys)
        partsupp_keys = [(partsupp_key_dict['partkey'], partsupp_key_dict['suppkey']) for partsupp_key_dict in partsupp_keys_query_result]
        

        print("Starting...")


        # RF 1 (insert ORDERS and LINEITEM)
        
        current_db_hits = 0
        current_time = 0


        for key in new_order_keys:
            
            # choose random customer for order
            customer = str(cust_keys[random.randrange(customer_pool)])
            
            if is_relational_model:
                create_order = "PROFILE MERGE (o:ORDERS{o_orderkey: " + str(key) + ", o_custkey: " + customer + ", o_comment: 'new'})"
            else:
                create_order = "PROFILE MATCH (c:CUSTOMER{c_custkey: " + customer + "}) MERGE (o:ORDERS{o_orderkey: " + str(key) + ", o_comment: 'new'})-[r:ORDERS_CUSTOMER]->(c)"
            current_db_hits_time = show_query_details(new_db, create_order)
            current_db_hits += current_db_hits_time[0]
            current_time += current_db_hits_time[1]

                   
            for line_number in range(1, random.randrange(2,8)):
                
                partsupp = partsupp_keys[random.randrange(partsupp_pool)]
                
                if is_relational_model:
                    create_lineitem = "PROFILE MERGE (l:LINEITEM{l_orderkey: " + str(key) + ", l_linenumber: " + str(line_number) + ", l_partkey: " + str(partsupp[0]) + ", l_suppkey: " + str(partsupp[1]) + ", l_comment: 'new'})"
                elif is_semirelational_model:
                    create_lineitem = create_lineitem = "PROFILE MATCH (ps:PARTSUPP{ps_partkey: " + str(partsupp[0]) + ", ps_suppkey: " + str(partsupp[1]) +"}) WITH ps AS partsupp MERGE (l:LINEITEM{l_orderkey: " + str(key) + ", l_linenumber: " + str(line_number) + ", l_comment: 'new'})-[r3:LINEITEM_PARTSUPP]->(partsupp)"
                else:
                    create_lineitem = "PROFILE MATCH (p:PART{p_partkey: " + str(partsupp[0]) + "}), (s:SUPPLIER{s_suppkey: " + str(partsupp[1]) + "}), (p)<-[]-(ps:PARTSUPP)-[]->(s), (o:ORDERS{o_orderkey: " + str(key) + "}) WITH ps AS partsupp, o AS orders MERGE (orders)<-[r2:LINEITEM_ORDERS]-(l:LINEITEM{l_linenumber: " + str(line_number) + ", l_comment: 'new'})-[r3:LINEITEM_PARTSUPP]->(partsupp)"
                current_db_hits_time = show_query_details(new_db, create_lineitem)
                current_db_hits += current_db_hits_time[0]
                current_time += current_db_hits_time[1]



        db_hits[0].append(current_db_hits)
        times[0].append(current_time)


        
        print("Created")
        # time.sleep(60)
        
        



        # RF 2 (delete ORDERS and LINEITEM)


        get_order_keys = "PROFILE MATCH(o:ORDERS) RETURN o.o_orderkey AS order_key ORDER BY rand() LIMIT " + str(limit)
        order_keys_query_result = new_db.execute_query_with_output_result(get_order_keys)
        order_keys = [order_key_dict['order_key'] for order_key_dict in order_keys_query_result]

        current_db_hits = 0
        current_time = 0

        for key in order_keys:
            if is_relational_model or is_semirelational_model:
                delete_order_lineitems = "PROFILE MATCH (o:ORDERS{o_orderkey: " + str(key) + "}), (l:LINEITEM{l_orderkey: " + str(key) + "})  DETACH DELETE o,l"
            else:
                delete_order_lineitems = "PROFILE MATCH (o:ORDERS{o_orderkey: " + str(key) + "})<-[:LINEITEM_ORDERS]-(l:LINEITEM) DETACH DELETE o,l"
                pass
            current_db_hits_time = show_query_details(new_db, delete_order_lineitems)
            current_db_hits += current_db_hits_time[0]
            current_time += current_db_hits_time[1]

        db_hits[1].append(current_db_hits)
        times[1].append(current_time)

    
        
        print("Deleted")
        print()
        # time.sleep(20)
        
        

    print(db_hits)
    print(times)
    print()

    db_hits[0] = sorted(db_hits[0])[outliers:-outliers]
    db_hits[1] = sorted(db_hits[1])[outliers:-outliers]
    times[0] = [round(time_of_run / 1000000) for time_of_run in sorted(times[0])[outliers:-outliers]]
    times[1] = [round(time_of_run / 1000000) for time_of_run in sorted(times[1])[outliers:-outliers]]

    print(db_hits)
    print(times)
    print()

    average_db_hits = [[],[]]
    average_time = [[],[]]

    average_db_hits[0].append(round(sum(db_hits[0])/(runs - 2*outliers)))
    average_db_hits[1].append(round(sum(db_hits[1])/(runs - 2*outliers)))
    average_time[0].append(round(sum(times[0])/(runs - 2*outliers)))
    average_time[1].append(round(sum(times[1])/(runs - 2*outliers)))

    print(average_db_hits)
    print(average_time)

    # Experiment results

    filename = "Refresh_query_results_" + str(factor) + "_" + str(today) + "---" + str(current) + ".xlsx"
    sheetname = "Experiment"
    experiment_name = f"TPC-H RF1 and RF2 with scaling factor {factor}"

    # can probably be deleted
    experiment_details = []
    
    # create content
    rf_1_content = [["Refresh query RF1:"], ["DbHits: "] + db_hits[0] + [" ", "Average DbHits: "] + average_db_hits[0], ["Time (in ms): "] + times[0] + [" ", "Average time (in ms): "] + average_time[0]]
    rf_2_content = [["Refresh query RF2:"], ["DbHits: "] + db_hits[1] + [" ", "Average DbHits: "] + average_db_hits[1], ["Time (in ms): "] + times[1] + [" ", "Average time (in ms): "] + average_time[1]]
    content = [[],[],[is_relational_model * "Relational semantics modelling" + is_semirelational_model * "Mixed semantics modelling" + is_graph_model * "Graph semantics modelling"], [],[f"{percentage}% of original RF amount"],[],[]] + rf_1_content + [[''],['']] + rf_2_content

    # writing to file
    write_to_excel(filename, sheetname, experiment_name, experiment_details, content)


    # closing db
    new_db.close()


main()





