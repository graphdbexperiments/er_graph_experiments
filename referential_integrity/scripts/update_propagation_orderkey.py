
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
    local_user = "neo4j"


    ###############################
    #                             #
    #     Experiment settings     #
    #                             #
    ###############################   

    factor = 1 # sclaing factor for TPC-H (small = 0.01 / mdedium = 0.1 / large = 1)
    percentage_to_update = [0.2, 0.4, 0.6, 0.8, 1] # percent of dedicated records to be updated
    
    # exactly one of the three boolean variables needs to be set to True to determine which semantics is used
    is_relational_model = False 
    is_semirelational_model = False
    is_graph_model = True

    # amount of runs for experiments with one dedicated percentage value of updates
    runs = 20

    # amount of bottom and top results based on amount of runs that will be disregarded for average caluclation
    outliers = 5



    
    chain = "ORDERS -> LINEITEM"

    # Initialise DB
    new_db = gdbms_test(local_bolt, local_user, local_pw)

    # Initialise db_hits and time update propagation
    db_hits = [[], [], [], [], []]
    times = [[], [], [], [], []]

    average_db_hits = [[], [], [], [], []]
    average_time = [[], [], [], [], []]


    # current date and time
    today = datetime.now().strftime("%Y_%m_%d")
    current = datetime.now().strftime("%H_%M_%S")


        
    # perform experiment multiple times

    for index in range(len(percentage_to_update)):

        runs = 20

        outliers = 5

        for i in range(0, runs):
            
                
            # shows if experiment is still running
            print()
            print(f"Percentage: {percentage_to_update[index]} out of {percentage_to_update}")
            print(f"{runs-i} more experiments to go...")
            print()

            current_db_hits = 0
            current_time = 0

            ## execute query



            ########## Update propagation ORDERS -> LINEITEM

            # get order nodes for update propagation


            get_orders_amount = "PROFILE MATCH(o:ORDERS) RETURN COUNT(o) AS amount"
            orders_amount_query_result = new_db.execute_query_with_output_result(get_orders_amount)
            amount = orders_amount_query_result[0]['amount']


            if percentage_to_update[index] != 'single':
                offset = random.randint(0, amount - round(amount * percentage_to_update[index]))
                activation = f"MATCH (o:ORDERS) WITH o AS ords SKIP {offset} LIMIT {round(amount * percentage_to_update[index])} SET ords:ORDERS:ACTIVATED"
                new_db.execute_query(activation)
            else:
                offset = random.randint(0, amount-1)
                activation = f"MATCH (o:ORDERS) WITH o AS ords SKIP {offset} LIMIT 1 SET ords:ORDERS:ACTIVATED"
                new_db.execute_query(activation)

            #print("labelling done")

            #time.sleep(5) #wait for nodes to be labelled


            


            # Enforce key constraint for ORDERS nodes on ACTIVATED nodes in case query planner starts with a node filter on ACTIVATED (this way index on ORDERS is still being used)

            index_activation = "CREATE CONSTRAINT activated_orders IF NOT EXISTS FOR (a:ACTIVATED) REQUIRE (a.o_orderkey) IS NODE KEY"
            new_db.execute_query(index_activation)

            #print("constraint done")

            #time.sleep(5) #wait for contraint to be created
            

            if is_relational_model:
                update = f"PROFILE MATCH (a:ACTIVATED), (l:LINEITEM) WHERE l.l_orderkey = a.o_orderkey SET a.o_orderkey = right(('00000000' + toString(a.o_orderkey)), 8), l.l_orderkey = right(('00000000' + toString(l.l_orderkey)), 8)"
                current_db_hits_time = show_query_details(new_db, update)
                current_db_hits += current_db_hits_time[0]
                current_time += current_db_hits_time[1]


            if is_semirelational_model:
                update = f"PROFILE MATCH (a:ACTIVATED), (l:LINEITEM) WHERE l.l_orderkey = a.o_orderkey SET a.o_orderkey = right(('00000000' + toString(a.o_orderkey)), 8), l.l_orderkey = right(('00000000' + toString(l.l_orderkey)), 8)"
                current_db_hits_time = show_query_details(new_db, update)
                current_db_hits += current_db_hits_time[0]
                current_time += current_db_hits_time[1]

                    
            if is_graph_model:
                update = f"PROFILE MATCH (a:ACTIVATED) SET a.o_orderkey = right(('00000000' + toString(a.o_orderkey)), 8)"
                current_db_hits_time = show_query_details(new_db, update)
                current_db_hits += current_db_hits_time[0]
                current_time += current_db_hits_time[1]


            db_hits[index].append(current_db_hits)
            times[index].append(current_time)

            

            #print("Updated")
            #time.sleep(10)


            # undo update to restart experiment
            
            update = "MATCH (a:ACTIVATED) SET a.o_orderkey = toInteger(a.o_orderkey)"
            new_db.execute_query(update)
            if is_relational_model or is_semirelational_model:
                update = "MATCH (l:LINEITEM) SET l.l_orderkey = toInteger(l.l_orderkey)"
                new_db.execute_query(update)


            # Deactive fraction for update propagation
            deactivation = "MATCH (a:ACTIVATED) REMOVE a:ACTIVATED"
            new_db.execute_query(deactivation)

            index_deactivation = "DROP CONSTRAINT activated_orders"
            new_db.execute_query(index_deactivation)

            #print("Back to normal")
            #time.sleep(10)

        if outliers > 0:
            db_hits[index] = sorted(db_hits[index])[outliers:-outliers]
            times[index] = [round(time_of_run / 1000000,1) for time_of_run in sorted(times[index])[outliers:-outliers]]
        else:
            db_hits[index] = sorted(db_hits[index])
            times[index] = [round(time_of_run / 1000000,1) for time_of_run in sorted(times[index])]


        average_db_hits[index].append(round(sum(db_hits[index])/(runs - 2*outliers)))
        average_time[index].append(round(sum(times[index])/(runs - 2*outliers),1))

    # Show results
    

    # Experiment results

    filename = "Update_propagation_results_" + str(factor) + "_" + (is_relational_model * "relational" + is_semirelational_model * "mixed" + is_graph_model * "graph") + "_" + str(today) + "---" + str(current) + ".xlsx"
    sheetname = "Experiment"
    experiment_name = f"TPC-H update propagation with scaling factor {factor}"

    # can probably be deleted
    experiment_details = []
    
    # create content for Excel
    content = [["Update propagation: " + chain ],[is_relational_model * "Relational semantics modelling" + is_semirelational_model * "Mixed semantics modelling" + is_graph_model * "Graph semantics modelling"], [],[]]
    for index in range(len(percentage_to_update)):
        try:
            content.append([f"Updating {round(percentage_to_update[index]*100,1)} percent of values:"])
        except:
            content.append([f"Updating single value:"])
        content.append(["DbHits: "] + db_hits[index] + [" ", "Average DbHits: "] + average_db_hits[index])
        content.append(["Time (in ms): "] + times[index] + [" ", "Average time (in ms): "] + average_time[index])
        content.append([])

    # writing to file
    write_to_excel(filename, sheetname, experiment_name, experiment_details, content)


    # closing db
    new_db.close()


main()





