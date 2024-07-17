'''
Graph Database experiments on TPC-H benchmark dataset

Key Verification
'''


import random # used to split up departments randomly
import math # math module

import xlsxwriter # writing to excel

from neo4j import GraphDatabase

from datetime import datetime

import time # use for benchmarking code and finding bottlenecks





# databse class
class gdbms_test:


    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def reset(self):
        with self.driver.session() as session:
            session.run("MATCH (m) DETACH DELETE m")

    def execute_query(self, query):
        with self.driver.session() as session:
            session.run(query)

    def execute_query_with_output(self, query):
        with self.driver.session() as session:
            record = session.run(query)
        return record


    def execute_query_with_output_result(self, query):
        with self.driver.session() as session:
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

    #local bolt and http port, etc:
    local_bolt = "<local_bolt>"
    local_pw = "<password>"
    local_user = "neo4j"

    # Initialise DB
    new_db = gdbms_test(local_bolt, local_user, local_pw)

    # Initialise db_hits and time update propagation
    db_hits = [[], [], [], [], []]
    times = [[], [], [], [], []]

    average_db_hits = [[], [], [], [], []]
    average_time = [[], [], [], [], []]


    # Experiment setting

    factor = 0.01 # sclaing factor for TPC-H (small = 0.01 / mdedium = 0.1 / large = 1)
    percentage_to_update = [0.2, 0.4, 0.6, 0.8, 1]
    is_relational_model = True
    is_semirelational_model = False
    is_graph_model = False

    key = "LINEITEM"

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



            ########## Key verification: LINEITEM

            # Determine LINEITEM nodes for key verification

            get_lineitem_amount = "PROFILE MATCH(l:LINEITEM) RETURN COUNT(l) AS amount"
            lineitem_amount_query_result = new_db.execute_query_with_output_result(get_lineitem_amount)
            amount = lineitem_amount_query_result[0]['amount']

            

            activation = f"MATCH (l:LINEITEM) WITH l AS lineitems, rand() AS r ORDER BY r LIMIT {round(amount * percentage_to_update[index])} SET lineitems:LINEITEM:ACTIVATED"
            new_db.execute_query(activation)

            time.sleep(5) #wait for nodes to be labelled

            print("labelling done")

            # Enforce key constraint for LINEITEM nodes on ACTIVATED nodes in case query planner starts with a node filter on ACTIVATED (this way index on LINEITEM is still being used)

            if is_relational_model:
                index_activation = "CREATE CONSTRAINT activated_lineitem FOR (a:ACTIVATED) REQUIRE (a.l_orderkey, a.l_linenumber) IS NODE KEY"
                new_db.execute_query(index_activation)

                time.sleep(5) #wait for contraint to be created

                print("constraint done")


            if is_relational_model:
                verification = f"PROFILE MATCH (l1:ACTIVATED), (l2:ACTIVATED) WHERE id(l1) < id(l2) AND l1.l_orderkey = l2.l_orderkey AND l1.l_linenumber = l2.l_linenumber RETURN l1,l2"
                current_db_hits_time = show_query_details(new_db, verification)
                current_db_hits += current_db_hits_time[0]
                current_time += current_db_hits_time[1]
                    
            if is_semirelational_model:
                pass

                    
            if is_graph_model:
                verification = f"PROFILE MATCH pattern = (l1:ACTIVATED)-[]->(o:ORDERS)<-[]-(l2:ACTIVATED) WHERE id(l1) < id(l2) AND l1.l_linenumber = l2.l_linenumber RETURN pattern"
                current_db_hits_time = show_query_details(new_db, verification)
                current_db_hits += current_db_hits_time[0]
                current_time += current_db_hits_time[1]
                    


            db_hits[index].append(current_db_hits)
            times[index].append(current_time)

            
            # Deactive fraction to verify key
            deactivation = "MATCH (l:LINEITEM:ACTIVATED) REMOVE l:ACTIVATED"
            new_db.execute_query(deactivation)

            if is_relational_model:
                index_deactivation = "DROP CONSTRAINT activated_lineitem"
                new_db.execute_query(index_deactivation)

            


        if outliers > 0:
            db_hits[index] = sorted(db_hits[index])[outliers:-outliers]
            times[index] = [round(time_of_run / 1000000) for time_of_run in sorted(times[index])[outliers:-outliers]]
        else:
            db_hits[index] = sorted(db_hits[index])
            times[index] = [round(time_of_run / 1000000) for time_of_run in sorted(times[index])]


        average_db_hits[index].append(round(sum(db_hits[index])/(runs - 2*outliers)))
        average_time[index].append(round(sum(times[index])/(runs - 2*outliers)))

     
        

    
    # Experiment results

    filename = "Key_verification_results_" + str(factor) + "_" + str(today) + "---" + str(current) + ".xlsx"
    sheetname = "Experiment"
    experiment_name = f"TPC-H key verification with scaling factor {factor}"

    # can probably be deleted
    experiment_details = []
    
    # create content for Excel
    content = [["Key verification: " + key ],[is_relational_model * "Relational semantics modelling" + is_semirelational_model * "Mixed semantics modelling" + is_graph_model * "Graph semantics modelling"], [],[]]
    for index in range(len(percentage_to_update)):
        content.append([f"Verifying {round(percentage_to_update[index]*100)} percent of values:"])
        content.append(["DbHits: "] + db_hits[index] + [" ", "Average DbHits: "] + average_db_hits[index])
        content.append(["Time (in ms): "] + times[index] + [" ", "Average time (in ms): "] + average_time[index])
        content.append([])

    # writing to file
    write_to_excel(filename, sheetname, experiment_name, experiment_details, content)

    


    # closing db
    new_db.close()


main()




