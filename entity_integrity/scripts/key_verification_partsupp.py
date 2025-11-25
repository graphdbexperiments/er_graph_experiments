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
    local_user = "neo4j"


    ###############################
    #                             #
    #     Experiment settings     #
    #                             #
    ###############################   

    factor = 1 # sclaing factor for TPC-H (small = 0.01 / mdedium = 0.1 / large = 1)
    
    # exactly one of the three boolean variables needs to be set to True to determine which semantics is used
    is_relational_model = False 
    is_semirelational_model = False
    is_graph_model = True
    active_database = is_relational_model * "relational" + is_semirelational_model * "mixed" + is_graph_model * "graph"

    # amount of runs for experiments with one dedicated percentage value of updates
    runs = 20

    # amount of bottom and top results based on amount of runs that will be disregarded for average caluclation
    outliers = 5

    # specify experiment setting: whether or not to create constraint with associated index on key properties of PARTSUPP / LINEITEM nodes (not possible under graph semantics)
    with_index = False

    # specify precision for measuring time to validate E/R key
    precision = 3
    
    

    # Initialise DB
    new_db = gdbms_test(local_bolt, local_user, local_pw, active_database)

    # Initialise db_hits and time update propagation
    db_hits = []
    times = []

    average_db_hits = []
    average_time = []


    key = "PARTSUPP"

    # current date and time
    today = datetime.now().strftime("%Y_%m_%d")
    current = datetime.now().strftime("%H_%M_%S")



    # create index
    if with_index:
        new_db.execute_query("CREATE CONSTRAINT partsupp_index IF NOT EXISTS FOR (ps:PARTSUPP) REQUIRE (ps.ps_partkey, ps.ps_suppkey) IS NODE KEY")


        
    # perform experiment multiple times


    for i in range(0, runs):
        
            
        # shows if experiment is still running
        print()
        print(f"{runs-i} more experiments to go...")
        print()

        current_db_hits = 0
        current_time = 0

        ## execute query



        ########## Key verification: PARTSUPP

        # check if PARTSUPP key violated for new values

        partkey = 1
        suppkey = 1

        
        if is_relational_model or is_semirelational_model:
            verification = f"""PROFILE OPTIONAL MATCH (ps1:PARTSUPP{{ps_partkey: {partkey}, ps_suppkey: {suppkey}}}), (ps2:PARTSUPP{{ps_partkey: {partkey}, ps_suppkey: {suppkey}}}) WHERE id(ps1) <> id(ps2)
                            RETURN ps1, ps2"""
            current_db_hits_time = show_query_details(new_db, verification)
            current_db_hits += current_db_hits_time[0]
            current_time += current_db_hits_time[1]
                


                
        if is_graph_model:
            verification = f"""PROFILE OPTIONAL MATCH (ps1:PARTSUPP)-[]->(s:SUPPLIER{{s_suppkey: {suppkey}}})<-[]-(ps2:PARTSUPP), (ps1:PARTSUPP)-[]->(p:PART{{p_partkey: {partkey}}})<-[]-(ps2:PARTSUPP) WHERE id(ps1) <> id(ps2)
                            RETURN ps1, ps2"""
            current_db_hits_time = show_query_details(new_db, verification)
            current_db_hits += current_db_hits_time[0]
            current_time += current_db_hits_time[1]
                


        db_hits.append(current_db_hits)
        times.append(current_time) 


    if outliers > 0:
        db_hits = sorted(db_hits)[outliers:-outliers]
        times = [round(time_of_run / 1000000, precision) for time_of_run in sorted(times)[outliers:-outliers]]
    else:
        db_hits = sorted(db_hits)
        times = [round(time_of_run / 1000000, precision) for time_of_run in sorted(times)]


    average_db_hits.append(round(sum(db_hits)/(runs - 2*outliers)))
    average_time.append(round(sum(times)/(runs - 2*outliers), precision))


    # delete index
    if with_index:
        new_db.execute_query("DROP CONSTRAINT partsupp_index IF EXISTS")     
        

    
    # Experiment results

    filename = "Key_verification_results_" + str(factor) + "_" + (is_relational_model * "relational" + is_semirelational_model * "mixed" + is_graph_model * "graph") + "_" + str(today) + "---" + str(current) + ".xlsx"
    sheetname = "Experiment"
    experiment_name = f"TPC-H key verification with scaling factor {factor}"

    # can probably be deleted
    experiment_details = []
    
    # create content for Excel
    content = [["With index on partkey, suppkey for PARTSUPP nodes"*with_index + "Without index on partkey, suppkey for PARTSUPP nodes"*(1-with_index)],[],["Key verification: " + key ],[is_relational_model * "Relational semantics modelling" + is_semirelational_model * "Mixed semantics modelling" + is_graph_model * "Graph semantics modelling"], [],[]]
    content.append(["DbHits: "] + db_hits + [" ", "Average DbHits: "] + average_db_hits)
    content.append(["Time (in ms): "] + times + [" ", "Average time (in ms): "] + average_time)

    # writing to file
    write_to_excel(filename, sheetname, experiment_name, experiment_details, content)

    


    # closing db
    new_db.close()


main()





