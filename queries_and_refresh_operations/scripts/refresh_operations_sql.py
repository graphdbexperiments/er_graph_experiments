import mysql.connector
import time
import random

import xlsxwriter # writing to excel

from datetime import datetime


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

# Execute experiments


host = <local_host>
user = <user>
password = <password>
database = <local_db>


my_db = mysql.connector.connect(user = user, password = password, host = host, database = database)


mycursor = my_db.cursor()


# Experiment setting

scaling_factor = 1
limit = int(scaling_factor*1500)


# scaling of number of orders with respect to original RF 1 and RF 2

percentage = 100 # adjustable to create / delete more records than specified in RF1 / RF2





# Initialise  time for RF1 and RF2
times = [[], []]


# perform experiment multiple times

runs = 20

outliers = 5

for i in range(0, runs):
    
        
    # shows if experiment is still running
    print()
    print(f"{runs-i} more experiments to go...")
    print()


    # Get orders keys for deletion

    query = "SELECT * FROM orders ORDER BY rand() LIMIT " + str(int(limit*percentage/100))


    mycursor.execute(query)

    order_keys = []
    for result in mycursor:
        order_keys.append(result[0])


    # Get customer keys

    query = "SELECT * FROM customer ORDER BY rand() LIMIT " + str(int(limit*percentage/100))

    mycursor.execute(query)

    customer_keys = []
    for result in mycursor:
        customer_keys.append(result[0])


    # get max orderkey

    query = "SELECT MAX(o.o_orderkey) FROM orders AS o"

    mycursor.execute(query)

    for result in mycursor:
        max_order_key = result[0]


    # create new order keys

    new_order_keys = [max_order_key + 1 + key for key in range(int(limit*percentage/100))]




    # RF 1 (insert ORDERS and LINEITEM)

    total_time = [0, 0]


    for order_number in range(int(limit*percentage/100)):
        query = "INSERT INTO orders VALUES (" + str(new_order_keys[order_number]) + ", " + str(customer_keys[order_number]) + ", 'x', 0.0, '2024-01-01', 'x', 'x', 0, 'new');"
        start = time.time()
        mycursor.execute(query)
        end = time.time()
        total_time[0] += (end - start) * 1000  # get time in miliseconds

        # get random PARTSUPP (ps_partkey, ps_suppkey) for new LINEITEMS

        query = "SELECT * FROM partsupp ORDER BY rand() LIMIT 7"
        mycursor.execute(query)
        partsupp_keys = []
        for result in mycursor:
            partsupp_keys.append(result[:2])

        for line_number in range(0, random.randrange(1,8)):
            query = "INSERT INTO lineitem VALUES (" + str(new_order_keys[order_number]) + ", " + str(partsupp_keys[line_number][0]) + ", " + str(partsupp_keys[line_number][1]) + ", " + str(line_number + 1) + ", 0.00, 0.00, 0.00, 0.00, 'x', 'x', '2024-01-01', '2024-01-01', '2024-01-01', 'x', 'x', 'new');"
            start = time.time()
            mycursor.execute(query)
            end = time.time()
            total_time[0] += (end - start) * 1000 # get time in miliseconds

        my_db.commit()


    # RF 2 (DELETE ORDERS and LINEITEM)
        
    for order_number in range(int(limit*percentage/100)):
        query = "DELETE FROM lineitem WHERE lineitem.L_ORDERKEY = " + str(order_keys[order_number])
        start = time.time()
        mycursor.execute(query)
        end = time.time()
        total_time[1] += (end - start) * 1000 # get time in miliseconds

        query = "DELETE FROM orders WHERE orders.O_ORDERKEY = " + str(order_keys[order_number])
        start = time.time()
        mycursor.execute(query)
        end = time.time()
        total_time[1] += (end - start) * 1000 # get time in miliseconds

        my_db.commit()


    times[0].append(round(total_time[0]))
    times[1].append(round(total_time[1]))


print()
print(times[0])
print(times[1])
print()


times[0] = [time_of_run for time_of_run in sorted(times[0])[outliers:-outliers]]
times[1] = [time_of_run for time_of_run in sorted(times[1])[outliers:-outliers]]

print()
print(times[0])
print(times[1])
print()

average_time = [[],[]]


average_time[0].append(round(sum(times[0])/(runs - 2*outliers)))
average_time[1].append(round(sum(times[1])/(runs - 2*outliers)))


print(average_time) 


# Experiment results

# current date and time
today = datetime.now().strftime("%Y_%m_%d")
current = datetime.now().strftime("%H_%M_%S")

filename = "Refresh_operations_results_" + str(scaling_factor) + "_" + str(today) + "---" + str(current) + ".xlsx"
sheetname = "Experiment"
experiment_name = f"TPC-H RF1 and RF2 with scaling factor {scaling_factor}"

# can probably be deleted
experiment_details = []

# create content
rf_1_content = [["Refresh operation RF1:"], ["Time (in ms): "] + times[0] + [" ", "Average time (in ms): "] + average_time[0]]
rf_2_content = [["Refresh operation RF2:"], ["Time (in ms): "] + times[1] + [" ", "Average time (in ms): "] + average_time[1]]
content = [[f"{percentage}% of original RF amount"],[],[]] + rf_1_content + [[''],['']] + rf_2_content

# writing to file
write_to_excel(filename, sheetname, experiment_name, experiment_details, content)

# closing db


my_db.close()
