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

# Benchmark queries

query_1 = ["""select
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
            sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
            from
            lineitem
            where
            l_shipdate <= DATE_ADD('1998-12-01', INTERVAL -(60 + floor(60*rand())) DAY)
            group by
            l_returnflag,
            l_linestatus
            order by
            l_returnflag,
            l_linestatus;"""]


query_2 = ["SET @types = JSON_ARRAY('TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER');",
            "SET @type = JSON_UNQUOTE(JSON_EXTRACT(@types, CONCAT('$[',FLOOR(4*RAND()),']')));",
            "SET @regions = JSON_ARRAY('AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST');",
            "SET @regionPosition = FLOOR(4*RAND());",
            "SET @region = JSON_UNQUOTE(JSON_EXTRACT(@regions, CONCAT('$[', @regionPosition,']')));",
            """select
            s_acctbal,
            s_name,
            n_name,
            p_partkey,
            p_mfgr,
            s_address,
            s_phone,
            s_comment
            from
            part,
            supplier,
            partsupp,
            nation,
            region
            where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and p_size = 1 + FLOOR(49*RAND())
            and p_type like CONCAT('%', @type)
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = @region
            and ps_supplycost = (
            select
            min(ps_supplycost)
            from
            partsupp, supplier,
            nation, region
            where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = @region
            )
            order by
            s_acctbal desc,
            n_name,
            s_name,
            p_partkey;"""]

query_3 = ["SET @segments = JSON_ARRAY('AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD');",
            "SET @segmentPosition = FLOOR(4*RAND());",
            "SET @segment = JSON_UNQUOTE(JSON_EXTRACT(@segments, CONCAT('$[', @segmentPosition,']')));",
            "SET @selectedDay = 1 + FLOOR(30*RAND());",
            "SET @selectedDate = date(CONCAT('1995-03-', @selectedDay));",
            """select
            l_orderkey,
            sum(l_extendedprice*(1-l_discount)) as revenue,
            o_orderdate,
            o_shippriority
            from
            customer,
            orders,
            lineitem
            where
            c_mktsegment = @segment
            and c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and o_orderdate < @selectedDate
            and l_shipdate > @selectedDate
            group by
            l_orderkey,
            o_orderdate,
            o_shippriority
            order by
            revenue desc,
            o_orderdate;"""]


query_4 = ["SET @selectedYear = FLOOR(1993 + 4*RAND());",
            "SET @selectedMonth = FLOOR(1+9*RAND());",
            "SET @startDate = date(CONCAT(@selectedYear, '-',@selectedMonth,'-01'));",
            "SET @endDate = date(CONCAT(@selectedYear, '-',@selectedMonth + 3,'-01'));",
            """select
            o_orderpriority,
            count(*) as order_count
            from
            orders
            where
            o_orderdate >= @startDate
            and o_orderdate < @endDate
            and exists (
            select
            *
            from
            lineitem
            where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
            )
            group by
            o_orderpriority
            order by
            o_orderpriority;"""]


query_5 = ["SET @regions = JSON_ARRAY('AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST');",
            "SET @regionPosition = FLOOR(4*RAND());",
            "SET @region = JSON_UNQUOTE(JSON_EXTRACT(@regions, CONCAT('$[', @regionPosition,']')));",
            "SET @selectedYear = 1993 + FLOOR(4*RAND());",
            "SET @startDate = date(CONCAT(@selectedYear, '-01-01'));",
            "SET @endDate = date(CONCAT(@selectedYear + 1, '-01-01'));",
            """select
            n_name,
            sum(l_extendedprice * (1 - l_discount)) as revenue
            from
            customer,
            orders,
            lineitem,
            supplier,
            nation,
            region
            where
            c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and l_suppkey = s_suppkey
            and c_nationkey = s_nationkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = @region
            and o_orderdate >= @startDate
            and o_orderdate < @endDate
            group by
            n_name
            order by
            revenue desc;"""]


query_6 = ["SET @selectedYear = 1993 + FLOOR(4*RAND());",
            "SET @startDate = date(CONCAT(@selectedYear, '-01-01'));",
            "SET @endDate = date(CONCAT(@selectedYear + 1, '-01-01'));",
            "SET @discount = floor(2+7*rand())/100",
            """select
            sum(l_extendedprice * l_discount) as revenue
            from
            lineitem
            where
            l_shipdate >= @startDate
            and l_shipdate < @endDate
            and l_discount between @discount - 0.01 and @discount + 0.01
            and l_quantity < floor(24 + 2*rand());"""]


query_7 = ["""SET @nations = JSON_ARRAY(
            'ALGERIA',
            'ARGENTINA',
            'BRAZIL',
            'CANADA',
            'EGYPT',
            'ETHIOPIA',
            'FRANCE',
            'GERMANY',
            'INDIA',
            'INDONESIA',
            'IRAN',
            'IRAQ',
            'JAPAN',
            'JORDAN',
            'KENYA',
            'MOROCCO',
            'MOZAMBIQUE',
            'PERU',
            'CHINA'
            'ROMANIA',
            'SAUDI ARABIA',  
            'VIETNAM',
            'RUSSIA',
            'UNITED KINGDOM',
            'UNITED STATES');""",
        "SET @nationPosition1 = FLOOR(25*RAND());",
        "SET @nationPosition2 = MOD(@nationPosition1 + 1 + FLOOR(23*RAND()), 25);",
        "SET @nation1 = JSON_UNQUOTE(JSON_EXTRACT(@nations, CONCAT('$[', @nationPosition1,']')));",
        "SET @nation2 = JSON_UNQUOTE(JSON_EXTRACT(@nations, CONCAT('$[', @nationPosition2,']')));",
        """select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
        from
        (
            select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
            from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
            where
            s_suppkey = l_suppkey
            and o_orderkey = l_orderkey
            and c_custkey = o_custkey
            and s_nationkey = n1.n_nationkey
            and c_nationkey = n2.n_nationkey
            and (
                (n1.n_name = @nation1 and n2.n_name = @nation2)
                or (n1.n_name = @nation2 and n2.n_name = @nation1)
            )   
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
        ) as shipping
        group by
        supp_nation,
        cust_nation,
        l_year
        order by
        supp_nation,
        cust_nation,
        l_year;"""]


query_8 = ["""SET @nationRegions = JSON_ARRAY(
            'AFRICA',        -- ALGERIA
            'AMERICA',       -- ARGENTINA
            'AMERICA',       -- BRAZIL
            'AMERICA',       -- CANADA
            'AFRICA',        -- EGYPT
            'AFRICA',        -- ETHIOPIA
            'EUROPE',        -- FRANCE
            'EUROPE',        -- GERMANY
            'ASIA',          -- INDIA
            'ASIA',          -- INDONESIA
            'ASIA',          -- IRAN
            'ASIA',          -- IRAQ
            'ASIA',          -- JAPAN
            'ASIA',          -- JORDAN
            'AFRICA',        -- KENYA
            'AFRICA',        -- MOROCCO
            'AFRICA',        -- MOZAMBIQUE
            'AMERICA',       -- PERU
            'ASIA',          -- CHINA
            'EUROPE',        -- ROMANIA
            'ASIA',          -- SAUDI ARABIA
            'ASIA',          -- VIETNAM
            'EUROPE',        -- RUSSIA
            'EUROPE',        -- UNITED KINGDOM
            'AMERICA'        -- UNITED STATES
        );""",
        """SET @nations = JSON_ARRAY(
            'ALGERIA',
            'ARGENTINA',
            'BRAZIL',
            'CANADA',
            'EGYPT',
            'ETHIOPIA',
            'FRANCE',
            'GERMANY',
            'INDIA',
            'INDONESIA',
            'IRAN',
            'IRAQ',
            'JAPAN',
            'JORDAN',
            'KENYA',
            'MOROCCO',
            'MOZAMBIQUE',
            'PERU',
            'CHINA'
            'ROMANIA',
            'SAUDI ARABIA',  
            'VIETNAM',
            'RUSSIA',
            'UNITED KINGDOM',
            'UNITED STATES');""",
        "SET @nationPosition = FLOOR(25*RAND());",
        "SET @nation = JSON_UNQUOTE(JSON_EXTRACT(@nations, CONCAT('$[', @nationPosition,']')));",
        "SET @region = JSON_UNQUOTE(JSON_EXTRACT(@nationRegions, CONCAT('$[', @nationPosition,']')));",
        "SET @types1 = JSON_ARRAY('STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO');",
        "SET @type1 = JSON_UNQUOTE(JSON_EXTRACT(@types1, CONCAT('$[',FLOOR(5*RAND()),']')));",
        "SET @types2 = JSON_ARRAY('ANODIZED', 'BURNISHED', 'PLATED', 'POLISHED', 'BRUSHED');",
        "SET @type2 = JSON_UNQUOTE(JSON_EXTRACT(@types2, CONCAT('$[',FLOOR(4*RAND()),']')));",
        "SET @types3 = JSON_ARRAY('TIN', 'NICKEL', 'BRASS', 'STEEL', 'COPPER');",
        "SET @type3 = JSON_UNQUOTE(JSON_EXTRACT(@types3, CONCAT('$[',FLOOR(4*RAND()),']')));",
        "SET @type = CONCAT(@type1, ' ', @type2, ' ', @type3);",
        """select
        o_year,
        sum(case
            when nation = @nation then volume
            else 0
        end) / sum(volume) as mkt_share
        from
        (
            select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
            from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
            where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = @region
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = @type
        ) as all_nations
        group by
        o_year
        order by
        o_year;"""]


query_9 = ["SET @colors = JSON_ARRAY('almond', 'antique', 'aquamarine', 'azure', 'beige', 'bisque', 'black', 'blanched', 'blue', 'blush', 'brown', 'burlywood', 'burnished', 'chartreuse', 'chiffon', 'chocolate', 'coral', 'cornflower', 'cornsilk', 'cream', 'cyan', 'dark', 'deep', 'dim', 'dodger', 'drab', 'firebrick', 'floral', 'forest', 'frosted', 'gainsboro', 'ghost', 'goldenrod', 'green', 'grey', 'honeydew', 'hot', 'indian', 'ivory', 'khaki', 'lace', 'lavender', 'lawn', 'lemon', 'light', 'lime', 'linen', 'magenta', 'maroon', 'medium', 'metallic', 'midnight', 'mint', 'misty', 'moccasin', 'navajo', 'navy', 'olive', 'orange', 'orchid', 'pale', 'papaya', 'peach', 'peru', 'pink', 'plum', 'powder', 'puff', 'purple', 'red', 'rose', 'rosy', 'royal', 'saddle', 'salmon', 'sandy', 'seashell', 'sienna', 'sky', 'slate', 'smoke', 'snow', 'spring', 'steel', 'tan', 'thistle', 'tomato', 'turquoise', 'violet', 'wheat', 'white', 'yellow');",
            "SET @colorPosition = FLOOR(80*RAND());",
            "SET @color = JSON_UNQUOTE(JSON_EXTRACT(@colors, CONCAT('$[',@colorPosition,']')));",
            """select
            nation,
            o_year,
            sum(amount) as sum_profit
            from
            (
                select
                n_name as nation,
                extract(year from o_orderdate) as o_year,
                l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                from
                part,
                supplier,
                lineitem,
                partsupp,
                orders,
                nation
                where
                s_suppkey = l_suppkey
                and ps_suppkey = l_suppkey
                and ps_partkey = l_partkey
                and p_partkey = l_partkey
                and o_orderkey = l_orderkey
                and s_nationkey = n_nationkey
                and p_name like CONCAT('%', @color)
            ) as profit
            group by
            nation,
            o_year
            order by
            nation,
            o_year desc;"""]


query_10 = ["SET @startDate = date('1993-02-01') + interval floor(25*rand()) month;",
            """select
                c_custkey,
                c_name,
                sum(l_extendedprice * (1 - l_discount)) as revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
                from
                customer,
                orders,
                lineitem,
                nation
                where
                c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and o_orderdate >= @startDate
                and o_orderdate < @startDate + interval '3' month
                and l_returnflag = 'R'
                and c_nationkey = n_nationkey
                group by
                c_custkey,
                c_name,
                c_acctbal,
                c_phone,
                n_name,
                c_address,
                c_comment
                order by
                revenue desc"""]






# Establish connection to MySQL instance
host = <local_host>
user = <user>
password = <password>
database = <local_db>





###############################
#                             #
#     Experiment settings     #
#                             #
###############################   


scaling_factor = 1 # sclaing factor for TPC-H (small = 0.01 / mdedium = 0.1 / large = 1)


query_number = 1 # required for writing to excel

query = query_1 # query to be executed



precision = 1 # precision for time measured in miliseconds for query execution



# amount of runs for experiments
runs = 20

# amount of bottom and top results based on amount of runs that will be disregarded for average caluclation
outliers = 5








# Initialise DB

my_db_conncetion = mysql.connector.connect(user = user, password = password, host = host, database = database)

my_cursor = my_db_conncetion.cursor(buffered=True)


# Initialise times
times = []




# perform experiment multiple times

for i in range(0, runs):
    
        
    # shows if experiment is still running
    print()
    print(f"{runs-i} more experiments to go...")
    print()




    # run benchmark query

    total_time = 0

    start = time.time()
    for subquery in query:
        my_cursor.execute(subquery, multi=True)
    end = time.time()
    total_time = (end - start) * 1000  # get time in miliseconds
    times.append(total_time)




times = [round(time_of_run, precision) for time_of_run in sorted(times)[outliers:-outliers]]


average_time = round(sum(times)/(runs - 2*outliers), precision)


# current date and time
today = datetime.now().strftime("%Y_%m_%d")
current = datetime.now().strftime("%H_%M_%S")

# Experiment results

filename = "Banchmark_query_results_" + "query_" + str(query_number) + "___" + str(scaling_factor) + "_" + str(today) + "---" + str(current) + ".xlsx"
sheetname = "Experiment"
experiment_name = f"TPC-H benchmark queries with scaling factor {scaling_factor}"

# can probably be deleted
experiment_details = []

# create content for Excel
content = [["Benchmark query number " + str(query_number) + ": " + "".join(query)],[],[]]
content.append(["Time (in ms): "] + times + [" ", "Average time (in ms): "] + [average_time])

# writing to file
write_to_excel(filename, sheetname, experiment_name, experiment_details, content)

# closing db

my_cursor.close()

my_db_conncetion.close()

