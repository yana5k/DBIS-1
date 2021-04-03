import psycopg2
import csv
import connect
import os

import time

source_file_start = 'Odata'  # between start and end year will be located. It's importing for running code
source_file_end = 'File.csv'
path_result_time = 'result_time.txt'
path_result_query = 'result_querry.csv'

#-----------------------------------------------
#section for working with DB
#-----------------------------------------------  
def take_header(year):
    with open(source_file_start + str(year) + source_file_end, encoding='cp1251') as f:
        header = [h.strip('"') for h in f.readline().strip().split(';')]
        header.append('year')
        f.close()
        return header

def create_table(header):

    begin_creation = """CREATE TABLE IF NOT EXISTS results ("""
    end_creation = """);"""

    for names in header:
        if names == "OUTID":datatype = "varchar(100) PRIMARY KEY"
        elif names == "Birth" or names == "year": datatype = "smallint"
        elif "Ball" in names: datatype = "numeric"
        else: datatype = "TEXT"
        continuation = """\n    {} {},""".format(names, datatype)
        begin_creation += continuation

    creation = begin_creation[:-1] + end_creation
    return creation

def work_with_DB_table(drop_table, conn):
    print('Start table creation')
    t0 = time.perf_counter()
    cur = conn.cursor()
    drop = ("""DROP TABLE IF EXISTS results;""")
    create = create_table(take_header(2019))
    if not (drop_table):
        cur.execute(drop)
        conn.commit()
    cur.execute(create)
    conn.commit()
    cur.close()
    t1 = time.perf_counter()
    with open(path_result_time,"w") as ftime:
        ftime.write("Creating Tables: \n")
        ftime.write(str(t1-t0)+"s\n")
    print('End table creation')
    
#-----------------------------------------------
#creating the temporary file for working with data
#and upload temp file to database
#-----------------------------------------------            
def recording_from_year_file(year,conn):
    t0 = time.perf_counter()
    print('Start Uploading ' + str(year) + 'file. . .')
    path = 'tempfile.csv'
    step = 5000
    max_row_number = step
    with open(source_file_start + str(year) + source_file_end, encoding='cp1251') as f:
        f.readline()
        data = csv.reader(f,delimiter=';',quotechar='"',quoting=csv.QUOTE_ALL)    
        i = 1
        open_empty_file(path)
        
        IN_DB_ROWS = check_downloads(conn, year)
        if IN_DB_ROWS:
            max_row_number = IN_DB_ROWS + step
            
        for row in data:
            if IN_DB_ROWS < i:
                i, max_row_number = write_row_to_temp_file(path, row, i, step, max_row_number,year,conn) 
            else:
                print(i, ' is already in file')
                if IN_DB_ROWS == i and not check_file(path):
                    print('Starting coping from file new lines')
                    time.sleep(2)
                i += 1        
    if check_file(path):
        copy_file_to_database(path,conn)
    os.remove(path)
    print('End Uploading ' + str(year) + 'file to DB')
    t1 = time.perf_counter()
    with open(path_result_time,"a") as ftime:
        ftime.write("Upload data from "+ str(year) + " file:\n")
        ftime.write(str(t1-t0)+"s\n")
    time.sleep(2)
        

def prepare_values_for_recording(enter_arr, year):
    result = []
    for value in enter_arr:
        result.append(clean_csv_value(value)) 
    result.append(year)
    return result

def clean_csv_value(value):
    if value == 'null':
        return value
    try:
        res = int(float(value.replace(',', '.')))
        return res
    except:
        try:
            res = float(value.replace(',', '.'))
            return res
        except:
            return value

def copy_file_to_database(path, conn):
    cur = conn.cursor()
    with open(path,'r') as file:
        cur.copy_from(file, 'results', sep=';', null='null')
        conn.commit()
        print('File Copied')
    cur.close() 
    open_empty_file(path)

def read_file(file):
    for row in file:
        print(row)
    file.seek(0)

def write_row_to_temp_file(path, row, i, step, max_row_number,year, conn):
    print(year,' <> ', i)
    temp = open(path, mode='a+', newline='')
    csv_writer = csv.writer(temp, delimiter=';')
    try:
        csv_writer.writerow(prepare_values_for_recording(row,year))
    except ex as Exception:
        print(ex)
    temp.close()
    i += 1
    if max_row_number < i:
        print("-------------------------------------------------------------------------------------")
        max_row_number += step                       
        copy_file_to_database(path,conn)
    return i, max_row_number

def check_downloads(conn, year):
    count_rows = 'SELECT count(*) FROM public.results where year=' + str(year)

    cur = conn.cursor()
    cur.execute(count_rows)
    conn.commit()
    result = cur.fetchall()
    cur.close()
    print('IN DB ROWS: ',result[0][0])
    time.sleep(2)
    return int(result[0][0])

def open_empty_file(path):
    file = open(path,'w+')
    file.close()

def check_file(path):
    line = ''
    f = open(path,"r") 
    line = f.readline()
    f.close()
    if line == '' :
        return False
    return True

#-----------------------------------------------
#Executing querry and save results
#----------------------------------------------- 
querry = ("""select res2019.regname  as "Region",
        res2019.eng_avg as "English 2019",
        res2020.eng_avg as "English 2020"
    from (select regname, avg(engball100) eng_avg
        from results
        where results.engteststatus = 'Зараховано'
            and results.year = 2019
        group by results.regname) as res2019
            join
        (select regname, avg(engball100) as eng_avg
        from results
        where results.engteststatus = 'Зараховано'
            and results.year = 2020
        group by results.regname) as res2020
        on res2019.regname = res2020.regname
    order by "Region";""")

def do_querry(querry,conn):
    print('Start querry executing')
    t0 = time.perf_counter()
    
    cur = conn.cursor()
    cur.execute(querry)
    conn.commit()
    rows = cur.fetchall()
    cur.close()
    
    t1 = time.perf_counter()
    with open(path_result_time,"a") as ftime:
        ftime.write("Execute querry:\n")
        ftime.write(str(t1-t0)+"s\n")
    
    write_data_in_csv(rows)
    print('End querry executing')

def write_data_in_csv(rows):
    header = ['Region','English 2019','English 2020']
    with open(path_result_query, 'w+', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(header)
        for row in rows:
            writer.writerow(get_result_line_arr(row))

def get_result_line_arr(row):
    result = []
    for elem in row:
        result.append(elem)
    return result

if __name__ == '__main__':
    conn = connect.connect()
    work_with_DB_table(1,conn)
    
    recording_from_year_file(2019,conn)
    recording_from_year_file(2020,conn)

    do_querry(querry,conn)
    connect.disconnect(conn)