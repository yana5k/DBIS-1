import psycopg2
import csv
import connect
import create_table
import tempfile
import os

conn = connect.connect()

#creating the table
#-----------------------------------------------
# cur = conn.cursor()
# drop = ("""DROP TABLE IF EXISTS results;""")
# command = create_table.create_table()
# cur.execute(drop)
# cur.execute(command)
# cur.close()
#-----------------------------------------------

#creating the temporary file for working with data
#-----------------------------------------------
def recording_from_year_file(year):
    path = 'tempfile.csv'
    step = 10
    max_row_number = step
    with open('Odata' + str(year) + 'File.csv',encoding='cp1251') as f:
        f.readline()
        # data = csv.reader(f,delimiter=';')
        data = csv.reader(f,delimiter=';',quotechar='"')
        i = 0
        
        with open(path,mode='w+') as temp:
            for row in data:
                csv_writer = csv.writer(temp,delimiter=';')
                if i < max_row_number:
                    print(i)
                    csv_writer.writerow(prepare_values_for_recording(row,year))
                    i += 1
                else:
                    """Remove this if! This block is for inserting in DB""" 
                    if i < 25:
                        max_row_number += step
                    else:
                        break
            cur = conn.cursor()
            # copy_data = ("""COPY results FROM \'""" + path + """\' WITH (FORMAT csv, DELIMITER \',\', QUOTE \'|\')""")
            # print(copy_data)
            temp.seek(0)

            cur.copy_from(temp,'results',sep=';',null='null')
            cur.close() 
        # os.remove(path)                    #deleting temp file  
            
        

def prepare_values_for_recording(enter_arr, year):
    result = []
    for value in enter_arr:
        result.append(clean_csv_value(value)) 
    result.append(year)
    return result

def clean_csv_value(value):
    # if value == 'null':
    #     return value
    try:
        res = int(float(value.replace(',', '.')))
        return res
    except:
        try:
            res = float(value.replace(',', '.'))
            return res
        except:
            return value
            # return value.replace('\'','*')

if __name__ == '__main__':
    recording_from_year_file(2019)