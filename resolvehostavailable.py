# -*- coding: utf-8 -*-
import mysql.connector
import threading
import csv
import datetime
import time
from subprocess import Popen, PIPE

host_status = {}
# Verificar
csv_lock = threading.Lock()

def get_target_hosts(query):
    """Fetches target hosts from the database.

    Args:
        query (str): The SQL query to execute.

    Returns:
        list: A list of lists containing the effective host data.
    """

    result = ""
    try:
        # Verificar
        connection = mysql.connector.connect(host="192.168.1.40", user="root", password="agustin", database="datamodem")
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        connection.close()
    except Exception as e:
        print("Error:", str(e))
        result = str(e) + " S" + str(datetime.now().time())
        return 0

    if not result:
        print("The query returned no information for the date:", str(datetime.datetime.now()))
        return 1

    data_encoded = [[item for item in row] for row in result]
    effective_data = [row[1:] for row in data_encoded]  # Remove unnecessary IDs
    return effective_data


def host_is_available(host):
    """Checks if a host is available and writes the status to the CSV file.

    Args:
        host (list): A list containing host information (IP address might be in a specific index).
    """

    global host_status

    if host[1] is not None:
        cmd = ['ping', '-c', '1', host[1]]
        process = Popen(cmd, stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()
        host_status[host[1]] = "True" if process.returncode == 0 else "False"
        host.append(host_status[host[1]])
    else:
        host.append("False")

    # Synchronized CSV writing with lock
    with csv_lock:
        with open("archivo.csv", 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(host)

if __name__ == "__main__":
    
    query = "select * from cablemodem order by id;"
    target_hosts = get_target_hosts(query)
    # Modificar
    numbers_of_elements = 100
    # Check availability for each target host
    i=1
    threads = []
    inicio = time.time()
    for targethost in target_hosts:
        thread = threading.Thread(target=host_is_available, args=(targethost,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()  
        print("Status: "+str(round(i/numbers_of_elements,4)*100)+" % Completed")
        i+=1
    fin = time.time()
    print("Availability checks completed. Results saved to archivo.csv")
    print("Tiempo de ejecucion aproximado: "+str(fin-inicio)+" s")
    