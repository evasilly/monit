# -*- coding: utf-8 -*-

import time
import mysql.connector
import threading

cnx = mysql.connector.connect(user='dbuser', password='dbpassword', database='monit')

cursor = cnx.cursor(buffered=True)


def proc():
    cursor.execute(u"""select * from locktable;""")


def main():
    cursor.execute(u"""lock tables locktable read;""")
    runlist = []
    for i in range(50):
        tname = '%s' % (i+1)
        thread = threading.Thread(name=tname, target=proc).start()
        runlist.append(thread)
        time.sleep(5)
    time.sleep(100)
    cursor.execute(u"""unlock tables;""")
    runlist.join[i]
    cursor.close()
    cnx.close()


if __name__ == '__main__':
    main()