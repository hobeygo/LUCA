import mysql.connector as mariadb

mariadb_connection = mariadb.connect(user='root', password='example', database='static')

cursor = mariadb_connection.cursor()

