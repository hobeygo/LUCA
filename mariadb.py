import mysql.connector as mariadb

mariadb_connection = mariadb.connect(user='root', password='example', database='luca', port=3306)
cursor = mariadb_connection.cursor()

try:
  cursor.execute("INSERT INTO testing (id,text) VALUES (%s,%s)", ("3", "Testing 3"))
  mariadb_connection.commit()
except mariadb.Error as error:
    print("Error: {}".format(error))

cursor.execute("SELECT id, text FROM testing")

for id, text in cursor:
    print("ID:"+str(id)+", TEXT: "+text)