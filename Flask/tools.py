from config import *
from sqlalchemy import create_engine
import os
import json
from termcolor import colored

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='kafka:29092')

currentDirectory = 'sqlite:///'+ os.getcwd() + '/flask_auth_db.db'
e = create_engine(currentDirectory,connect_args={'timeout': 5})

# ===============================================================================================
#
# ===============================================================================================
def val_user(username, password,api_name):
    conn = e.connect()
    query = conn.execute("SELECT al1.id FROM api_users al1 join map_users_api al2 on al1.id = al2.id_api_users join api_list al3 on al2.id_api_list = al3.id where al1.username = ? and al1.password = ? and al3.name = ?", [username,password,api_name])
    
    id = 0
    for row in query:
        id = row[0]
    conn.close()

    if id > 0:
        return 1
    else:
        print (colored("========================================================================",color="red"))
        print(colored('============ || BAD USER NAME || ============',color='yellow',on_color='on_white'))
        print (colored("========================================================================",color="red"))
        return 0

# ===============================================================================================
#
# ===============================================================================================
def log_to_kafka(topic, event):
    print (colored("========================================================================",color="red"))
    print(colored('============ || FLASK SENDING TO KAFKA || ============',color='yellow',on_color='on_white'))

    producer.send(topic, event.encode())
    print(colored("Json Load  = ",color='red') + str(event))

    return 1
# ===============================================================================================
#
# ===============================================================================================