import stat
import os
import time
import multiprocessing
import subprocess
from time import sleep
from random import seed
from random import random
from random import randint
from termcolor import colored

import json
import requests
from flask import (Flask, jsonify, abort, request, make_response, url_for, Response)

from tools import *

class Puller_Url(multiprocessing.Process):
# ==============================================================
#
# ==============================================================
    def run(self):
        seed(1)
        self.pull_url()
        return
# ==============================================================
#
# ==============================================================
    def pull_url(self):
        while True:
            sleep(get_sleep())
            self.get_json_load()
            print (colored("========================================================================",color="red"))
            
            user = get_user()
            server_input, url  = self.get_json_load()	
            http_headers = get_http_headers()
            print(colored('============ || CALLING FLASK API || ============',color='yellow',on_color='on_white'))
            print(colored("User/Pass  = ",color='red') + colored(str(user),color='green'))
            print(colored("API Call   = ",color='red') + colored(str(url),color='green'))
            print(colored("Json Load  = ",color='red') + colored(str(server_input),color='green'))
            
            try:
                r = requests.post(url,auth=user,headers=http_headers,timeout=20,json=server_input)
                print(colored("Response  = ",color='red') + colored(r,color='green'))
            except requests.exceptions.RequestException as e:
                print(colored("Error  = ",color='red') + colored(e,color='red',on_color='on_white'))
            print (colored("========================================================================",color="red"))
# ==============================================================
#
# ==============================================================
    def get_json_load(self):
        random_number = randint(1,3)

        if random_number == 1:
            return get_json_1()
        
        if random_number == 2:
            return get_json_2()
        
        if random_number == 3:
            return get_json_3()
                                                    
# ==============================================================
#
# ==============================================================
