from collections import namedtuple
from multiprocessing import Queue
import random
import time

from __init__ import SystemConfig
from messages import *
from sim import System, Mailbox
 
import socket, threading
import random

class CommunicationThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        
    def run(self):

        print("empty queue?:", system.logger.accepted_results_q.empty())
        while True:
            while not system.logger.accepted_results_q.empty():
                s,i,v, status = system.logger.accepted_results_q.get()
                if i not in instance_results_sent_log:
                    instance_results_sent_log.add(i)
                    print("^^^^^^^^^^^^^^^^^^^^^^^^^^^Success for node %i, instance %i"%(s,i), v, status)
                    
                    clientSocket = client_dict[v["client_id"]]
                    if v["type"] == 'lock':
                        msg = "SUCCESSFULLY ACQUIRED LOCK " + str(v["lock_id"])
                    elif v["type"] == 'unlock':
                        msg = "SUCCESSFULLY RELEASED LOCK " + str(v["lock_id"])
                    clientSocket.send(bytes(msg,'UTF-8'))



            while not system.logger.failed_results_q.empty():
                s2, v2, status2 = system.logger.failed_results_q.get()
                
                print("^^^^^^^^^^^^^^^^^^^^^^^^^^^Failure for node %i "%(s2), v2, status2)
                    
                clientSocket = client_dict[v2["client_id"]]
                if v2["type"] == 'lock':
                    msg2 = "FAILURE TO ACQUIRE LOCK " + str(v2["lock_id"])
                elif v2["type"] == 'unlock':
                    msg2 = "FAILURE TO RELEASE LOCK " + str(v2["lock_id"])
                clientSocket.send(bytes(msg2,'UTF-8'))

                


        

class ClientThread(threading.Thread):
    def __init__(self,clientAddress,clientsocket):
        threading.Thread.__init__(self)
        self.csocket = clientsocket
        self.clientAddress = clientAddress
        print ("New connection added: ", self.clientAddress)
    def run(self):
        print ("Connection from : ", self.clientAddress)

        self.csocket.send(bytes("Hi, you are client " + str(self.clientAddress),'utf-8'))
        
        msg = ''
        while True:
            data = self.csocket.recv(2048)
            msg = data.decode()
            if msg=='bye' or msg == '':
                break
            
            split_messages  = msg.split(" ")
            if len(split_messages) == 2:

                method = split_messages[0]
                lock_id = split_messages[1]

                if method == "lock" and lock_id in locks:
                    print("LOCKING")
                    print("LOCK ID", lock_id)
                    print("CLIENT", self.clientAddress[1])

                    message = {"type": "lock", "lock_id": int(lock_id), "client_id": self.clientAddress[1]}
                    node = random.randint(0, number_of_locks - 1)
                    print("SENDING TO NODE", node)
                    system.mailbox.send(node,ClientRequestMsg(None, message))

                    
                elif method == "unlock" and lock_id in locks:
                    print("UNLOCKING")
                    print("LOCK ID", lock_id)
                    print("CLIENT", self.clientAddress[1])

                    message = {"type": "unlock", "lock_id": int(lock_id), "client_id": self.clientAddress[1]}

                    node = random.randint(0, number_of_locks - 1)
                    print("SENDING TO NODE", node)
                    system.mailbox.send(node,ClientRequestMsg(None, message))
 
                else:
                    print ("wrong format", msg)
                    msg = "Wrong format for message, you should have lock/unlock x "
                    self.csocket.send(bytes(msg,'UTF-8'))

            else:
                print ("wrong format", msg)
                msg = "Wrong format for message, you should have lock/unlock x "
                self.csocket.send(bytes(msg,'UTF-8'))
            
        
        print ("Client at ", self.clientAddress, " disconnected...")   


number_of_locks = 5
locks = ['0','1','2','3','4']

LOCALHOST = "127.0.0.1"
PORT = 8080
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((LOCALHOST, PORT))
print("Server started")

instance_results_sent_log = set()
    

client_dict = {}

system = System(SystemConfig(5))
system.start()

    
print("Waiting for client request..")
communicationthread = CommunicationThread()
communicationthread.start()

while True:
    server.listen(1)
    clientsock, clientAddress = server.accept()
    client_dict[clientAddress[1]] = clientsock
    newthread = ClientThread(clientAddress, clientsock)
    newthread.start()

   
system.shutdown_agents()
system.logger.print_results()


system.quit()
