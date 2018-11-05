##############################################
#   Nicasia Beebe-Wang 1723387 nbbwang@uw.edu
#   Ayse Berceste Dincer 1723315 abdincer@uw.edu
#
# Code for CSE 550 HW2
# 
# Code is modified from: https://github.com/gdub/python-paxos 
##############################################


from collections import namedtuple, defaultdict
from multiprocessing import Process, Queue, JoinableQueue
import random
import time
import socket, threading
import queue
import numpy as np

from Messages import *
from Node import *


#Messenger class for sending and receiving messages across nodes
class Messenger:
    
    def __init__(self, system, num_nodes, failure_states = None, fail_ratio = 0):
    

        self.num_nodes = num_nodes
        self.system = system
        self.funnel = Queue()
        self.inbox = [Queue() for i in range(self.num_nodes)]
        self.message_count = 0
        self.fail_ratio = fail_ratio

        self.active = True
        self.terminate = False

        self.failure_states = failure_states

    def set_failures(failure_states):
        self.failure_states = failure_states

    #Run forever to receive and send messages
    def run(self):
        while True:
            if not self.active and self.terminate:
                break
            try:
                dest, msg = self.funnel.get(timeout=0.5)
            except queue.Empty:
                pass
            else:
                self.inbox[dest].put(msg)

    #Method for sending message
    def send(self, from_, to, msg):
        if self.failure_states[from_][0] == 1:
            print("!!!!!!!!! Node {} failed".format(from_))
        else:
            if isinstance(msg, PrepareMsg) and (self.failure_states[from_][1] == 1 or self.failure_states[from_][0] == 1):
                #print("!!!!!!!!! MESSAGE failed to be sent from Node {} (message = {})".format(from_, msg))
                self.failure_states[from_][0] = 1 
            elif isinstance(msg, AcceptMsg) and (self.failure_states[from_][2] == 1 or self.failure_states[from_][0] == 1):
                #print("!!!!!!!!! MESSAGE failed to be sent from Node {} (message = {})".format(from_, msg))
                self.failure_states[from_][0] = 1 
            elif isinstance(msg, PrepareResponseMsg) and (self.failure_states[from_][3] == 1 or self.failure_states[from_][0] == 1):
                #print("!!!!!!!!! MESSAGE failed to be sent from Node {} (message = {})".format(from_, msg))
                self.failure_states[from_][0] = 1
            elif isinstance(msg, AcceptResponseMsg) and (self.failure_states[from_][4] == 1 or self.failure_states[from_][0] == 1):
                #print("!!!!!!!!! MESSAGE failed to be sent from Node {} (message = {})".format(from_, msg))
                self.failure_states[from_][0] = 1
            else:
                if isinstance(msg, ClientRequestMsg): 
                    self.message_count += 1
                    self.funnel.put((to, msg))
                else:
                    #Create random failure rate
                    fail_prob = random.uniform(0, 1)
                    if fail_prob < self.fail_ratio:
                        print("!!!!!!!!! MESSAGE failed to be sent from Node {} (message = {}) with fail prob {}".format(from_, msg, fail_prob))
                    else:
                        self.message_count += 1
                        self.funnel.put((to, msg))


    #Method for receiving message
    def recv(self, from_):
        return self.inbox[from_].get()

    def task_done(self, pid):
        self.funnel.task_done()



#Class for defining the Paxos system consisting of nodes
class PaxosServer():
   
    #Create the default system
    def __init__(self, num_nodes, num_locks, failure_states = None, fail_ratio = 0):

        #Queues for log values
        self.accepted_results_q = Queue()
        self.failed_results_q = Queue()

        #set the ids of agents in the system
        self.num_nodes = num_nodes
        self.node_ids = list(range(0, num_nodes))
        self.message_timeout= 3
        self.num_locks = num_locks
        self.failure_states = failure_states
        self.fail_ratio = fail_ratio

        #start messenger threads
        self.messenger = Messenger(self, self.num_nodes, self.failure_states, self.fail_ratio)
        self.messenger_thread = Thread(target=self.messenger.run)
        self.messenger_thread.start()

        #start all node processes
        self.processes = self.launch_processes()

    #Method for logging the successful results
    def log_result(self, source, instance, value, status):
        self.accepted_results_q.put((source, instance, value, status))

    #Method for logging the failed results
    def log_failure(self, source, value, status):
        self.failed_results_q.put((source, value, status))

    #Method for starting all processes
    def launch_processes(self) :
        processes = []
        for pid in range(self.num_nodes):
            node = Node(pid, self, self.messenger)
            p = Process(target=node.run)
            p.start()
            processes.append(p)
        return processes

    #Join all node processes
    def join(self):
        for process in self.processes:
            process.join()



#Thread for managing the communication between lock server and clients
class CommunicationThread(threading.Thread):
    
    def __init__(self):
        threading.Thread.__init__(self)
        
    #Run forever to receive and send client messages 
    def run(self):

        while True:

            #Read from the successful request queue and send message to user
            while not system.accepted_results_q.empty():
                s,i,v, status = system.accepted_results_q.get()
                if i not in instance_results_sent_log:
                    instance_results_sent_log.add(i)                    
                    clientSocket = client_dict[v["client_id"]]
                    if v["type"] == 'lock':
                        msg = "SUCCESSFULLY ACQUIRED LOCK " + str(v["lock_id"])
                    elif v["type"] == 'unlock':
                        msg = "SUCCESSFULLY RELEASED LOCK " + str(v["lock_id"])
                    clientSocket.send(bytes(msg,'UTF-8'))

            #Read from the failed request queue and send message to user
            while not system.failed_results_q.empty():
                s2, v2, status2 = system.failed_results_q.get()
                    
                clientSocket = client_dict[v2["client_id"]]
                if v2["type"] == 'lock':
                    msg2 = "FAILURE TO ACQUIRE LOCK " + str(v2["lock_id"])
                elif v2["type"] == 'unlock':
                    msg2 = "FAILURE TO RELEASE LOCK " + str(v2["lock_id"])
                clientSocket.send(bytes(msg2,'UTF-8'))

                
class NodeFailureThread(threading.Thread):
    
    def __init__(self, messenger):
        threading.Thread.__init__(self)
        self.messenger = messenger
        
    #Run forever to receive failed nodes from server
    def run(self):
        while True:
            out_data = input()
            tokens = out_data.split(" ")
            if len(tokens) == 2 and tokens[0] == 'quit':
                print("Node {} is stopped".format(tokens[1]))
                self.messenger.send(-1, int(tokens[1]), "quit")


#Creating a new thread for handling client requests independently
class ClientThread(threading.Thread):

    #Initiate new client threas
    def __init__(self,clientAddress,clientsocket):
        threading.Thread.__init__(self)
        self.csocket = clientsocket
        self.clientAddress = clientAddress
        print ("New connection added: ", self.clientAddress)
    
    #Receive and send client requests
    def run(self):

        print ("Connection from : ", self.clientAddress)
        self.csocket.send(bytes("Hi, you are client " + str(self.clientAddress),'utf-8'))
        
        msg = ''
        
        #Keep receiving messages until client decides to quit
        while True:
            data = self.csocket.recv(2048)
            msg = data.decode()
            if msg=='bye' or msg == '':
                break
            
            split_messages  = msg.split(" ")
            if len(split_messages) == 2:

                method = split_messages[0]
                lock_id = split_messages[1]

                #If the request is lock, send a lock request to the system
                if method == "lock" and lock_id in locks:
                    print("LOCKING")
                    print("LOCK ID", lock_id)
                    print("CLIENT", self.clientAddress[1])

                    message = {"type": "lock", "lock_id": int(lock_id), "client_id": self.clientAddress[1]}
                    node = random.randint(0, number_of_nodes - 1)
                    print("SENDING TO NODE", node)
                    system.messenger.send(-1, node,ClientRequestMsg(None, message))

                #If the request is unlock, send an unlock request to the system   
                elif method == "unlock" and lock_id in locks:
                    print("UNLOCKING")
                    print("LOCK ID", lock_id)
                    print("CLIENT", self.clientAddress[1])

                    message = {"type": "unlock", "lock_id": int(lock_id), "client_id": self.clientAddress[1]}
                    node = random.randint(0, number_of_nodes - 1)
                    print("SENDING TO NODE", node)
                    system.messenger.send(-1, node,ClientRequestMsg(None, message))
 
                else:
                    print ("wrong format", msg)
                    msg = "Wrong format for message, you should have lock/unlock x "
                    self.csocket.send(bytes(msg,'UTF-8'))

            else:
                print ("wrong format", msg)
                msg = "Wrong format for message, you should have lock/unlock x "
                self.csocket.send(bytes(msg,'UTF-8'))
            
        
        print ("Client at ", self.clientAddress, " disconnected...")   

import sys

if __name__ == "__main__":

    ######################################
    #DEFINE THE LOCK SERVER SYSTEM
    number_of_locks = 5
    locks = ['0','1','2','3','4']
    number_of_nodes = 5
    if( len(sys.argv) == 1):
        print("All nodes in the lock server is active")
        system = PaxosServer(number_of_nodes, number_of_locks, np.zeros((number_of_nodes, 5)))
    
    #Get the system failure state from user
    elif int(sys.argv[1]) == 1:
        print("Node 1 will fail after sending proposal")
        failure_states = np.zeros((number_of_nodes, 5))
        failure_states[1, 1] = 1
        system = PaxosServer(number_of_nodes, number_of_locks, failure_states)
    
    elif int(sys.argv[1]) == 2:
        print("Node 2 will fail after sending accept")
        failure_states = np.zeros((number_of_nodes, 5))
        failure_states[2, 2] = 1
        system = PaxosServer(number_of_nodes, number_of_locks, failure_states)
    
    elif int(sys.argv[1]) == 3:
        print("Node 3 will fail after receiving prepare message")
        failure_states = np.zeros((number_of_nodes, 5))
        failure_states[3, 3] = 1
        system = PaxosServer(number_of_nodes, number_of_locks, failure_states) 

    elif int(sys.argv[1]) == 4:
        print("Node 4 will fail after receiving accept message")
        failure_states = np.zeros((number_of_nodes, 5))
        failure_states[4, 4] = 1
        system = PaxosServer(number_of_nodes, number_of_locks, failure_states) 

    elif int(sys.argv[1]) == 5:
        print("Node 2 and 3 will fail after receiving prepare message")
        failure_states = np.zeros((number_of_nodes, 5))
        failure_states[2, 3] = 1
        failure_states[3, 3] = 1
        system = PaxosServer(number_of_nodes, number_of_locks, failure_states) 

    elif int(sys.argv[1]) == 6:
        print("Node 0 and 1 will fail after receiving accept message")
        failure_states = np.zeros((number_of_nodes, 5))
        failure_states[0, 4] = 1
        failure_states[1, 4] = 1
        system = PaxosServer(number_of_nodes, number_of_locks, failure_states) 
    
    elif int(sys.argv[1]) == 7:
        print("Node 0 and 1 and 4 will fail after receiving accept message")
        failure_states = np.zeros((number_of_nodes, 5))
        failure_states[0, 4] = 1
        failure_states[1, 4] = 1
        failure_states[4, 4] = 1
        system = PaxosServer(number_of_nodes, number_of_locks, failure_states) 
    
    elif int(sys.argv[1]) == 0:
        print("All nodes in the lock server is active and there is a 0.2 chance of a message failure")
        system = PaxosServer(number_of_nodes, number_of_locks, np.zeros((number_of_nodes, 5)), 0.2) 
        


    #Define lock server ports and satrt system
    LOCALHOST = "127.0.0.1"
    PORT = 8080
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((LOCALHOST, PORT))
    
    print("Lock server is started")

    #keep track of all results sent to user
    instance_results_sent_log = set()
        
    #keep a dictionary of all clients
    client_dict = {}

    #Create thread for handling client requests
    print("Waiting for client requests...")
    communicationThread = CommunicationThread()
    communicationThread.start()
    failureThread = NodeFailureThread(system.messenger)
    failureThread.start()

    #Keep receiving new client connections and creating new threads for them
    while True:
        server.listen(1)
        clientsock, clientAddress = server.accept()
        client_dict[clientAddress[1]] = clientsock
        newthread = ClientThread(clientAddress, clientsock)
        newthread.start()



     

       