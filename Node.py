##############################################
#   Nicasia Beebe-Wang 1723387 nbbwang@uw.edu
#   Ayse Berceste Dincer 1723315 abdincer@uw.edu
#
# Code for CSE 550 HW2
# 
# Code is modified from: https://github.com/gdub/python-paxos 
##############################################

import sys
import time
from threading import Thread
from multiprocessing import Queue
from queue import Empty

from Messages import *
from Paxos import *

#Basic class for a node in Paxos algorithm
class Node:
  
    def __init__(self, pid, system, messenger):
        
        self.pid = pid #pid of the node
        self.system = system #keep a reference to the system to get the ids of other nodes
        self.active = True #decide whether to stop the node
        self.messenger = messenger #messenger for sending messages

        ######## list of variables for managing a certain number of locks #########
        self.logged_values = [] #keep a list of values logged by paxos
        self.states_list_print =[] #keep a list of states for lock statuses
        num_locks = system.num_locks #number of locks to manage
        self.lock_status_list = [None]*num_locks #keep the statuses for locks

        ######## list of variables for handling rejected proposals #########
        self.rejections_seen = set() #keep list of rejected proposal numbers
        self.valid_client_proposals = {} #create a dictionary for all proposals and their values
        
        ######## list of variables for PROPOSER #########
        self.sequence = self.pid #initial proposal number assigned to the first proposal coming by the node, using pid to make it unique
        self.sequence_step = len(self.system.node_ids) #number to increase the proposal number every time, increase by the number of nodes
        self.proposer_instances = {} #list of PROPOSER PROTOCOL instances
        self.instance_sequence = 1 #the instance number for the Paxos algorithm
        
        ######## list of variables for ACCEPTOR #########
        self.acceptor_instances = {} #list of ACCEPTOR PROTOCOL instances
        
        ######## list of variables for LEARNER #########
        self.results = {} #keep list of all results in the system
        self.learner_instances = {} #list of LEARNER PROTOCOL instances
        
        
    #Method for running a node forever
    def run(self):
        
        print("Node {} started".format(self.pid))
        #continue receiving while node is alive
        while self.active:
            msg = self.recv() #receive the message
            self.handle_message(msg) #handle the message 
        print("Node {} shutting down".format(self.pid))

    #Method for sending message from the node to another node
    def send_message(self, msg, pids):
        for pid in pids:
            print("Node {} sending message to {} (message = {})".format(self.pid, pid, msg))
            self.messenger.send(self.pid, pid, msg)

    #Method for receiving message from other nodes
    def recv(self):
        msg = self.messenger.recv(self.pid)
        source = getattr(msg, 'source', None)
        print("Node {} received message from {} (message = {})".format(self.pid, source, msg))
        return msg

    def message_done(self):
        self.messenger.task_done(self.pid)

    #Method for deciding on actions for different message types
    def handle_message(self, msg):

        ######### SYSTEM QUIT MESSAGE ###########
        if msg == 'quit':
            self.handle_quit()
            
        ######### PROPOSER MESSAGES ###########
        if isinstance(msg, ClientRequestMsg):
            self.handle_client_request(msg)
        elif isinstance(msg, PrepareResponseMsg):
            self.handle_prepare_response(msg)
        elif isinstance(msg, AcceptResponseMsg):
            self.handle_accept_response(msg)
        elif isinstance(msg, RejectionMsg):
            #If the proposal is not accepted the first time, add it to the list of rejected values to try again after consensus is reached
            if msg.proposal.number not in self.rejections_seen:
                print("Message is rejected, will try again (message = {})".format(msg))
                self.rejections_seen.add(msg.proposal.number)
 
        ######### ACCEPTOR MESSAGES ###########
        elif isinstance(msg, PrepareMsg):
            self.handle_prepare(msg)
        elif isinstance(msg, AcceptMsg):
            self.handle_accept(msg)
            
    #Method for quitting the node
    def handle_quit(self):
        self.active = False


    ######### ALL METHODS FOR PROPOSER ###########

    #Method for handling the client request
    def handle_client_request(self, msg, instance=None):

        #Check whether the lock/unlock request is valid
        CLIENT_PROP = msg.value
        if (CLIENT_PROP["type"] == "lock" and self.lock_status_list[CLIENT_PROP["lock_id"]]==None)\
        or(CLIENT_PROP["type"] == "unlock" and self.lock_status_list[CLIENT_PROP["lock_id"]] == CLIENT_PROP["client_id"]):
            #Create a new proposal for the new client request for the current instance
            proposal = self.create_proposal(instance)
            if proposal.instance not in self.proposer_instances:
                self.proposer_instances[proposal.instance] = {}
            if proposal.number not in self.proposer_instances[proposal.instance]:
                self.proposer_instances[proposal.instance][proposal.number] = PaxosProposer(self, proposal)
            #Call paxos algorithm for proposer to handle the request
            self.proposer_instances[proposal.instance][proposal.number].request = msg.value
            self.proposer_instances[proposal.instance][proposal.number].handle_client_request(proposal)
            #Add this new proposal to the proposal dictionary
            self.valid_client_proposals[proposal.number] = msg.value
        else:
            #Reject an invalid lock/unlock request
            print("Message is an invalid lock/unlock operation (message = {})".format(msg))
            self.system.log_failure(self.pid, msg.value, self.lock_status_list) #log the failed proposal
            return
      
    #Method for handling the prepare rquest
    def handle_prepare_response(self, msg):
        #Call paxos algorithm for proposer to handle the request
        self.proposer_instances[msg.proposal.instance][msg.proposal.number].handle_prepare_response(msg)
        
    #Method for creating a new proposal
    def create_proposal(self, instance=None):

        #Set the instance for the proposal
        if instance:
            instance_sequence = instance
        else:
            instance_sequence = self.instance_sequence

        #Create the new proposal
        print("Node {} creating proposal (number {} and instance {})".format(self.pid, self.sequence, instance_sequence))
        proposal = Proposal(self.sequence, instance_sequence, self.pid)
        
        #Now increment the number for the next proposal by the number of nodes
        self.sequence += self.sequence_step
        
        return proposal
        
    ######### ALL METHODS FOR ACCEPTOR ###########

    #Method for handling prepare message
    def handle_prepare(self, msg):
        #Call paxos algorithm for acceptor to handle prepate message
        self.create_instance(msg.proposal.instance).handle_prepare(msg)

    #Method for handling accept message
    def handle_accept(self, msg):
        #Call paxos algorithm for acceptor to handle accept message
        self.create_instance(msg.proposal.instance).handle_accept(msg)
        
    #Method for creating Paxos acceptor instance   
    def create_instance(self, instance_id):
        if instance_id not in self.acceptor_instances:
            self.acceptor_instances[instance_id] = PaxosAcceptor(self)
        return self.acceptor_instances[instance_id]
        
        
    ######### ALL METHODS FOR LEARNER ###########       
         
    #Method for handling accept response        
    def handle_accept_response(self, msg):
    
        number = msg.proposal.number
        instance_id = msg.proposal.instance
        if instance_id not in self.learner_instances:
            self.learner_instances[instance_id] = {}
        if number not in self.learner_instances[instance_id]:
            self.learner_instances[instance_id][number] = PaxosLearner(self)
        #Call paxos algorithm for learner to handle message
        self.learner_instances[instance_id][number].handle_accept_response(msg)

        #Increament the instance number by one
        self.instance_sequence = msg.proposal.instance + 1
        
    #Method for logging the consensus value
    def log_result(self, msg):

        #Add the value to set of results and locg the value
        instance = msg.proposal.instance
        value = msg.proposal.value
        self.results[instance] = value
        print("------------Node {} final log for instance  = {} and value = {}".format(self.pid, instance, value))
        
        self.logged_values.append("Node {} result for instance {} = {}".format(self.pid, instance, value))
        self.system.log_result(self.pid, instance, value, self.lock_status_list)

        #Update the status of locks based on the type of operations
        if value["type"] == "lock":
            self.lock_status_list[value["lock_id"]] = value["client_id"]
        else:
            self.lock_status_list[value["lock_id"]] = None

        print("------------FINISHED AND CONSENSUS REACHED")

        #Print the list of lock statuses and logs
        to_print_status = "-".join([str(x) for x in self.lock_status_list])
        self.states_list_print.append(to_print_status)
        print("------------", list(self.logged_values))
        print("------------", self.states_list_print)

        #If there are any rejected proposals, send them again with the next instance
        while self.rejections_seen:
            rej_proposal_number = self.rejections_seen.pop()
            print("Retrying rejected response = {}".format((rej_proposal_number)))
            self.handle_client_request(ClientRequestMsg(None, self.valid_client_proposals[rej_proposal_number]))
                       
        