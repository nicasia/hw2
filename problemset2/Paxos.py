#Paxos classes for executing 3 roles for Paxos: Proposer, Acceptor, Learner

from collections import defaultdict
from Messages import *

#Basic class for Paxos protocol
class Paxos:

    def __init__(self, node):
        self.node = node #assigns an node to a protocal
        self.config = self.node.system

    #Method for determining whether the majority of acceptors have accepted the message
    def have_acceptor_majority(self, acceptors):
        self.config = self.node.system
        majority_number = len(self.config.node_ids) / float(2)
        current_number = len(acceptors)
        return current_number > majority_number
    
#Basic class for Paxos PROPOSER protocol
class PaxosProposer(Paxos):

	def __init__(self, node, proposal):
		super(PaxosProposer, self).__init__(node)
		self.request = None #the request received from the client
		self.proposal = proposal #the current proposal for the current instance

		self.prepare_responders = set() #set of acceptors that returned a response to prepare request
		self.highest_proposal_from_promises = Proposal(-1, None) #the proposal with the highest number ever seen
		self.accept_responders = set() #set of acceptors that returned a response to accept request

		#List of states for status of PROPOSER
		self.state = None
		self.PREPARE_SENT = 0
		self.ACCEPT_SENT = 1

	#Method for handling a client request
	def handle_client_request(self, proposal):
		next_msg = PrepareMsg(proposal.pid, proposal) #create a prepare message
		self.node.send_message(next_msg, self.config.node_ids) #send the prepare messafe to all nodes
		self.state = self.PREPARE_SENT #now the state is updated

	#Method for handling prepare responses received from ACCEPTORs
	def handle_prepare_response(self, msg):
		#When we receive a prepare response from an acceptor, we append it t list of prepare responders
		self.prepare_responders.add(msg.source)
		
		#If the proposal number of this message is higher than our own, we update the highest proposal number
		if msg.highest_proposal.number > self.highest_proposal_from_promises.number:
			self.highest_proposal_from_promises = msg.highest_proposal
		
		#If the prepare messages are sent, check whether the majority is reached
		if self.state == self.PREPARE_SENT:
			if self.have_acceptor_majority(self.prepare_responders):

				#Set the highest proposal from either the user request or value received from other acceptors
				if self.highest_proposal_from_promises.value is not None:
					self.proposal.value = self.highest_proposal_from_promises.value
					self.client_request_handled = False
				else:
					self.proposal.value = self.request
					self.client_request_handled = True

				next_msg = AcceptMsg(self.node.pid, self.proposal) #create an accept message 
				self.node.send_message(next_msg, self.config.node_ids) #send accept message to all nodes
				self.state = self.ACCEPT_SENT #update the PROPOSER state

	#Method for handling accept responses from LEARNERs
	def handle_accept_response(self, msg):
		self.accept_responders.add(msg.source)


#Basic class for Paxos ACCEPTOR protocol
class PaxosAcceptor(Paxos):

    def __init__(self, node):
        super(PaxosAcceptor, self).__init__(node)
        self.highest_proposal_promised = Proposal(-1, None) #highest proposal we promised to
        self.highest_proposal_accepted = Proposal(-1, None) #highest proposal we accepted to

    #Method for sending prepare responses to all ACCEPTORs
    def handle_prepare(self, msg):

    	#if the current proposal is higher than what we have promised to before, we can send prepare response messages
        if msg.proposal.number > self.highest_proposal_promised.number:
            self.highest_proposal_promised = msg.proposal #update the highest proposal promised
            next_msg = PrepareResponseMsg(self.node.pid, msg.proposal, self.highest_proposal_accepted) #create prepare response message
            self.node.send_message(next_msg, [msg.source]) #send the message to all ACCEPTORs

        #if the current proposal is NOT higher than what we have promised to before, try sending the same proposal in the next instance
        else:
            next_msg = RejectionMsg(self.node.pid, msg.proposal) 
            self.node.send_message(next_msg,[msg.source])			  

    #Method for sending accept responses to all ACCEPTORs
    def handle_accept(self, msg):
        #if the current proposal is higher than what we have promised to before, we can send accept response messages
        if msg.proposal.number >= self.highest_proposal_promised.number:
            self.highest_proposal_accepted = msg.proposal #update the highest proposal accepted
            next_msg = AcceptResponseMsg(self.node.pid, msg.proposal) #create accept response messsage
            self.node.send_message(next_msg,[msg.source] + list(self.config.node_ids)) #send it to all nodes
        #if the current proposal is NOT higher than what we have promised to before, try sending the same proposal in the next instance
        else:
            next_msg = RejectionMsg(self.node.pid, msg.proposal)
            self.node.send_message(next_msg,[msg.source])			  

#Basic class for Paxos LEARNER protocol							  							  
class PaxosLearner(Paxos):

    def __init__(self, node):
        super(PaxosLearner, self).__init__(node)
        self.accept_responders = defaultdict(set) #set of acceptors that responded to learner
        self.state = None
        self.RESULT_SENT = 1

    #Method for sending accept responses to all ACCEPTORs
    def handle_accept_response(self, msg):
    	#record the accept responders
        newkey =  msg.proposal.value["type"] + "-" +  str(msg.proposal.value["lock_id"])  + "-" +  str(msg.proposal.value["client_id"])
        self.accept_responders[newkey].add(msg.source)
        if self.state == self.RESULT_SENT:
            return
        #When we receive accept messages from the majority of ACCEPTORs, we can log the result
        if self.have_acceptor_majority(self.accept_responders[newkey]):
            self.node.log_result(msg)
            self.state = self.RESULT_SENT
