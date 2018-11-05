##############################################
#   Nicasia Beebe-Wang 1723387 nbbwang@uw.edu
#   Ayse Berceste Dincer 1723315 abdincer@uw.edu
#
# Code for CSE 550 HW2
# 
# Code is modified from: https://github.com/gdub/python-paxos 
##############################################

#Create all classes for defining message classes

#Class for a paxos proposal
class Proposal:
    def __init__(self, number, instance, pid=None, value=None):
        self.number = number #proposal number
        self.instance = instance #instance number (iteration)
        self.pid = pid #id of node
        self.value = value #proposal value
    def __str__(self):
        return "PROPOSAL = [proposal number = {}, instance = {}, value = {}]".format(self.number, self.instance, self.value)

#Parent class for all messages in the system
class Message:
    def __init__(self, source):
        self.source = source #pid of the sender of the message

#Message class for sending the request of the client to the PROPOSER
class ClientRequestMsg(Message):
    def __init__(self, source, value):
        super(ClientRequestMsg, self).__init__(source)
        self.value = value #the value user is passing for consensus
    def __str__(self):
        return "CLIENT REQUEST = [value = {}]".format(self.value)

#Message class for sending the proposal across nodes
class ProposalMsg(Message):
    def __init__(self, source, proposal):
        super(ProposalMsg, self).__init__(source)
        self.proposal = proposal #keeps the proposal in the message

    def __str__(self):
        return "PROPOSAL MESSAGE = [proposal = {}]".format(self.name, self.proposal)

#Message class for sending the prepare message from PROPOSER to ACCEPTORs
class PrepareMsg(ProposalMsg):
    def __str__(self):
        return "PREPARE MESSAGE = [proposal = {}]".format(self.proposal)

#Message class for sending the prepare message from ACCEPTOR back to PROPOSER
class PrepareResponseMsg(ProposalMsg):
    def __init__(self, source, proposal, highest_proposal):
        super(PrepareResponseMsg, self).__init__(source, proposal)
        self.highest_proposal = highest_proposal #highest proposal number ever received
    def __str__(self):
        return "PREPARE RESPONSE MESSAGE = [proposal = {}, highest proposal number = {}]".format(self.proposal, self.highest_proposal)

#Message class for sending the accept message from PROPOSER to ACCEPTORs                  
class AcceptMsg(ProposalMsg):
   def __str__(self):
        return "ACCEPT MESSAGE = [proposal = {}]".format(self.proposal)

#Message class for sending the accept response message from ACCEPTORs to PROPOSER and LEARNERs
class AcceptResponseMsg(ProposalMsg):
    def __str__(self):
        return "ACCEPT MESSAGE = [proposal = {}]".format(self.proposal)

#Message class for sending the rejected proposal from acceptors to proposer
class RejectionMsg(ProposalMsg):
    def __str__(self):
        return "REJECT MESSAGE = [proposal = {}]".format(self.proposal)
