import sys
import time
from threading import Thread
from multiprocessing import Queue
from queue import Empty

from messages import *
from protocol import *
# from analyzer import *


class BaseSystem:
    """
    Base class that simulation system classes should inherit.
    Included here so that this module can check for a system instance's type
    without creating circular import by importing the actual implementation
    class that lives someplace else.
    """

    
class Agent:
    """
    A Paxos agent, meant to be subclassed for implementing the paxos roles.
    """

    def __init__(self, pid, mailbox, logger):
        self.config = None
        self.pid = pid
        self.mailbox = mailbox
        self.logger = logger
        # Flag that will shutdown process.
        self.active = True
        # Flag for any process threads to shutdown.
        self.stopping = False

        ######## LOCK FUNCTIONALITY STUFF HERE #########
        self.logged_values = []
        self.states_list_print =[]
        num_locks = 5
        self.lock_status_list = [None]*5


        ######## PROPOSER #########
        # Paxos Made Simple suggests that proposers in a system use a disjoint
        # set of proposal numbers.  So, we start each Proposer's sequence
        # number at its pid value (which is unique) and we increment by the
        # number of proposers.
        self.sequence = self.pid
        # Step gets set when the process receives the system configuration
        # message on startup.
        self.sequence_step = None

        # States for various instances of the algorithm, i.e. sequence numbers.
        # This will itself contain dictionaries of states for the rounds of
        # each proposal tried during an instance.
        self.proposer_instances = {}
        self.instance_sequence = 1
        
        ########## ACCEPTOR ###########
        self.acceptor_instances = {}
        
        ######## LEARNER ###########
        # Results stored by instance number.
        self.results = {}
        self.learner_instances = {}
        
        self.rejections_seen = set()
        self.rejected_proposals = []
        self.valid_client_proposals = {}
        
        
        
        
        
    def run(self):
        """
        Loop forever, listening for and handling any messages sent to us.
        """
        print("{}-{} started".format(self.pid, self.__class__.__name__))
        while self.active:
            msg = self.recv()
            self.handle_message(msg)
            #self.message_done()
        print("Process {} shutting down".format(self.pid))

    def send_message(self, msg, pids):
        for pid in pids:
            print("Process {}-{} sending message to {}: {}".format(
                  self.pid, self.__class__.__name__, pid, msg))
            self.mailbox.send(pid, msg)

    def recv(self):
        """
        Blocking receive of a message destined to this agent process.
        """
        msg = self.mailbox.recv(self.pid)
        source = getattr(msg, 'source', None)
        print("  Process {}-{} received message from {}: {}".format(
              self.pid, self.__class__.__name__, source, msg))
        return msg

    def message_done(self):
        """
        Signal to the mailbox that we've finished processing of the message.
        This gives the mailbox to take care of any needed accounting.
        """
        self.mailbox.task_done(self.pid)

    def handle_message(self, msg):
        """
        Handle a received message.  Meant to be overridden in subclasses for
        customizing agent's behavior.
        """
        if isinstance(msg, SystemConfig):
            self.set_config(msg)
        if msg == 'quit':
            self.handle_quit()
            
        ######### PROPOSER ###########
        if isinstance(msg, ClientRequestMsg):
            self.handle_client_request(msg)
        elif isinstance(msg, PrepareResponseMsg):
            self.handle_prepare_response(msg)
        elif isinstance(msg, AcceptResponseMsg):
            self.handle_accept_response(msg)
        elif isinstance(msg, RejectionMsg):
            # if msg.proposal not in self.pending_proposals:
            #self.pending_proposals.append(msg.proposal)
            
            if msg.proposal.number not in self.rejections_seen:
                print("REJECTED - RESTARTING", msg)
                self.rejections_seen.add(msg.proposal.number)
                self.rejected_proposals.append(msg.proposal)

            
        ####### ACCEPTOR ###########
        elif isinstance(msg, PrepareMsg):
            self.handle_prepare(msg)
        elif isinstance(msg, AcceptMsg):
            self.handle_accept(msg)
            
        ######### LEARNER #############
        # elif isinstance(msg, AcceptResponseMsg):
            # self.handle_accept_response(msg)


    def set_config(self, config):
        self.config = config

        """
        Set this process's sequence step to the number of proposers.
        """
        if config.proposer_sequence_start:
            self.sequence = config.proposer_sequence_start
        if config.proposer_sequence_step:
            self.sequence_step = config.proposer_sequence_step
        else:
            self.sequence_step = len(config.agent_ids)
        #print("********* SEQUENCE STEP:", self.sequence_step)

            
            
            

    def stop(self):
        """Stop any helper threads."""
        self.stopping = True

    def handle_quit(self):
        self.stop()
        self.mailbox.shutdown()
        self.active = False


    ############ PROPOSER #################
    def handle_client_request(self, msg, instance=None):
        """
        Start a Paxos instance.
        """
        #msg.proposal = {"type": "lock", "lock_id": 0, "client_id": 1}
   
        print("PRINTING MESSAGE:", msg.value)
        CLIENT_PROP = msg.value
        if (CLIENT_PROP["type"] == "lock" and self.lock_status_list[CLIENT_PROP["lock_id"]]==None)\
        or(CLIENT_PROP["type"] == "unlock" and self.lock_status_list[CLIENT_PROP["lock_id"]] == CLIENT_PROP["client_id"]):
            proposal = self.create_proposal(instance)
            if proposal.instance not in self.proposer_instances:
                self.proposer_instances[proposal.instance] = {}
            if proposal.number not in self.proposer_instances[proposal.instance]:
                self.proposer_instances[proposal.instance][proposal.number] = \
                                    BasicPaxosProposerProtocol(self, proposal)
            self.proposer_instances[proposal.instance][proposal.number].request = msg.value
            self.proposer_instances[proposal.instance][proposal.number].handle_client_request(proposal)
            
            self.valid_client_proposals[proposal.number] = msg.value
        else:
            print("rejected!!!!!!!!!!!")
            self.logger.log_failure(self.pid, msg.value, self.lock_status_list)
            return
      
      
                
        

    def handle_prepare_response(self, msg):
        self.proposer_instances[msg.proposal.instance][msg.proposal.number].handle_prepare_response(msg)
        



    def create_proposal(self, instance=None):


        """
        Create a new proposal using this process's current proposal number
        sequence and instance number sequence.  If instance is given, then
        use it as the instance number instead of using this process's current
        instance sequence number.
        """
        if instance:
            instance_sequence = instance
        else:
            instance_sequence = self.instance_sequence
        print("*** Process {} creating proposal with Number {}, Instance {}"
              .format(self.pid, self.sequence, instance_sequence))
        proposal = Proposal(self.sequence, instance_sequence, self.pid)
        self.sequence += self.sequence_step
        
        # Only increment the instance sequence if we weren't given one.
        # if instance is None:
            # self.instance_sequence += 1


        return proposal
        

        
    #############  ACCEPTOR ###################
    def handle_prepare(self, msg):
        self.create_instance(msg.proposal.instance).handle_prepare(msg)

    def handle_accept(self, msg):
        self.create_instance(msg.proposal.instance).handle_accept(msg)
        
        
    def create_instance(self, instance_id):

        
        """
        Create a protocol instances for the given instance_id if one doesn't
        already exist.  Return the protocol instance.
        """

       
        if instance_id not in self.acceptor_instances:
            self.acceptor_instances[instance_id] = BasicPaxosAcceptorProtocol(self)
        return self.acceptor_instances[instance_id]
        
        
    ############# LEARNER  #######################            
            
    def handle_accept_response(self, msg):
    

        number = msg.proposal.number
        instance_id = msg.proposal.instance
        if instance_id not in self.learner_instances:
            self.learner_instances[instance_id] = {}
        if number not in self.learner_instances[instance_id]:
            self.learner_instances[instance_id][number] = BasicPaxosLearnerProtocol(self)
        self.learner_instances[instance_id][number].handle_accept_response(msg)

        self.instance_sequence = msg.proposal.instance + 1
        
        
                       
                       
    def record_result(self, instance, value):
        self.results[instance] = value

    def log_result(self, msg):
        instance = msg.proposal.instance
        value = msg.proposal.value
        self.record_result(instance, value)
        print("*** {} logging result for instance {}: {}".format(self.pid, instance, value))
        self.logged_values.append("*** {} logging result for instance {}: {}".format(self.pid, instance, value))

        
        if value["type"] == "lock":
            self.lock_status_list[value["lock_id"]] = value["client_id"]
        else:
            self.lock_status_list[value["lock_id"]] = None
            
        self.logger.log_result(self.pid, instance, value, self.lock_status_list)


        print("FINISHED AND CONSENSUS REACHED")
        print("CURRENT STATE:", self.lock_status_list)


        to_print_status = "-".join([str(x) for x in self.lock_status_list])
        self.states_list_print.append(to_print_status)
        
        print(list(self.logged_values))
        print(self.states_list_print)

        while self.rejections_seen:
            print("HANDLING REJECT RESPONSES", self.pid)#[str(list(x.value)) for x in self.pending_proposals])
            rej_proposal_number = self.rejections_seen.pop()
            # self.proposer_instances[msg.proposal.instance][msg.proposal.number].handle_accept_response(msg)
            self.handle_client_request(ClientRequestMsg(None, self.valid_client_proposals[rej_proposal_number]))
                       
        
        

        
        
        
        
        
class SystemConfig:
    """
    Encapsulates the configuration of a system, i.e. the processes IDs of all
    the proposer, acceptor, and learner processes.
    """
    def __init__(self, num_agents,
                 agent_class=Agent,
                 proposer_sequence_start=None,
                 proposer_sequence_step=None,
                 message_timeout= 3,
                 num_test_requests=0,
                 weights=None,
                 dynamic_weights=False,
                 debug_messages=False,
                 ):
        #self.agent_config = (num_processes)
        self.num_agents = num_agents
        self.agent_class = agent_class

        self.agent_ids = list(range(0, num_agents))
        #self.acceptor_ids = list(range(0, num_processes))
        #self.learner_ids = list(range(0, num_processes))

        
        self.proposer_sequence_start = proposer_sequence_start
        self.proposer_sequence_step = proposer_sequence_step
        self.message_timeout = message_timeout
        self.num_test_requests = num_test_requests

        # # configure weights based on static/dynamic setting
        # if not dynamic_weights:
            # self.config_static_weights(weights, num_acceptors)
        # else:
            # self.config_dynamic_weights(num_acceptors)
        # self.dynamic_weights = dynamic_weights

        # The following used by DebugMailbox.
        # If True, each process will record who they sent messages to.
        self.debug_messages = debug_messages

    def __str__(self):
        return "System Configuration: {}".format(self.agent_ids)

    def process_list(self):
        """
        Return a list of (pid, agent class) two-tuples that get used by
        System.launch_processes.
        """
        pid = 0
        for pid in self.agent_ids:
            yield (pid, self.agent_class)
        #    yield (pid, self.acceptor_class)
        #    yield (pid, self.learner_class)

    # def config_static_weights(self, weights, num_acceptors):
        # if weights:
            # assert len(weights) == num_acceptors
        # else:
            # weights = [1] * num_acceptors
        # # Convert weights list into dict mapping pid to weight.
        # self.weights = {}
        # for pid, weight in zip(self.acceptor_ids, weights):
            # self.weights[pid] = weight
        # self.total_weight = sum(weights)

    # def config_dynamic_weights(self, num_acceptors):
        # weight = round(1/num_acceptors,2)
        # self.weights = {}
        # for pid in self.acceptor_ids:
            # self.weights[pid] = weight
        # self.total_weight = 1.0