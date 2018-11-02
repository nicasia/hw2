
##############################
### TO DOs:

### server -- client should be able to pass "lock(x)" and "unlock(x)" and the server should 
        # - create system instance
        # - lock/unlock --> parsed message 
### add threads for concurrency 
### kill nodes
### remove artifacts --- message printing, mailbox, logger, etc
### collapse some files & protocols
### replace sim (integrate mailbox and logger into system) and test with lockserver
### writeup







from collections import namedtuple
from multiprocessing import Queue
import random
import time

from __init__ import SystemConfig
from messages import *
from sim import System, Mailbox

class Lock:
    def __init__(self, owner, lock_status, lock_number):
        self.owner = owner
        self.lock_status = lock_status
        self.lock_number = lock_number
    def __str__(self):
        return "Lock status: {} and owner: {}".format(self.lock_status, self.owner)



def test_paxos(sytem):
    for x in range(2):
        system.mailbox.send(random.randint(0, len(system.config.proposer_ids)-1),
                            ClientRequestMsg(None, "Query {}".format(x)))
        #time.sleep(0.5)

def test_paxos2():
    time.sleep(2)
    system.mailbox.send(0,ClientRequestMsg(None, "Query {}".format(0)))
    system.mailbox.send(1,ClientRequestMsg(None, "Query {}".format(1)))
    time.sleep(0.5)
    system.mailbox.send(0,ClientRequestMsg(None, "Query {}".format(2)))
    time.sleep(0.5)
    system.mailbox.send(2,ClientRequestMsg(None, "Query {}".format(3)))
    time.sleep(1)
    system.mailbox.send(1,ClientRequestMsg(None, "Query {}".format(4)))
    system.mailbox.send(1,ClientRequestMsg(None, "Query {}".format(5)))
    system.mailbox.send(1,ClientRequestMsg(None, "Query {}".format(6)))

    time.sleep(0.5)
    system.mailbox.send(1,ClientRequestMsg(None, "Query {}".format(7)))

    time.sleep(0.5)
    system.mailbox.send(2,ClientRequestMsg(None, "Query {}".format(8)))

def test_paxos3():
    time.sleep(2)

    lock1 = Lock("User 1", 1, 1)
    system.mailbox.send(0, ClientRequestMsg(None, lock1))

    lock1 = Lock("User 2", 1, 1)
    system.mailbox.send(0, ClientRequestMsg(None, lock1))
    time.sleep(2)
    lock1 = Lock("User 3", 1, 1)
    system.mailbox.send(0, ClientRequestMsg(None, lock1))
    lock1 = Lock("User 4", 0, 1)
    system.mailbox.send(1, ClientRequestMsg(None, lock1))
    time.sleep(2)
    lock1 = Lock("User 5", 1, 1)
    system.mailbox.send(3, ClientRequestMsg(None, lock1))
    time.sleep(2)
    lock1 = Lock("User 6", 0, 1)
    system.mailbox.send(3, ClientRequestMsg(None, lock1))

    time.sleep(2)
    lock1 = Lock("User 8", 0, 1)
    system.mailbox.send(1, ClientRequestMsg(None, lock1))
    lock1 = Lock("User 9", 0, 1)
    system.mailbox.send(2, ClientRequestMsg(None, lock1))
    lock1 = Lock("User 10", 0, 1)
    system.mailbox.send(3, ClientRequestMsg(None, lock1))
    lock1 = Lock("User 11", 0, 1)
    system.mailbox.send(4, ClientRequestMsg(None, lock1))

def test_server():
    time.sleep(2)
    
    message = {"type": "lock", "lock_id": 1, "client_id": 1}
    message2 = {"type": "lock", "lock_id": 2, "client_id": 2}
  #  message3 = {"type": "lock", "lock_id": 3, "client_id": 3}

    system.mailbox.send(0,ClientRequestMsg(None, message))
    system.mailbox.send(0,ClientRequestMsg(None, message2))
   # system.mailbox.send(2,ClientRequestMsg(None, message3))



def test_multi_paxos():
    config = SystemConfig(3, 3, 3)
    system = DebugSystem(config)
    system.start()

    for x in range(20):
        # Always send to the same proposer, effectively using that proposer as
        # the leader.
        to = 0
        system.mailbox.send(to, ClientRequestMsg(None, "Query {}".format(x)))
        time.sleep(random.random()/10)

    system.shutdown_agents()
    system.logger.print_results()
    #print(system.print_sent_messages())
    system.quit()
    import sys
    sys.exit()

if __name__ == "__main__":
	# test_multi_paxos()
	system = System(SystemConfig(3))
	#system = DebugSystem(SystemConfig(2, 3, 2, proposer_sequence_start=1,
	#                             proposer_sequence_step=1))
	#system = DebugSystem(SystemConfig(1, 3, 1))
	system.start()
	#test_paxos(system)
	test_server()
	system.shutdown_agents()
	system.logger.print_results()
	# print(system.print_sent_messages())
	system.quit()