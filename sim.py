from collections import defaultdict
from multiprocessing import Process, Queue, JoinableQueue
import queue
from threading import Thread
import time

from __init__ import Agent, BaseSystem


class Mailbox:
    """
    Provides messaging functionality for a paxos system instance.
    """

    def __init__(self, config):
        self.config = config
        self.funnel = Queue()
        self.inbox = [Queue() for i in range(config.num_agents)]
        self.message_count = 0

        # Two flags, active to signal when we haven't received any messages
        # for timeout_interval seconds, and terminate to signal when we have
        # been asked to shutdown by the System.
        self.active = True
        self.terminate = False
        # If don't receive any messages in this amount of time, then shutdown.
        self.timeout_interval = 3 * self.config.message_timeout
        # Time stamp of the last seen message, used together with
        # timeout_interval to determine when the mailbox should shutdown.
        self.last_seen = None

    def run(self):
        print("Mailbox started")
        while True:
            if not self.active and self.terminate:
                break
            if self.active and self.last_seen and (time.time() - self.last_seen) > self.timeout_interval:
                self.active = False
            # Take messages off the funnel and deliver them to the appropriate
            # process.
            try:
                dest, msg = self.funnel.get(timeout=0.5)
                self.last_seen = time.time()
            except queue.Empty:
                pass
            else:
                self.inbox[dest].put(msg)
        print("Mailbox shutting down")

    def send(self, to, msg):
        """
        Send msg to process id ``to``.
        """
        # Funnel all messages through a primary queue so that we can keep track
        # of when we are done (i.e. all messages are processed).
        self.message_count += 1
        self.funnel.put((to, msg))

    def recv(self, from_):
        """
        Receive (blocking) msg destined for process id ``from_``.
        """
        return self.inbox[from_].get()

    def task_done(self, pid):
        """
        Inform pid's queue that it has processed a task.
        Called by agents when they are done processing a message, and used by
        the queue instance to know when all messages have been processed.
        """
        self.funnel.task_done()

    def join(self):
        """
        Block until all messages have finished processing and we haven't had
        any messages for a while (i.e. active set to False).
        """
        while self.active:
            time.sleep(0.5)
        # Don't join funnel queue because there's a good chance that it will
        # never be fully exhausted due to heart beat messages.
        #self.funnel.join()

    def shutdown(self):
        """
        Perform any shutdown actions prior to quitting.  In this base Mailbox
        class this is just a hook that isn't used.
        """
        pass

    def quit(self):
        self.terminate = True


class ResultLogger:
    """
    Class to hold log of results from each learner process.
    """

    def __init__(self, config):
        self.config = config
        self.queue = Queue()
        self.active = True
        # Results, PID mapped to list of decided values.
        self.results = defaultdict(dict)
        self.accepted_results_q = Queue()
        self.failed_results_q = Queue()

    def run(self):
        print("Logger started")
        while True:
            if not self.active and self.queue.empty():
                break
            try:
                source, instance, value = self.queue.get(timeout=0.5)
            except queue.Empty:
                pass
            else:
                if source == "quit":
                    self.active = False
                else:
                    self.results[source][instance] = value
        print("Logger shutting down")

    def log_result(self, source, instance, value, status):
        self.queue.put((source, instance, value))
        self.accepted_results_q.put((source, instance, value, status))
        print("~~~~~~~~WINNER!!!!", source, instance, value, status)

    def log_failure(self, source, value, status):
        self.failed_results_q.put((source, value, status))
        print("~~~~~~~~LOSER!!!!", source, value, status)

        
    def print_results(self):
        print("Process Result Log:")
        processes = sorted(self.results.keys())
        for pid in processes:
            instances = range(1, self.config.num_test_requests + 1)
            results = [(instance, self.results[pid].get(instance))
                       for instance in instances]
            print("  {}: {}".format(pid, results))

    def check_results(self):
        """
        Check that each learner process got the same results in the same order.
        Return False if there is a result list that is not the same.  If all
        results are the same, then return the list of results.
        """
        results = list(self.results.values())
        compare_list = results[0]
        result = True
        for result_list in results[1:]:
            if result_list != compare_list:
                result =  False
        print("Logger results consistent:", result)
        return result

    def get_summary_data(self):
        return ResultSummary(self)

    def print_summary(self):
        summary = self.get_summary_data()
        summary.print_summary()



class System(BaseSystem):
    """
    Class for simulating a network of paxos agents.
    """

    def __init__(self, config, mailbox=None):
        """
        ``mailbox`` should be a mailbox class; if None, then use default
        Mailbox class.
        """
        print("System starting...")
        self.config = config
        # Set up mailbox and logger before launching agent processes so that
        # the agent processes will have access to them.
        if mailbox:
            mailbox_class = mailbox
        else:
            mailbox_class = Mailbox
        self.mailbox = mailbox_class(config)
        self.mailbox_process = Thread(target=self.mailbox.run, name="System Mailbox")
        self.mailbox_process.start()
        # Start the logger thread.
        self.logger = ResultLogger(config)
        self.logger_process = Thread(target=self.logger.run, name="System Logger")
        self.logger_process.start()

        self.processes = self.launch_processes()

    def launch_processes(self):
        """
        Launch ``number`` number of processes of the given ``agent_class``,
        using the given Mailbox instance and with process id starting with the
        given ``pid`` and incrementing for each process spawned.
        Return the incremented pid value when done, which is meant to be used
        in subsequent calls to this method for a starting pid.
        """
        processes = []
        for pid, agent_class in self.config.process_list():
            agent = agent_class(pid, self.mailbox, self.logger)
            p = Process(target=agent.run)
            p.start()
            processes.append(p)
        return processes

    def join(self):
        """
        Join with all processes that have been launched.
        """
        for process in self.processes:
            process.join()

    def start(self):
        """
        Start the system by sending a message to each process containing
        this system object.
        """
        for x in range(len(self.processes)):
            self.mailbox.send(x, self.config)

    def shutdown_agents(self):
        """
        Wait for all mailbox messages to be processed, then send quit messages
        to all processes and join with all processes.  This will block until
        all agents have terminated.
        """
        print("System waiting for mailbox to go inactive...")
        # Sleep a bit to allow any actions based on timeouts to fire.
        #time.sleep(10)
        self.mailbox.join()
        print("System shutting down agents...")
        for x in range(len(self.processes)):
            self.mailbox.send(x, "quit")
        self.join()

    def quit(self):
        self.logger.log_result("quit", None, None, None)
        self.logger_process.join()
        self.mailbox.quit()
        self.mailbox_process.join()
        print("System terminated.")