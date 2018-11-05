import socket
import time

SERVER = "127.0.0.1"
PORT = 8080

#Class for a manually managed client
class Client:

    def __init__(self, name):
        self.name = name
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.client.connect((SERVER, PORT))
        in_data =  self.client.recv(1024)
        print("Client {} received message {} from server".format(self.name, in_data))
     
    #Method for sending a new client request
    def send_request(self, message):
        self.client.sendall(bytes(message,'UTF-8'))
        in_data =  self.client.recv(1024)
        print("Client {} received message {} from server".format(self.name, in_data) )

#####################################
#Define a list of demos for showing various properties of the system

def demo1():

    print("DEMO for showing lock and unlock for only one client")
    client1 = Client('Alice')

    print(client1.name + " is trying to lock 0")
    client1.send_request('lock 0')
    print("Waiting for 1 second")
    time.sleep(1)
    
    print(client1.name + " is trying to lock 1")
    client1.send_request('lock 1')
    print(client1.name + " is trying to lock 4")
    client1.send_request('lock 4')
    print("Waiting for 1 second")
    time.sleep(1)

    print(client1.name + " is trying to unlock 0")
    client1.send_request('unlock 0')
    print(client1.name + " is trying to unlock 1")
    client1.send_request('unlock 1')
    print(client1.name + " is trying to unlock 4")
    client1.send_request('unlock 4')


def demo2():

    print("DEMO for showing lock and unlock for multiple clients")
    client1 = Client('Alice')
    client2 = Client('Bob')
    client3 = Client('Charlie')

    print(client1.name + " is trying to lock 2")
    print(client2.name + " is trying to lock 2")
    print(client3.name + " is trying to lock 2")
    client1.send_request('lock 2')
    client2.send_request('lock 2')
    client3.send_request('lock 2')
    print("Waiting for 1 second")
    time.sleep(1)
    
    print(client1.name + " is trying to lock 2")
    print(client2.name + " is trying to lock 2")
    print(client3.name + " is trying to lock 2")
    client1.send_request('lock 2')
    client2.send_request('lock 2')
    client3.send_request('lock 2')
    print("Waiting for 1 second")
    time.sleep(1)

    print(client1.name + " is trying to lock 0")
    print(client2.name + " is trying to lock 1")
    print(client3.name + " is trying to lock 2")
    client1.send_request('lock 0')
    client2.send_request('lock 1')
    client3.send_request('lock 2')
    print("Waiting for 1 second")
    time.sleep(1)

    print(client1.name + " is trying to unlock 2")
    print(client2.name + " is trying to unlock 2")
    print(client3.name + " is trying to unlock 2")
    client1.send_request('unlock 2')
    client2.send_request('unlock 2')
    client3.send_request('unlock 2')
    print("Waiting for 1 second")
    time.sleep(1)

    print(client1.name + " is trying to unlock 0")
    print(client2.name + " is trying to unlock 1")
    client1.send_request('unlock 0')
    client2.send_request('unlock 1')


def demo3():
    client1 = Client('Alice')
    client2 = Client('Bob')
    client3 = Client('Charlie')
    client4 = Client('Daniel')
    client5 = Client('Ellen')

    print(client1.name + " is trying to lock 0")
    client1.send_request('lock 0')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client5.name + " is trying to lock 1")
    client5.send_request('lock 1')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client3.name + " is trying to lock 0")
    client3.send_request('lock 0')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client3.name + " is trying to lock 4")
    client3.send_request('lock 4')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client1.name + " is trying to unlock 0")
    client1.send_request('unlock 0')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client2.name + " is trying to lock 0")
    client2.send_request('lock 0')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client3.name + " is trying to unlock 3")
    client3.send_request('unlock 3')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client3.name + " is trying to lock 3")
    client3.send_request('lock 3')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client3.name + " is trying to unlock 3")
    client3.send_request('unlock 3')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client3.name + " is trying to unlock 4")
    client3.send_request('unlock 4')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client3.name + " is trying to unlock 1")
    client5.send_request('unlock 1')
    print("Waiting for 1 second")
    time.sleep(1)
    print(client3.name + " is trying to unlock 0")
    client2.send_request('unlock 0')



def demo4():
    client1 = Client('Alice')
    client2 = Client('Bob')
    client3 = Client('Charlie')
    client4 = Client('Daniel')
    client5 = Client('Ellen')

    print(client1.name + " is trying to lock 0")
    print(client2.name + " is trying to lock 0")
    print(client3.name + " is trying to lock 0")
    client1.send_request('lock 0')
    client2.send_request('lock 0')
    client3.send_request('lock 0')

    print("Waiting for 1 second")
    time.sleep(1)
    print(client1.name + " is trying to lock 1")
    print(client4.name + " is trying to lock 1")
    print(client5.name + " is trying to lock 1")
    client1.send_request('lock 1')
    client4.send_request('lock 1')
    client5.send_request('lock 1')

    print("Waiting for 1 second")
    time.sleep(1)
    print(client4.name + " is trying to lock 0")
    print(client5.name + " is trying to lock 0")
    client4.send_request('lock 0')
    client5.send_request('lock 0')

    print("Waiting for 1 second")
    time.sleep(1)
    print(client1.name + " is trying to unlock 0")
    print(client2.name + " is trying to unlock 0")
    print(client3.name + " is trying to unlock 0")
    client1.send_request('unlock 0')
    client2.send_request('unlock 0')
    client3.send_request('unlock 0')

    print("Waiting for 1 second")
    time.sleep(1)
    print(client5.name + " is trying to lock 0")
    client5.send_request('lock 0')

    print("Waiting for 1 second")
    time.sleep(1)
    print(client5.name + " is trying to unlock 0")
    print(client1.name + " is trying to unlock 1")
    print(client4.name + " is trying to unlock 1")
    print(client5.name + " is trying to unlock 1")
    client5.send_request('unlock 0')
    client1.send_request('unlock 1')
    client4.send_request('unlock 1')
    client5.send_request('unlock 1')




print("############ DEMO 1 ############")
demo1()
time.sleep(5)
print("############ DEMO 2 ############")
demo2()
time.sleep(5)
print("############ DEMO 3 ############")
demo3()
time.sleep(5)
print("############ DEMO 4 ############")
demo4()
time.sleep(5)


