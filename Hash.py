import socket
import os
import sys
from _thread import *
from queue import Queue
import time
import threading
from threading import Event, Lock
import string
from random import *

ServerSocket = socket.socket()
host = '127.0.0.1'
port = 1233
ThreadCount = 0

#clients = []
#clients_lock = threading.Lock()

flag = 1
endMessage = False
emptyQueues = 0
event = Event()
mutex = Lock()
CAPACITY = 10
brokerList = []
threadList = []
wantedHash = 0

try:
    ServerSocket.bind((host, port))
except socket.error as e:
    print(str(e))
    
print('Waiting for a Connection..')
ServerSocket.listen(2)

class Publisher():
    def __init__(self, cname):
        self.name = cname
        self.cnt = 0
        
    def generate_string(self, RecvBroker):
        global wantedHash
        global endMessage
        #set the hash
        wantedHash = 8
        while self.cnt < 20:
            min_char = 8
            max_char = 12
            allchar = string.ascii_letters + string.digits
            data = "".join(choice(allchar) for x in range(randint(min_char, max_char)))
            RecvBroker.q.put(data)
            self.cnt = self.cnt + 1
            time.sleep(.1)
        endMessage = True

class Broker():
    #constructor for broker
    def __init__(self, cname):
        self.q = Queue()
        self.numberOfClients = 0
        self.name = cname
        self.visited = False
        self.neighbours = []   
        self.ThreadCount = 0
        self.messageCount = 0
        self.clientList = []
        self.clientNames = []
        
    def start_message_exchange(self):
        t = threading.Thread(target=self.ReadFromQueueSendToQueue, args=[self, self.neighbours[0]])
        t.start()
        time.sleep(.1)
        
    def hash(self, key):
        suma = 0
        for i, c in enumerate(key):
            suma += (i + len(key)) ** ord(c)
            suma = suma % CAPACITY
            #print("Hash je: {}".format(suma))
        print("Krajnji hash za {} je: {}".format(key, suma))
        return suma
        
    def CheckHash(self):
        global wantedHash
        i = 0
        for i in range(len(self.clientNames)):
            print("IME KLIJENTA {}".format(self.clientNames[i]))
            res1 = self.hash(self.clientNames[i])
            if res1 == wantedHash:
                print("I found the hash set by the publisher")
                return i
        return -1
        
    def threaded_client(self):
        global emptyQueues
        global endMessage
        print('Server {}'.format(self))
        print("Broker {} sending message".format(self))
        if self.q.empty() == True and endMessage == True:
            print("{} broker has empty queue".format(self))
            emptyQueues += 1
            return
        
        #print("Duzina liste {}".format(len(self.clientList)))
        print("Proveravam hash za klijente brokera {}".format(self))
        
        h = self.CheckHash()
        print("VREDNOST ZA H JE OVDE {}".format(h))
        
        if h != -1:
            print("POKLOPIO SE HASH...")
            while True:
                mutex.acquire()
                data = self.q.get()
                mutex.release()
                print("{} sending {}".format(self, data))
                self.clientList[h].sendall(str.encode(data))
                time.sleep(.1)
        else:
            self.start_message_exchange()
    
    def wait_for_conn(self):
        while self.ThreadCount < self.numberOfClients:
            Client, address = ServerSocket.accept()
            self.clientList += [Client]
            print('Connected to: ' + address[0] + ':' + str(address[1]))
            self.ThreadCount += 1
            print('Thread Number: ' + str(self.ThreadCount))
        
    def close_connection(self):
        for c in self.clientList:
            c.close()
        
    #method for moving messages that could not be sent from 
    #current to next broker           
    def ReadFromQueueSendToQueue(self, src, dst):
        while True:
            while self.q.empty() == False:
                #take message from queue and send it to next broker queue
                a = src.q.get()
                #print("PREPISUJEM {} iz {} u {}".format(a, src, dst))
                dst.q.put(a)
                #send.wait()
                
#graph       
class Graph:
    nodes = list()
    edges = list()

    #constructor
    def __init__(self):
        self.nodes = list()
        self.edges = list()

    #breadth first search algorithm
    def BFS(self, s):
        global flag
        print("BFS")
        #mark all the vertices as not visited
        for i in range(len(self.nodes)):
            self.nodes[i].visited = False
        
        #mark first node as visited - starting node
        s.visited = True

        #create a queue for BFS
        queue = list()

        #enqueue the first node
        queue.append(s)
        #do the message publishing for the first node
        if flag == 1:
            s.wait_for_conn()
            print("Sacekao sam konekcije")
            #for i in range(2):
            #    print("ISPISUJEM KLIJENTE U BROKERU: ".format(s.clientList[i]))
        elif flag == 2:
            #for c in s.clientList:
            #s.start_message_exchange()
            s.threaded_client()
        else:
            s.close_connection()
        #for c in clients:
        #    print("CHECK CLIENT {}".format(c))
        #    s.threaded_client(c)
        print("Poslao sam poruke")
        #s.messageCount = 0
        
        #go through the graph, until all the nodes are visited
        while len(queue) != 0:
            print("Usao u queue")
            #dequeue a vertex from queue and print it
            s = queue.pop()
            print("POP: {}".format(s))
                
            #get all the adjacent brokers of the dequeued broker s.
            #if adjacent broker has not been visited, mark it as visited
            #and enqueue it
            for broker in s.neighbours:
                print("Proveravam komsijske cvorove")
                if broker.visited == False:
                    print("Stavaljam u red {}".format(broker))
                    queue.append(broker)
                    #publish messages for current broker
                    if flag == 1:
                        print()
                        print("Waiting for conn")
                        broker.wait_for_conn()
                    elif flag == 2:
                        print()
                        #broker.start_message_exchange()
                        print("Sending messages")
                        #for c in broker.clientList:
                        broker.threaded_client()
                    else:
                        broker.close_connection()
                    broker.visited = True
                
#testing graph - 3 brokers added
def Get_Predefined_Dag():
    clientCount = 0
    nodes = int(input("Enter total number of nodes in graph: "))
    #nodes = 3
    adjacency = list()

    G = Graph()
    
    #make broker instances
    objs = []
    for i in range(nodes):
        print("GARI PRAVIM BROKERE")
        objs.append(Broker("Broker" + str(i)))
        print("GARI BROKER: {}".format(objs[i]))
        print("IME BROKERA: {}".format(objs[i].name))
        
    for i in range(nodes):
        print("STAMPAM IMENA: {}".format(objs[i]))
    """
    B1 = Broker("Broker1")
    B2 = Broker("Broker2")
    B3 = Broker("Broker3")
    """
    
    """
    B2 = BrokerOther("Broker2")
    B3 = BrokerOther("Broker3")
    """
    
    for i in range(nodes):
        numberOfClients = int(input("Enter total number of clients for Broker{}: ".format(i+1)))
        objs[i].numberOfClients = numberOfClients
        print("BROKER {} IMA {} KLIJENATA: ".format(objs[i], objs[i].numberOfClients))
        for j in range(objs[i].numberOfClients):
            objs[i].clientNames.append("Client" + str(clientCount))
            clientCount += 1
            print("CLIENT COUNT {}".format(clientCount))
        
        
    #add number of clients for each broker
    """
    numberOfClients = int(input("Enter total number of clients for B1: "))
    B1.numberOfClients = numberOfClients
    for i in range(B1.numberOfClients):
        B1.clientNames.append("Client" + str(clientCount))
        clientCount += 1
        print("CLIENT COUNT {}".format(clientCount))
    
    numberOfClients = int(input("Enter total number of clients for B2: "))
    B2.numberOfClients = numberOfClients
    for i in range(B2.numberOfClients):
        B2.clientNames.append("Client" + str(clientCount))
        clientCount += 1
        print("CLIENT COUNT {}".format(clientCount))
    
    numberOfClients = int(input("Enter total number of clients for B3: "))
    B3.numberOfClients = numberOfClients
    for i in range(B3.numberOfClients):
        B3.clientNames.append("Client" + str(clientCount))
        clientCount += 1
        print("CLIENT COUNT {}".format(clientCount))
    """
    
    for i in range(nodes - 1):
        objs[i].neighbours.append(objs[i+1])
    
    objs[nodes - 1].neighbours.append(objs[0])
    
    
    """
    B1.neighbours.append(B2)
    B2.neighbours.append(B3)
    B3.neighbours.append(B1)
    """
    
    for i in range(nodes):
        print("BROKER: {}".format(objs[i].name))
        G.nodes.append(objs[i])
    
    """
    G.nodes.append(B1)
    G.nodes.append(B2)
    G.nodes.append(B3)
    """
    
    #testing queue
    objs[0].q.put("Message One")
    #B1.q.put("Message Two")
    #B1.q.put("Message Three")
    objs[0].q.put("Message Four")
    objs[0].q.put("Message Five")
    objs[0].q.put("Message Six")
    objs[0].q.put("Liman")

    # Append adjacencies
    for i in range(len(adjacency)):
        G.edges.append(adjacency[i])
    # Append subscribers
    #G.subscribers = {G.nodes[0]: [S1, S2], G.nodes[1]: [S3, S4, S5], G.nodes[2]: [S6]}

    return G
        
if __name__ == "__main__":    
    graph = Get_Predefined_Dag()
    P1 = Publisher("Publisher")   
    t1 = threading.Thread(target=P1.generate_string, args=[graph.nodes[0]])
    t1.start()
    print("ISPISUJEM BROKERE")
    print(graph.nodes[0])
    print(graph.nodes[1])
    print(graph.nodes[2])
    graph.BFS(graph.nodes[0])
    flag = 2
    graph.BFS(graph.nodes[0])
    print("Closing connections")
    ServerSocket.close()
    sys.exit()
