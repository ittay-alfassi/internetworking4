import time
import socket
import threading
import datetime




class Job:
    def __init__(self, job_kind, job_len): #expect time in sec. save in ns
        self.kind = job_kind
        self.cost = job_len

class Server:
    def __init__(self, kind, address, port_no = 80):
        assert(kind == "V" or kind == "M")
        self.__queued_jobs = 0             # Amount of jobs queued on the server
        self.__t_avail = time.time()    # Point in time where the server will be available
        self.__kind = kind                 # Server kind: "V" for video, "M" for music
        self.__mutex = threading.Lock()

        self.__socket= socket.socket(socket.AF_INET, socket.SOCK_STREAM)    # Server-type socket for the clients
        self.__socket.connect((address, port_no))


    # Returns how much longer will the server be busy
    def __get_tbusy(self):
        if self.__queued_jobs == 0:
            return 0
        return self.__t_avail - time.time()

    # Returns how much a specific job will cost (in ns) on this server
    def __get_job_cost(self, job):
        if self.__kind == "V":
            if job.kind == "V" or job.kind == "P":
                return job.cost
            else:
                return job.cost * 2
        
        else:
            if job.kind == "M":
                return job.cost
            elif job.kind == "P":
                return job.cost * 2
            else:
                return job.cost * 3

    # Returns the total cost of this server
    def get_cost(self, job):
        self.__mutex.acquire()
        res = self.__get_tbusy() + self.__get_job_cost(job)
        self.__mutex.release()
        return res

    # sends 
    def send_and_recv(self, msg, cost):
        self.__mutex.acquire() # Updating the server's queue state
        if self.__queued_jobs == 0:
            self.__t_avail = time.time()
        self.__queued_jobs += 1
        self.__t_avail += cost
        self.__mutex.release()

        self.__socket.send(msg)
        res = self.__socket.recv(1024)
        assert(res)

        self.__mutex.acquire() # Updating the server's queue state
        self.__queued_jobs -= 1
        if self.__queued_jobs == 0:
            self.__t_avail = time.time()
        self.__mutex.release()

        return res

    def get_name(self):
        return self.__socket.getpeername()


class LoadBalancer:
    def __init__(self, address = "10.0.0.1", port_no = 80, client_count = 5):
        print("LB started-----")
        self.__server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__server_sock.bind((address, port_no))
        self.__server_sock.listen(client_count)

        print("Connecting to servers-----")
        s1 = Server("V", "192.168.0.101")
        s2 = Server("V", "192.168.0.102")
        s3 = Server("M", "192.168.0.103")
        self.__servers = [s1, s2, s3]

    def run(self):
        # accept all clients
        while True:
            client_sock, _ = self.__server_sock.accept()
            t = threading.Thread(target=self.__run_client, args=(client_sock,))
            t.start()

    def __run_client(self, client_sock):
        data = client_sock.recv(2)
        assert(data)
        res = self.__send_to_servers(data, client_sock.getpeername())
        client_sock.send(res) #send message back to indicate done
        client_sock.close()


    def __send_to_servers(self, msg, client_addr):
        j = Job(chr(msg[0]), int(chr(msg[1])))
        best_server = None
        best_cost = 2 ** 31 # that's safe, right?
        for s in self.__servers:
            curr = s.get_cost(j)
            if curr < best_cost:
                best_cost = curr
                best_server = s
        assert(best_server != None)

        print(f"{time.localtime()}: recieved request {msg.encode('ASCII')} from {client_addr}, sending to {best_server.get_name()}-----")
        return best_server.send_and_recv(msg, best_cost)



def main():
    lb = LoadBalancer()
    lb.run()
    

main()