import socket
import threading
from random import sample



class LoadBalancer:
    """
    Class for implementing the load balancer

    Attributes
    ----------
    remote : (str,int)
        a tuple to Identify the connection
    servers : [Server]
        an array for the connected servers using the inner class Server
    socket: socket
        a socket for the client to reach the load balancer

    Methods
    -------
    run(servers):
        initiates a connection to the servers and runs the load balancer.
    """
    def __init__(self, host, port):
        """
        initialises a load balancer instance
        :param host: the host for the connection of the clients to the load balancer
        :param port: the port for the connection of the clients to the load balancer
        """
        self.remote = {'host': host, 'port': port}
        self.servers = []
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("Load Balancer Initialized")

    def run(self, servers):
        """
        initiates a connection to the servers and runs the load balancer.
        :param servers: an array of tuples, each tuple contains the host, port and type of the server
        :return: no return value
        """
        for server_host, server_port, server_type in servers:
            self.servers += [self.Server(server_host, server_port, server_type)]
            print("Server {} at port {} Connected".format(server_host, server_port))

        self.socket.bind((self.remote['host'], self.remote['port']))
        self.socket.listen(40)
        print("Load Balancer Connected")

        while True:
            client_conn, _ = self.socket.accept()
            t = threading.Thread(None, self.__handle_request, None, (client_conn,)).start()

    def __handle_request(self, conn):
        """
        a private method which handles a request from the client
        sending it to a server and sending back the response
        :param conn: a socket which connects to the client
        :return: no return value
        """
        request = conn.recv(1024)
        if request is None:
            print("Request Failed")
            conn.close()
        else:
            request_type, request_length = request[0], int(request[1])
            print('Request of type {} with length {} Accepted'.format(request_type, request_length))
            server = self.__get_server(request_type, request_length)
            conn.sendall(server.request(request_type, request_length))
            print("Request of type {} with length {} Succeeded".format(request_type, request_length))
            conn.close()

    def __get_server(self, request_type, request_length):
        """
        a private method which finds a server to send a request to
        using the power of two load balancing algorithm
        :param request_type: the type of request
        :param request_length: the length of the request
        :return: the chosen server
        """
        server_1, server_2 = [self.servers[i] for i in sample(range(0, len(self.servers)), 2)]
        if server_1.server_cost(request_type, request_length, True) < \
                server_2.server_cost(request_type, request_length, True):
            return server_1
        else:
            return server_2

    class Server:
        """
        Inner Class for implementing a connection to the server

        Attributes
        ----------
        current_cost: int
            the current workload on the server and cost to use it.
            equal to the sum of all costs of the current requests on the server
        socket: socket
            a socket for connecting to the server
        server_type: string
            the type of the server
        Methods
        -------
        server_cost(self, request_type, request_length, total=False) -> int:
            returns the cost of applying this request on the server when total is False,
            when total is True returns the total cost with the cost of the given request.
        request(self, request_type, request_length) -> string:
            sends the request to the server and returns the response
        """
        def __init__(self, host, port, server_type):
            """
            initialises a server connection instance
            :param host: the host of the server
            :param port: the port of the server
            :param server_type: the type of the server
            """
            if server_type not in ['V', 'M']:
                raise ValueError('Invalid server type {}'.format(server_type))
            else:
                self.type = server_type
            self.current_cost = 0
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((host, port))

        def server_cost(self, request_type, request_length, total=False):
            """
            returns the cost of applying this request on the server when total is False,
            when total is True returns the total cost with the cost of the given request.
            :param request_type: the type of the request
            :param request_length: the length of the request
            :param total: a boolean value to indicate if to add the current cost of the server or not
            :return: if total is False the cost of the given request if total is True the cost of all the request
                     on the server including the given request
            """
            if request_type not in ['V', 'P', 'M']:
                raise ValueError('Invalid request type {}'.format(request_type))
            if self.type == 'V':
                if request_type in ['V', 'P']:
                    type_cost = 1
                else:
                    type_cost = 2
            else:
                if request_type == 'M':
                    type_cost = 1
                elif request_type == 'P':
                    type_cost = 2
                else:
                    type_cost = 3
            if total:
                return self.current_cost + type_cost * request_length
            else:
                return type_cost * request_length

        def request(self, request_type, request_length):
            """
            sends the request to the server and returns the response.
            :param request_type: the type of the request
            :param request_length: the length of the request
            :return: the response of the server
            """
            request_cost = self.server_cost(request_type, request_length)
            self.current_cost += request_cost
            self.socket.send(('{}{}'.format(request_type, request_length)).encode())
            response = self.socket.recv(1024)
            self.current_cost -= request_cost
            return response



if __name__ == '__main__':
    servers = [
        ('192.168.0.100', 80, 'V'),
        ('192.168.0.101', 80, 'V'),
        ('192.168.0.102', 80, 'V'),
        ('192.168.0.103', 80, 'V'),
        ('192.168.0.104', 80, 'V'),
        ('192.168.0.105', 80, 'V'),
        ('192.168.0.106', 80, 'M'),
        ('192.168.0.107', 80, 'M'),
        ('192.168.0.108', 80, 'M'),
        ('192.168.0.109', 80, 'M')
    ]
    LoadBalancer('10.0.0.1', 80).run(servers)
