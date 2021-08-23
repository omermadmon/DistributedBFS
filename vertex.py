from socket import socket
from socket import error
from socket import AF_INET
from socket import SOCK_STREAM
from threading import Thread
from threading import Lock
from MessageSending import send_message


class Vertex:
    def __init__(self, id, num_vertices, port, num, neighbours):
        self.id = id
        self.terminate = False
        self.ip = '127.0.0.1'
        self.num = num
        self.sum = num  # return value - sum of tree rooted at self
        self.num_vertices = num_vertices
        self.port = port
        self.k = 0
        self.child = set()  # children
        self.bvalue = 'F'
        self.parent = None
        self.neighbours = neighbours
        self.expected_replies = {n: 0 for n in neighbours}
        # num_vertices+1 as inf, since this more than the maximum depth possible
        self.INF = num_vertices + 1
        self.levels = {n: self.INF for n in neighbours}
        self.level = self.INF
        self.PORT = 0  # index of neighbour n port (inside the tuple representation of n)
        self.IP = 1  # index of neighbour n ip
        self.ID = 2

        self.threads = []

    def begin(self):
        if self.id == 1:  # v is source
            self.level = 0
            self.k = 1
            for n in self.neighbours:
                self.child.add(n)
                message = self.build_message('explore', self.k)
                send_message(message, n[self.PORT], n[self.IP])
                self.expected_replies[n] = 1

    def build_message(self, type, f):
        message = type + ','
        message = message + str(f) + ','
        message = message + str(self.id) + ','
        message = message + str(self.port) + ','
        message = message + str(self.ip)
        return message

    def listen(self):
        with socket(AF_INET, SOCK_STREAM) as s:
            s.bind((self.ip, self.port))
            s.listen()
            while not self.terminate:
                while True:
                    try:
                        conn, addr = s.accept()
                        break
                    except error:
                        print('error in listening')
                        continue
                data = ''
                with conn:
                    while True:
                        data = conn.recv(1024)
                        if not data:
                            break
                        conn.sendall(data)
                        break

                message = data.decode().split(',')
                message_type = message[0]

                locker = Lock()
                if message_type == 'explore':
                    t = Thread(target=self.handle_explore, args=(message, locker),
                               daemon=True, name=str(self.port)+'::: '+data.decode())
                    self.threads.append(t)
                    t.start()
                    t.join(3)
                elif message_type == 'reverse':
                    t = Thread(target=self.handle_reverse, args=(message, locker),
                               daemon=True, name=str(self.port)+'::: '+data.decode())
                    self.threads.append(t)
                    t.start()
                    t.join(3)
                elif message_type == 'forward':
                    t = Thread(target=self.handle_forward, args=(message, locker),
                               daemon=True,name=str(self.port)+'::: '+data.decode())
                    self.threads.append(t)
                    t.start()
                    t.join(3)
                elif message_type == 'terminate':
                    t = Thread(target=self.handle_terminate, args=(message, locker),
                               daemon=True, name=str(self.port) + '::: ' + data.decode())
                    self.threads.append(t)
                    t.start()
                    t.join(3)
                elif message_type == 'sum':
                    t = Thread(target=self.handle_sum, args=(message, locker),
                               daemon=True, name=str(self.port) + '::: ' + data.decode())
                    self.threads.append(t)
                    t.start()
                    t.join(3)

    def handle_explore(self, m, locker):
        # for process u, upon receipt a message <explore, f> from v
        locker.acquire()

        f = int(m[1])
        v_id = m[2]
        v_port = int(m[3])
        v_ip = m[4]
        self.levels[(v_port, v_ip)] = f - 1

        if self.level == self.INF:
            self.parent = (v_port, v_ip, v_id)
            self.level = f
            message = self.build_message('reverse', 'T')
            send_message(message, v_port, v_ip)

        elif self.level == f:
            message = self.build_message('reverse', 'F')
            send_message(message, v_port, v_ip)

        elif self.level == f - 1:
            message = self.build_message('reverse', 'F')
            send_message(message, v_port, v_ip)

        locker.release()
        return

    def handle_reverse(self, m, locker):
        # for process u, upon receipt a message <reverse, b> from v
        locker.acquire()

        b = m[1]  # 'T' or 'F'
        v_id = m[2]
        v_port = int(m[3])
        v_ip = m[4]
        v = (v_port, v_ip)

        self.expected_replies[v] -= 1

        if b == 'T':
            self.child.add(v)
            self.bvalue = 'T'

        expects_a_reply = False
        for k,v in self.expected_replies.items():
            if v != 0:
                expects_a_reply = True

        if not expects_a_reply:
            if self.parent is not None:
                message = self.build_message('reverse', self.bvalue)
                send_message(message, self.parent[self.PORT], self.parent[self.IP])
            elif self.bvalue == 'T':
                self.k += 1
                for c in self.child:
                    message = self.build_message('forward', self.k)
                    send_message(message, c[self.PORT], c[self.IP])
                    self.bvalue = 'F'
                    self.expected_replies[c] = 1
            else:
                for c in self.child:
                    message = self.build_message('terminate', 0)
                    send_message(message, c[self.PORT], c[self.IP])
                    self.expected_replies[c] = 1

        locker.release()
        return

    def handle_forward(self, m, locker):
        # for process u, upon receipt a message <forward, f> from v
        locker.acquire()

        f = int(m[1])
        v_id = m[2]
        v_port = int(m[3])
        v_ip = m[4]
        v = (v_port, v_ip)

        self.bvalue = 'F'
        for n in self.neighbours:
            self.expected_replies[n] = 0

        if self.level < f - 1:
            leaf = True
            for c in self.child:
                message = self.build_message('forward', f)
                send_message(message, c[self.PORT], c[self.IP])
                self.expected_replies[c] += 1
                leaf = False
            if leaf:
                message = self.build_message('reverse', 'F')
                send_message(message, v_port, v_ip)

        if self.level == f - 1:
            leaf = True
            for n in self.neighbours:
                if self.levels[n] == (f - 2):
                    continue
                else:
                    message = self.build_message('explore', f)
                    send_message(message, n[self.PORT], n[self.IP])
                    self.expected_replies[n] += 1
                    leaf = False
            if leaf:
                message = self.build_message('reverse', 'F')
                send_message(message, v_port, v_ip)

        locker.release()
        return

    def handle_terminate(self, m, locker):
        # for process u, upon receipt a message <terminate, _> from v
        locker.acquire()

        _ = int(m[1])
        v_id = m[2]
        v_port = int(m[3])
        v_ip = m[4]
        v = (v_port, v_ip)

        leaf = True
        for c in self.child:
            self.expected_replies[c] = 0
            message = self.build_message('terminate', _)
            send_message(message, c[self.PORT], c[self.IP])
            self.expected_replies[c] += 1
            leaf = False
        if leaf:
            message = self.build_message('sum', self.num)
            send_message(message, v_port, v_ip)
            self.terminate_procedure()

        locker.release()
        return

    def handle_sum(self, m, locker):
        # for process u, upon receipt a message <sum, s> from v
        locker.acquire()

        s = int(m[1])  # sum of sub-tree rooted at v (a child of u)
        v_id = m[2]
        v_port = int(m[3])
        v_ip = m[4]
        v = (v_port, v_ip)

        self.expected_replies[v] -= 1
        self.sum += s

        if not self.expects_a_reply():
            if self.parent is not None:
                message = self.build_message('sum', self.sum)
                send_message(message, self.parent[self.PORT], self.parent[self.IP])
            self.terminate_procedure()

        locker.release()
        return

    def expects_a_reply(self):
        for k, v in self.expected_replies.items():
            if v != 0:
                return True
        return False

    def terminate_procedure(self):
        file_name = 'output_vertex_' + str(self.id) + '.txt'
        with open(file_name, 'w') as f:
            f.write(str(self.level))
            f.write('\n')
            if self.parent is not None:
                f.write(self.parent[self.ID])
            else:
                f.write('root')
            f.write('\n')
            f.write(str(self.sum))
        self.terminate = True


def vertex(ID):
    with open('input_vertex_' + str(ID) + '.txt') as f:
        r = f.readlines()
        r = [line.split('\n')[0] for line in r][:-1]
        num_vertices = int(r[0])
        port = int(r[1])
        num = int(r[2])
        neighbours = []
        r_neighbours = r[3:]
        for i in range(0, len(r_neighbours), 2):
            neighbours.append((int(r_neighbours[i]), r_neighbours[i+1]))
        u = Vertex(ID, num_vertices, port, num, neighbours)

    u.begin()
    u.listen()
