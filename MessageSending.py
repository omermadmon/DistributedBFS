from socket import socket
from socket import error
from socket import AF_INET
from socket import SOCK_STREAM
from socket import SHUT_RDWR
from time import sleep
import random


def send_message(message, port, ip):
    random.seed(50)
    r = random.uniform(0, 0.03)
    sleep(r)
    while True:
        try:
            sock_tcp = socket(AF_INET, SOCK_STREAM)
            sock_tcp.connect((ip, port))
            sock_tcp.sendall(message.encode())
            sock_tcp.shutdown(SHUT_RDWR)
            break
        except error:
            continue
