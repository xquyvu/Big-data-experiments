import socket
import random
import sys

words = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.".split()

prefix = ['','#']

TCP_IP = '127.0.0.1'
TCP_PORT = 6789

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

try:
    conn, addr = s.accept()
    print ('Connection address:', addr)
    while 1:
        conn.send(bytes(random.choice(prefix) + random.choice(words) + '\n','utf-8'))
    conn.close()
except:
    # Ignore errors such as when spark terminates and closes the socket
    print("Unexpected error:", sys.exc_info())
    pass
