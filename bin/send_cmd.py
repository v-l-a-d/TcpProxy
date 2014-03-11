import socket
import sys


def main(argv):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
    sock.sendto(argv[0], (argv[1], int(argv[2])))

if __name__ == "__main__":
   main(sys.argv[1:])