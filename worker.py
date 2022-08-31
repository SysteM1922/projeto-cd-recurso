from argparse import ArgumentParser
import socket
from src.worker import Worker

SOCKET_TIMEOUT = 2

if __name__ == '__main__':

	parser = ArgumentParser()
	parser.add_argument('-b', '--broker', type=int, help='broker address')
	args = parser.parse_args()

	port = args.broker+1
	host = socket.gethostbyname(socket.gethostname())
	conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	conn.settimeout(SOCKET_TIMEOUT)
	while True:
		try:
			addr=(host, port)
			conn.bind(addr)
			break
		except OSError:
			port += 1
	
	worker = Worker((host, args.broker), conn, addr)

	worker.run()

	
	