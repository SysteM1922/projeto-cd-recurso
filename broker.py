from argparse import ArgumentParser
import socket
from src.broker import Broker
from tempfile import TemporaryDirectory
import os

SOCKET_TIMEOUT = 2

if __name__ == '__main__':	
	parser = ArgumentParser()
	parser.add_argument('-f', '--folder', type=str, help='folder with images')
	parser.add_argument('-s', '--height', type=int, help='height of final image')
	args = parser.parse_args()

	if not os.path.exists(args.folder):
		print('Folder ' + args.folder + ' não encontrado!')
		exit(1)
	
	if args.height <= 0:
		print('A altura da imagem deve ser maior que 0!')
		exit(1)

	if len(os.listdir(args.folder)) == 0:
		print('Este folder está vazio!')
		exit(1)


	addr = (socket.gethostbyname(socket.gethostname()), 5000)
	conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 5000)
	conn.settimeout(SOCKET_TIMEOUT)
	conn.bind(addr)

	with TemporaryDirectory() as tempdir:
		broker = Broker(args.folder, args.height, conn, addr, tempdir)
		broker.run()
	