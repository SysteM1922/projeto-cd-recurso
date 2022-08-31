import socket
import pickle
import time

PACKET_SIZE = 65507

class Protocol:

	@staticmethod
	def send(conn:socket.socket, msg: dict, addr: tuple, msg_nr: int):
		encode_msg=bytes("<inicio>", 'ascii')+pickle.dumps(msg)+bytes("<fim>", 'ascii')
		b_i=bytes("<"+str(msg_nr)+">", 'ascii')
		b_f=bytes("</"+str(msg_nr)+">", 'ascii')
		while len(encode_msg) > 0:
			time.sleep(0.02)
			conn.sendto(b_i+encode_msg[:PACKET_SIZE-7]+b_f, addr)
			encode_msg = encode_msg[PACKET_SIZE-7:]

	@staticmethod
	def recv(conn:socket.socket):
		try:
			msg, addr = conn.recvfrom(PACKET_SIZE)
			flag=msg[1:2].decode('ascii')
			msg=msg[3:-4]
			return msg, addr, flag
		except socket.timeout:
			return None, None, None

	@staticmethod
	def worker_register(conn:socket.socket, broker: int):
		Protocol.send(conn, {"type" : "worker_register"}, broker, 0)

	@staticmethod
	def give_work_resize(conn:socket.socket, addr: tuple, img: tuple, height: int):
		Protocol.send(conn, {"type" : "resize", "args": {"img": img, "height": height}}, addr, 0)

	@staticmethod
	def give_work_concat(conn:socket.socket, addr: tuple, img1: tuple, img2: tuple):
		Protocol.send(conn, {"type" : "concat", "args": {"img1": img1, "img2": img2}}, addr, 0)

	@staticmethod
	def return_img_resize(conn:socket.socket, img: bytes, broker: tuple):
		Protocol.send(conn, {"type" : "image_resized", "args": {"img": img}}, broker, 1)

	@staticmethod
	def return_img_concat(conn:socket.socket, img: bytes, broker: tuple):
		Protocol.send(conn, {"type" : "image_concat", "args": {"img": img}}, broker, 2)

	@staticmethod
	def im_alive(conn:socket.socket, broker: tuple):
		Protocol.send(conn, {"type" : "im_alive"}, broker, 3)

	@staticmethod
	def close_worker(conn:socket.socket, addr: tuple):
		Protocol.send(conn, {"type" : "close_worker"}, addr, 0)

	@staticmethod
	def error(conn:socket.socket, addr: tuple):
		Protocol.send(conn, {"type" : "error"}, addr, 0)