from PIL import Image
from src.protocol import Protocol
import io
import pickle
import random
import time
import logging

CHANCE_TO_CLOSE = 1 #Value from 0 to infinity

class Worker:

	def __init__(self, broker, conn, addr):

		name="worker"+str(addr[1])
		logging.getLogger('PIL').setLevel(logging.WARNING)
		logging.basicConfig(filename=name+".txt", level=logging.DEBUG, format='[%(asctime)s]%(message)s', datefmt='%H:%M:%S')

		self.socket = conn
		self.addr = addr
		self.broker = broker
		self.delay = random.randint(4, 10)
		self.time=time.time()

		Protocol.worker_register(self.socket, self.broker)
		print("[WORKER] Registo do worker " + self.addr[0] + ":" + str(self.addr[1]))
		logging.debug("[WORKER] Registo do worker " + self.addr[0] + ":" + str(self.addr[1]))

	def encodeImg(self, img):
		bytesimg = io.BytesIO()
		img.save(bytesimg, format="png")
		return bytesimg.getvalue()

	def decodeImg(self, img):
		out = io.BytesIO()
		out.write(img)
		return out

	def run(self):

		while True:
			tim=time.time() - self.time
			if tim > 5:
				chance=(100-tim* CHANCE_TO_CLOSE)/100
				if random.random() > chance:
					self.socket.close()
				self.time=time.time()
			
			msg = None
			b_msg = b""
			while True:
				rcv_msg, worker_addr, flag = Protocol.recv(self.socket)
				if rcv_msg == None:
					break
				b_msg+=rcv_msg
				if b_msg.endswith(bytes("<fim>", 'ascii')):
					if b_msg.startswith(bytes("<inicio>", 'ascii')):
						b_msg = b_msg[8:-5]
						try:
							msg=pickle.loads(b_msg)
						except pickle.UnpicklingError:
							Protocol.error(self.socket, self.broker)
						break
			
			Protocol.im_alive(self.socket, self.broker)

			if msg is not None:

				if msg['type'] == 'resize':
					img = Image.open(self.decodeImg(msg['args']['img'][0]))
					print("[WORKER] Recepcionado " + msg['args']['img'][1])
					logging.debug("[WORKER] Recepcionado " + msg['args']['img'][1])
					width=img.size[0]
					height=img.size[1]
					scale=width/height
					new_height=msg['args']['height']
					new_width=int(new_height*scale)
					return_img=img.resize((new_width, new_height), Image.ANTIALIAS)
					time.sleep(self.delay-1+random.random()*2)
					print("[WORKER] Redimensionamento completado")
					logging.debug("[WORKER] Redimensionamento completado")
					Protocol.return_img_resize(self.socket, self.encodeImg(return_img), self.broker)

				elif msg['type'] == 'concat':
					img1 = Image.open(self.decodeImg(msg['args']['img1'][0]))
					img2 = Image.open(self.decodeImg(msg['args']['img2'][0]))
					print("[WORKER] Recepcionado " + msg['args']['img1'][1] + " e " + msg['args']['img2'][1] + " para colagem")
					logging.debug("[WORKER] Recepcionado " + msg['args']['img1'][1] + " e " + msg['args']['img2'][1] + " para colagem")

					return_img = Image.new('RGB', (img1.size[0] + img2.size[0], img1.size[1]))
					return_img.paste(img1, (0, 0))
					return_img.paste(img2, (img1.size[0], 0))
					print("[WORKER] Colagem completada")
					logging.debug("[WORKER] Colagem completada")
					time.sleep(self.delay-1+random.random()*2)
					Protocol.return_img_concat(self.socket, self.encodeImg(return_img), self.broker)

				elif msg["type"] == 'close_worker':
					self.socket.close()
					print("[WORKER] Worker fechado")
					logging.debug("[WORKER] Worker fechado")
					break