import os
from PIL import Image
import time
from src.protocol import Protocol
import collections
import io
import pickle
import shutil
import logging

TIMEOUT = 20

class Broker:

	def __init__(self, folder, height, conn, addr, temp_folder):

		logging.getLogger('PIL').setLevel(logging.WARNING)
		logging.basicConfig(filename="broker.txt", level=logging.DEBUG, format='[%(asctime)s]%(message)s', datefmt='%H:%M:%S')
		
		self.socket = conn
		self.folder = folder
		self.addr = addr
		self.workers_list={}
		self.history={}
		self.works={}
		self.final_image=None
		self.height=height
		self.temp_folder=temp_folder+"/"
		self.combs=[]
		self.init_time=None
		self.final_time=None
		self.time=time.time()-TIMEOUT
		self.errors=0
		self.reserved={}

		images=[]
		for img in os.listdir(folder):
			shutil.copy(folder+"/"+img, self.temp_folder+os.path.splitext(img)[0]+".png")
			images.append(img)
			
		images=sorted(images, key=lambda x: os.stat(folder+"/"+x).st_mtime)
		images=[os.path.splitext(img)[0]+".png" for img in images]
		
		for i in range(0,len(images),2):
			self.combs.append((images[i], images[i+1]))
			if i+2==len(images)-1:
				self.combs.append((images[i+2], None))
				break
		
		nl=0
		l=len(self.combs)
		while nl+1!=l:
			for i in range(nl,l,2):
				self.combs.append((self.combs[i][0], self.combs[i+1][0]))
				if i+2==l-1:
					self.combs.append((self.combs[i+2][0], None))
					break
			nl=l
			l=len(self.combs)
		for comb in list(self.combs):
			if comb[1] is None:
				self.combs.remove(comb)

		print("[BROKER] Broker iniciado")
		logging.debug("[BROKER] Broker iniciado")

	def checkImgAvailable(self, comb):
		for i in self.reserved:
			if self.reserved[i][0]==1:
				if self.reserved[i][1][0] in comb:
					return False
			else:
				if comb==(self.reserved[i][1]):
					return False

		idx=self.combs.index(comb)
		for i in comb:
			for c in range(len(self.combs)):
				if idx > c and i in self.combs[c]:
					return False
		for i in comb:
			for w in self.works:
				if i in self.works[w][0]:
					return False
		return True

	def giveToReserved(self, addr):
		work=self.reserved[addr]
		if work[0] == 1:
			img=work[1][0]
			i=Image.open(self.temp_folder+img)
			Protocol.give_work_resize(self.socket, addr, (self.encodeImg(i), img), self.height)
			self.works[addr]=(work[1], time.time())
			print("[BROKER] Enviar " + os.path.basename(i.filename) + " para redimensionar no worker " + addr[0] +":"+str(addr[1]))
			logging.debug("[BROKER] Enviar " + os.path.basename(i.filename) + " para redimensionar no worker " + addr[0] +":"+str(addr[1]))
		else:
			img0=work[1][0]
			img1=work[1][1]
			i0=Image.open(self.temp_folder+img0)
			i1=Image.open(self.temp_folder+img1)
			Protocol.give_work_concat(self.socket, addr, (self.encodeImg(i0), img0), (self.encodeImg(i1), img1))
			self.works[addr]=(work[1], time.time())
			print("[BROKER] Enviar " + os.path.basename(i0.filename) + " e " + os.path.basename(i1.filename) + " para colagem no worker " + addr[0] +":"+str(addr[1]))
			logging.debug("[BROKER] Enviar " + os.path.basename(i0.filename) + " e " + os.path.basename(i1.filename) + " para colagem no worker " + addr[0] +":"+str(addr[1]))
		self.reserved.pop(addr)

	def getBestWorkerAvailable(self, opt, comb):

		times=[]
		if opt == 1:
			for worker in self.workers_list:
				try:
					times.append((sum(self.workers_list[worker]["resize_list"])/self.workers_list[worker]["n_resizes"], worker))
				except ZeroDivisionError:
					times.append((0, worker))			
			times.sort(key=lambda x: x[0])
		elif opt == 2:
			for worker in self.workers_list:
				try:
					times.append((sum(self.workers_list[worker]["concat_list"])/self.workers_list[worker]["n_concats"], worker))
				except ZeroDivisionError:
					times.append((0, worker))
			times.sort(key=lambda x: x[0])

		worker_avail=None
		for worker in times:
			if worker[1] not in self.works:
				worker_avail=worker
				if times[0]==worker_avail:
					return worker_avail[1]
				break
		
		for worker in times:
			if worker[1] in self.works and worker[1] not in self.reserved:
				if opt==1:
					try:
						if 2*sum(self.workers_list[worker[1]]['resize_list'])/self.workers_list[worker[1]]['n_resizes'] - time.time()+self.works[worker[1]][1] < worker_avail[0]:
							self.reserved[worker[1]]=[opt, comb]
							return None
						else:
							return worker_avail[1]
					except ZeroDivisionError:
						continue
				elif opt==2:
					try:
						if 2*sum(self.workers_list[worker[1]]['concat_list'])/self.workers_list[worker[1]]['n_concats'] - time.time()+self.works[worker[1]][1] < worker_avail[0]:
							self.reserved[worker[1]]=[opt, comb]
							return None
						else:
							return worker_avail[1]
					except ZeroDivisionError:
						continue

		return worker_avail[1]

	def encodeImg(self, img):
		bytesimg = io.BytesIO()
		image = img.convert('RGB')
		image.save(bytesimg, format="png")
		return bytesimg.getvalue()

	def decodeImg(self, img):
		out = io.BytesIO()
		out.write(img)
		return out

	def run(self):

		messages={}
		waiting_alive=[]

		while True:
			if time.time()>self.time+TIMEOUT:
				for worker in list(self.workers_list):
					if worker not in waiting_alive:
						waiting_alive.append(worker)
						if worker in self.works and time.time() - self.works[worker][1] > 2*TIMEOUT:
							self.works.pop(worker)
							print("[BROKER] O trabalho do worker "+ worker[0]+":"+str(worker[1])+" foi cancelado por timeout")
							logging.debug("[BROKER] O trabalho do worker "+ worker[0]+":"+str(worker[1]))
					else:
						if worker in messages:
							if len(messages[worker])>0:
								waiting_alive.pop(worker)
								break
							else:
								messages.pop(worker)
						if worker not in self.history:
							self.history[worker]=self.workers_list[worker]
						else:
							self.history[worker]['n_concats'] += self.workers_list[worker]['n_concats']
							self.history[worker]['n_resizes'] += self.workers_list[worker]['n_resizes']
							self.history[worker]['concat_list'].extend(self.workers_list[worker]['concat_list'])
							self.history[worker]['resize_list'].extend(self.workers_list[worker]['resize_list'])
						self.workers_list.pop(worker)
						waiting_alive.remove(worker)
						if worker in self.works:
							self.works.pop(worker)
							print("[BROKER] O trabalho do worker "+ worker[0]+":"+str(worker[1])+" foi cancelado por timeout")
							logging.debug("[BROKER] O trabalho do worker "+ worker[0]+":"+str(worker[1]))
						print("[BROKER] O worker " + worker[0]+":"+str(worker[1]) + " desligou-se")
						logging.debug("[BROKER] O worker " + worker[0]+":"+str(worker[1]))
				self.time=time.time()
			
			msg = None
			addr = None

			while True:
				rcv_msg, worker_addr, flag = Protocol.recv(self.socket)
				if rcv_msg is None:
					continue
				if worker_addr in waiting_alive:
					waiting_alive.remove(worker_addr)
				if worker_addr not in messages or flag not in messages[worker_addr]:
					messages[worker_addr] = {flag: rcv_msg}
				else:
					messages[worker_addr][flag] += rcv_msg
				if messages[worker_addr][flag].endswith(bytes("<fim>", 'ascii')) and messages[worker_addr][flag].startswith(bytes("<inicio>", 'ascii')):
					b_msg = messages[worker_addr][flag][8:-5]
					addr=worker_addr
					messages[addr].pop(flag)
					try:
						msg=pickle.loads(b_msg)
						break
					except pickle.UnpicklingError:
						self.errors+=1
						print("[BROKER] Erro na leitura da mensagem do worker " + addr[0]+":"+str(addr[1]) + ". A operação não foi aceite")
						logging.debug("[BROKER] Erro na leitura da mensagem do worker " + addr[0]+":"+str(addr[1]))
						if flag==1:
							self.workers_list[addr]['resize_list'].append(time.time()-self.works[addr][1])
						elif flag==2:
							self.workers_list[addr]['concat_list'].append(time.time()-self.works[addr][1])
						if addr in self.works:
							self.works.pop(addr)

			if msg['type'] == 'worker_register' and addr not in self.workers_list:
				self.workers_list[addr]={'n_concats': 0, 'n_resizes': 0, 'concat_list': [], 'resize_list': []}
				print("[BROKER] Confirmado o registo do worker " + addr[0]+":"+str(addr[1]))
				logging.debug("[BROKER] Confirmado o registo do worker " + addr[0]+":"+str(addr[1]))

			elif msg['type'] == 'im_alive' and addr in waiting_alive:
				waiting_alive.remove(addr)

			elif addr in self.works:
				if msg['type'] == 'image_resized':
					self.workers_list[addr]['resize_list'].append(time.time() - self.works[addr][1])
					self.workers_list[addr]['n_resizes'] += 1
					name=self.works[addr][0][0]
					self.works.pop(addr)
					image=Image.open(self.decodeImg(msg['args']['img']))
					image = image.save(self.temp_folder+name)
					print("[BROKER] Recepcionado " + name + " após redimencionamento no worker " + addr[0]+":"+str(addr[1]))
					logging.debug("[BROKER] Recepcionado " + name + " após redimencionamento no worker " + addr[0]+":"+str(addr[1]))
					if addr in self.reserved:
						self.giveToReserved(addr)

				elif msg["type"] == "image_concat":
					self.workers_list[addr]['concat_list'].append(time.time() - self.works[addr][1])
					self.workers_list[addr]['n_concats'] += 1
					name=self.works[addr][0][0]
					self.combs.remove(self.works[addr][0])
					os.remove(self.temp_folder+self.works[addr][0][1])
					self.works.pop(addr)
					image=Image.open(self.decodeImg(msg['args']['img']))
					image = image.save(self.temp_folder+name)
					print("[BROKER] Recepcionado " + name + " após colagem no worker " + addr[0]+":"+str(addr[1]))
					logging.debug("[BROKER] Recepcionado " + name + " após colagem no worker " + addr[0]+":"+str(addr[1]))
					if addr in self.reserved:
						self.giveToReserved(addr)

				elif msg['type'] == 'error':
					self.errors+=1
					print("[BROKER] Erro na leitura da mensagem no worker " + addr[0]+":"+str(addr[1]) + ". A operação foi cancelada")
					logging.debug("[BROKER] Erro na leitura da mensagem no worker " + addr[0]+":"+str(addr[1]))
					if addr in self.works:
						self.works.pop(addr)

			
			if len(self.workers_list)!=len(self.works):
				for comb in self.combs:
					if self.checkImgAvailable(comb):
						img=None

						if Image.open(self.temp_folder+comb[0]).size[1] != self.height:
							img = comb[0]

						elif Image.open(self.temp_folder+comb[1]).size[1] != self.height:
							img = comb[1]
						
						if img is None:
							worker=self.getBestWorkerAvailable(2, comb)
							if worker is None:
								break
							img0=Image.open(self.temp_folder+comb[0])
							img1=Image.open(self.temp_folder+comb[1])
							Protocol.give_work_concat(self.socket, worker, (self.encodeImg(img0), comb[0]), (self.encodeImg(img1), comb[1]))
							self.works[worker]=(comb, time.time())
							print("[BROKER] Enviar " + os.path.basename(img0.filename) + " e " + os.path.basename(img1.filename) + " para colagem no worker " + worker[0] +":"+str(worker[1]))
							logging.debug("[BROKER] Enviar " + os.path.basename(img0.filename) + " e " + os.path.basename(img1.filename) + " para colagem no worker " + worker[0] +":"+str(worker[1]))

						else:
							worker=self.getBestWorkerAvailable(1, (img, None))
							if worker is None:
								break
							i=Image.open(self.temp_folder+img)
							Protocol.give_work_resize(self.socket, worker, (self.encodeImg(i), img), self.height)
							self.works[worker]=((img, None), time.time())
							print("[BROKER] Enviar " + os.path.basename(i.filename) + " para redimensionar no worker " + worker[0] +":"+str(worker[1]))
							logging.debug("[BROKER] Enviar " + os.path.basename(i.filename) + " para redimensionar no worker " + worker[0] +":"+str(worker[1]))

						if self.init_time is None:
							self.init_time=time.time()
						break

				if len(os.listdir(self.temp_folder))==1:
					self.final_time=time.time()
					self.final_image=os.listdir(self.temp_folder)[0]
					i=Image.open(self.temp_folder+self.final_image)
					i.save("final_image.png")
					os.remove(self.temp_folder+self.final_image)
					self.final_image="final_image.png"
					Image.open(self.final_image).show()
					print("[BROKER] Imagem final: " + self.final_image+"\n")
					logging.debug("[BROKER] Imagem final: " + self.final_image+"\n")
					break

		for worker in list(self.workers_list):
			if worker not in self.history:
				self.history[worker]=self.workers_list[worker]

			else:
				self.history[worker]['n_concats'] += self.workers_list[worker]['n_concats']
				self.history[worker]['n_resizes'] += self.workers_list[worker]['n_resizes']
				self.history[worker]['concat_list'].extend(self.workers_list[worker]['concat_list'])
				self.history[worker]['resize_list'].extend(self.workers_list[worker]['resize_list'])

			Protocol.close_worker(self.socket, worker)

		self.history=collections.OrderedDict(sorted(self.history.items()))
		n_resizes=sum([ self.history[worker]['n_resizes'] for worker in self.history])
		n_concats=sum([ self.history[worker]['n_concats'] for worker in self.history])
		all_resized_times=[tm for worker in self.history for tm in self.history[worker]['resize_list']]
		all_concat_times=[tm for worker in self.history for tm in self.history[worker]['concat_list']]

		print("Número de redimensionamentos totais: " + str(n_resizes))
		logging.debug(" Número de redimensionamentos totais: " + str(n_resizes))
		print("Número de colagens totais: " + str(n_concats))
		logging.debug(" Número de colagens totais: " + str(n_concats))
		print("Número de redimensionamentos por worker (média): " + str(n_resizes/len(self.history) if len(self.history)!=0 else 0))
		logging.debug(" Número de redimensionamentos por worker (média): " + str(n_resizes/len(self.history) if len(self.history)!=0 else 0))
		print("Número de colagens por worker (média): " + str(n_concats/len(self.history) if len(self.history)!=0 else 0))
		logging.debug(" Número de colagens por worker (média): " + str(n_concats/len(self.history) if len(self.history)!=0 else 0))
		print("Tempo mínimo de um redimensionamento: "+str(min(all_resized_times) if len(all_resized_times)!=0 else 0) + " s")
		logging.debug(" Tempo mínimo de um redimensionamento: "+str(min(all_resized_times) if len(all_resized_times)!=0 else 0) + " s")
		print("Tempo médio de um redimensionamento: "+str(sum(all_resized_times)/len(all_resized_times) if len(all_resized_times) != 0 else 0) + " s")
		logging.debug(" Tempo médio de um redimensionamento: "+str(sum(all_resized_times)/len(all_resized_times) if len(all_resized_times) != 0 else 0) + " s")
		print("Tempo máximo de um redimensionamento: "+str(max(all_resized_times) if len(all_resized_times) else 0) + " s")
		logging.debug(" Tempo máximo de um redimensionamento: "+str(max(all_resized_times) if len(all_resized_times) else 0) + " s")
		print("Tempo mínimo de uma colagem: "+str(min(all_concat_times) if len(all_concat_times) != 0 else 0) + " s")
		logging.debug(" Tempo mínimo de uma colagem: "+str(min(all_concat_times) if len(all_concat_times) != 0 else 0) + " s")
		print("Tempo médio de uma colagem: "+str(sum(all_concat_times)/len(all_concat_times) if len(all_concat_times) != 0 else 0) + " s")
		logging.debug(" Tempo médio de uma colagem: "+str(sum(all_concat_times)/len(all_concat_times) if len(all_concat_times) != 0 else 0) + " s")
		print("Tempo máximo de uma colagem: "+str(max(all_concat_times) if len(all_concat_times) != 0 else 0) + " s")
		logging.debug(" Tempo máximo de uma colagem: "+str(max(all_concat_times) if len(all_concat_times) != 0 else 0) + " s")
		for worker in self.history:
			print("Worker "+str(worker)+": Redimensionamentos "+str(self.history[worker]['n_resizes'])+", Colagens: "+str(self.history[worker]['n_concats']))
			logging.debug(" Worker "+str(worker)+": Redimensionamentos "+str(self.history[worker]['n_resizes'])+", Colagens: "+str(self.history[worker]['n_concats']))
		print("Tempo total de execução: " + str(self.final_time-self.init_time if self.init_time is not None else 0) + " s")
		logging.debug(" Tempo total de execução: " + str(self.final_time-self.init_time if self.init_time is not None else 0) + " s")
		print("Número de erros durante a leitura de mensagens: " + str(self.errors))
		logging.debug(" Número de erros durante a leitura de mensagens: " + str(self.errors) + "\n")

		self.socket.close()
		print("\n[BROKER] Broker fechado")
		logging.debug("[BROKER] Broker fechado")