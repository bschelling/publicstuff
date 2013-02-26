import logging
import logging.handlers
import sys
import gevent
import zerorpc
import time
import greenlet
import inspect
import collections
import atexit
import urllib
import urllib2
import os

class NullElection(object):

	def __init__(self, addr, logserver, proxyserver, counter, config_file="server_config"):
		self.addr = addr
		self.connections = []
		self.servers = []
		self.acks = set()
		self.stati = {"Norm": 0, "Elec1": 1, "Elec2": 2, "Wait": 3}
		self.proxy_addr = proxyserver
		self.logserver = logserver

		self.definition = ""
		self.priority = 0
		self.haltnode = ""
		self.task_description = ""
		self.lastcontact = time.time()

		self.incarn = counter
		self.nextel = 0
		self.elid = ""
		self.ldr = 0

		self.fdlist = set()

		#initialised in StartStage2
		f = open(config_file, 'r')
		for line in f.readlines():
			line = line.rstrip()
			self.servers.append(line)

		print 'My addr: %s' % (self.addr)
		print 'Server list: %s' % (str(self.servers))
		print 'Logserver: %s' % self.logserver
		print 'Proxyserver: %s' % self.proxy_addr


		self.n = len(self.servers)
		#this is the set with higher priorities
		self.lesser = set()
		#this is the set with lower priorities
		self.greater = set()

		#this are the nodes we play dead to
		self.denied = set()

		self.pendack = 0
		self.i = -1


		if self.proxy_addr != "":
			self.proxy_conn = zerorpc.Client(timeout=10)
			self.proxy_conn.connect('tcp://' +self.proxy_addr)

			for i, server in enumerate(self.servers):
				if server == self.addr:
					self.i = i
					self.connections.append(self)
				else:
					if self.i == -1:
						self.lesser.add(server)
					else:
						self.greater.add(server)

					if self.proxy_addr == "":
						c = zerorpc.Client(timeout=10)
						c.connect('tcp://' + server)
						self.connections.append(c)

		#set self status to undefined (-1) so that others lern we're not normal!
		self.status = -1
		self.dbgcounter = 0

	#todo better datastructure to deal with prios and addresses?
	def id_by_elid(self,elid):
		return elid.split(",")[0]


	def prio_by_addr(self, addr):
		for i, server in enumerate(self.servers):
			if server == addr:
				return i
		logging.error('[%s] prio_by_addr couldnt find prio for %s called by %s!', self.addr, addr ,inspect.stack()[1][3])


	#helper function to get human readable status
	def status_by_nr(self,status):
		return  self.stati.keys()[self.stati.values().index(status)]

	def PlayDead(self, nodeset):
		self.denied.union(nodeset)
		logging.debug('[%s] playing dead denied is %s ',self.addr, str(self.denied))

	def PlayAlive(self, nodeset):
		for i in nodeset:
			if i in self.denied:
				self.denied.remove(i)
		logging.debug('[%s] playing alive denied is %s ',self.addr, str(self.denied))

	def StartFD(self, remote_addr):
		self.fdlist.add(remote_addr)
		logging.debug('[%s] added %s to failure detection ',self.addr, str(self.fdlist))

	def FD(self):
		while True:
			gevent.sleep(5)
			#logging.debug('[%s] fdlist %s', self.addr, self.fdlist)

			#make a copy here before iteration!
			checklist = self.fdlist.copy()
			for remote_addr in checklist:
				prio = self.prio_by_addr(remote_addr)
				try:
					if self.proxy_addr:
						isAlive = self.proxy_conn.OnReportStatusToFD(self.addr,remote_addr,self.addr)
					else:
						isAlive = self.connections[prio].OnReportStatusToFD(self.addr)
				except zerorpc.TimeoutExpired:
					logging.debug('[%s] timeout send OnReportStatusToFD to %s ',self.addr, remote_addr)
					isAlive = False

				if not isAlive:
					logging.debug('[%s] remote addr %s reported down ',self.addr, remote_addr)
					#remove from self.fdlist here we have a copy
					self.onDownSig(remote_addr)

	def StopFD(self, remote_addr):
		if remote_addr in self.fdlist:
			self.fdlist.remove(remote_addr)

	def OnReportStatusToFD(self,remote_addr):
		if remote_addr not in self.denied:
			#logging.debug('[%s] OnReportStatusToFD called by %s tell him ok ',self.addr, remote_addr)
			return True
		else:
			logging.debug('[%s] OnReportStatusToFD called by %s tell him NO! ',self.addr, remote_addr, str(self.denied))
			return False


	def onReject(self,remote_addr,elid):
		#for whatever reason we got rejected
		logging.debug('[%s] we got rejected from %s ',self.addr, remote_addr)
		#self.pendack is next lower priority... check continstage2
		if self.status == self.stati["Elec2"] and self.prio_by_addr(remote_addr) == self.pendack:
			self.ContinStage2()


	# a node receives a halt from remote addr, check if prio is higher or we are engaged
	# in a election with a potential leader with higher priority
	def onHalt(self,remote_addr, elid):
		logging.debug('[%s] received on halt request from %s ',self.addr, remote_addr)
		remote_prio = self.prio_by_addr(remote_addr)
		#reject if not status normal or my leader has higher priority (which ironically means lower prio)
		do_reject1  = self.status == self.stati["Norm"] and self.prio_by_addr(self.ldr) < remote_prio
		do_reject2 = self.status == self.stati["Wait"] and self.prio_by_addr(self.id_by_elid(elid)) < remote_prio
		# i was in norm but some lower priority tried to halt me

		if do_reject1 and self.status == self.stati["Norm"]:
			logging.debug('[%s] status NORMAL halt ignored from %s my prio %s is higher than %s ', self.addr, remote_addr, self.prio_by_addr(self.ldr), remote_prio)
		# i was in wait so i accepted a halting request but this was from a higher priority
		# than from the node that tries to halt me now
		if  do_reject2 and self.status == self.stati["Wait"]:
			logging.debug('[%s] status WAIT halt ignored from %s my ldr %s prio %s is higher than %s', self.addr, remote_addr, self.id_by_elid(self.elid), self.prio_by_addr(self.id_by_elid(elid)), remote_prio)

		logging.debug('[%s] do_reject1 %s do_reject2 %s my status %s', self.addr, do_reject1, do_reject2, self.status_by_nr(self.status))

		if do_reject1 or do_reject2:
			logging.debug('[%s] cond1 %s cond2 %s my status %s', self.addr, do_reject1, do_reject2, self.status_by_nr(self.status))
			try:
				if self.proxy_addr:
					self.proxy_conn.onReject(self.addr, remote_addr, remote_addr, elid)
				else:
					self.connections[remote_prio].onReject(remote_addr,elid)
			except zerorpc.TimeoutExpired:
				logging.debug('[%s] timeout send onAck to ',self.addr, remote_addr)
		else:
			#could be in status Elec2 or in status Elec1
			logging.debug('[%s] receiver on halt from %s ok halting ',self.addr,remote_addr)
			self.Halting(remote_addr, elid)


	def Halting(self, remote_addr, elid):
		#we don't accept requests from lower priority nodes anymore
		self.PlayDead(self.greater)
		#start monitoring the node who halted us
		self.StartFD(remote_addr)
		self.elid = elid
		self.status = self.stati["Wait"]
		logging.debug('[%s] accepted halt from %s now in wait with elid %s ', self.addr, remote_addr, str(elid))
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			if self.proxy_addr:
				self.proxy_conn.onAck(self.addr, remote_addr, self.addr, elid)
			else:
				self.connections[remote_prio].onAck(self.addr, elid)
		except zerorpc.TimeoutExpired:
			logging.debug('[%s] >>>>>>>>>>>>>> timeout send onAck to %s ',self.addr, remote_addr)

	def onAck(self,remote_addr, elid):
		# in the election and i receive an ack from a node that has been halted by me
		# check if the ack really is from the node i just halted (pendack) and this message is part of the
		# election that i initiated with the right election id
		remote_prio = self.prio_by_addr(remote_addr)
		if self.status == self.stati["Elec2"] and elid == self.elid and remote_prio == self.pendack:
			self.acks.add(remote_addr)
			self.ContinStage2()
		else:
			if self.status != self.stati["Elec2"]:
				logging.debug('[%s] remote addr %s sends me an ack but i am in status %s ', self.addr, remote_addr, self.status_by_nr(self.status))
			if elid != self.elid:
				logging.debug('[%s] remote addr %s sends me an ack but  my elid is %s and not %s ', self.addr, remote_addr, self.elid, elid)
			if remote_prio != self.pendack:
				logging.debug('[%s] remote addr %s sends me an ack but my pendack %s is not %s ', self.addr, remote_addr, self.pendack, remote_prio)


	def ContinStage2(self):
		self.dbgcounter +=1
		logging.debug('[%s] id %s continstage2 called from %s',self.addr,self.dbgcounter, inspect.stack()[1][3])
		#check all nodes with lower priority here, my id starts with zero
		if self.pendack < (self.n-1) :
			self.pendack +=1
			self.StartFD(self.servers[self.pendack])
			try:
				logging.debug('[%s] id %s my pendack %s',self.addr, self.dbgcounter, self.pendack)
				logging.debug('[%s] id %s halting %s ',self.addr,  self.dbgcounter, self.servers[self.pendack])
				if self.proxy_addr:
					self.proxy_conn.onHalt(self.addr, self.servers[self.pendack], self.addr, self.elid)
				else:
					self.connections[self.pendack].onHalt(self.addr,self.elid)
			except zerorpc.TimeoutExpired:
				logging.debug('[%s] timeout trying to halt %s calling downsig ',self.addr, self.servers[self.pendack])
				#Todo this should be captured by failure detector
				self.onDownSig(self.servers[self.pendack])

		else:
			#yes we won....
			self.ldr = self.addr
			self.status = self.stati["Norm"]
			logging.debug('[%s] id %s I am Leader have to notify %s', self.addr,self.dbgcounter, str(self.acks))
			#thes acks are set in
			for remote_addr in self.acks:
				remote_prio = self.prio_by_addr(remote_addr)
				try:
					if self.proxy_addr:
						self.proxy_conn.onLdr(self.addr, remote_addr, self.addr, self.elid)
					else:
						self.connections[remote_prio].onLdr(self.addr, self.elid)

				except zerorpc.TimeoutExpired:
					logging.debug('[%s] id %s timeout trying to set onLdr to %s',self.addr, self.dbgcounter, remote_addr)
			logging.debug('[%s] id %s ok i am leader  notified %s',self.addr,self.dbgcounter, str(self.acks))
			#start monitoring my followers
			#logging.debug('[%s] bool(self.greenlet_Periodically) %s',self.addr,bool(self.greenlet_Periodically))
			if bool(self.greenlet_Periodically):
				logging.error('[%s] periodically was active....',self.addr)
			else:
				self.greenlet_Periodically = self.pool.spawn(self.Periodically)


	#learn that a remote node is down
	def onDownSig(self,remote_addr):
		#go to get rid of the ack here!
		if remote_addr in self.acks:
			logging.debug('[%s]removed %s from my acks here....',self.addr, remote_addr)
			self.acks.remove(remote_addr)

		#stop monitioring we know it already
		if remote_addr in self.fdlist:
			self.fdlist.remove(remote_addr)

		cond1 = self.status == self.stati["Norm"] and remote_addr == self.ldr
		cond2 = self.status == self.stati["Wait"] and self.id_by_elid(self.elid) == self.ldr

		cond3 = self.status == self.stati["Elec2"] and self.prio_by_addr(remote_addr) == self.pendack

		if cond1:
			#oh shit my leader went down while i was norm
			logging.debug('[%s] my leader went down while i was norm...  %s',self.addr, remote_addr)
		if cond2:
			#oh shit my leader went down in an election status wait set before sending ack
			logging.debug('[%s] my leader went down while i was waiting...  %s',self.addr, remote_addr)

		if cond3:
			#cant reach lower priority id while in elec2
			logging.debug('[%s] couldnt reach lower priority node %s while in elec2 ',self.addr, remote_addr)

		if cond1 or cond2:
			self.StartStage2()
		else:
			if self.status == self.stati["Elec2"] and self.prio_by_addr(remote_addr) == self.pendack:
				self.ContinStage2()
			else:
				logging.debug('[%s] downsig from %s ignored... ',self.addr, remote_addr)



	def onLdr(self, remote_addr, elid):
		# accept leader here from a node that sucessfully fininished ContinStage2, my status has
		# been set to wait in Halting
		if self.status == self.stati["Wait"] and elid == self.elid:
			old_ldr = self.ldr
			#TODO stop failure detector for everybody?
			self.StopFD(self.ldr)
			self.ldr = remote_addr
			#ok kill the periodically greenlet if i we were leader before
			if bool(self.greenlet_Periodically):
				if old_ldr != self.addr:
					logging.error('[%s] ERROR periodically was active!!! but oldldr was ',self.addr, old_ldr)
				self.greenlet_Periodically.kill()
			self.status = self.stati["Norm"]
			self.StartFD(self.ldr)
			logging.debug('[%s] accepted Leader from %s now in normal', self.addr, remote_addr)
		else:
			if self.status != self.stati["Wait"]:
				logging.debug('[%s] dont accept  %s as  my leader my status is %s', self.addr, remote_addr,self.status_by_nr(self.status) )

			if elid  != self.elid:
				logging.debug('[%s] dont accept  %s as  my leader my elid %s and you offered %s ', self.addr, remote_addr, self.elid, elid)


	def StartStage2(self):
		#starting a new election so kill our Periodically
		if bool(self.greenlet_Periodically):
			self.greenlet_Periodically.kill()

		logging.debug('[%s] starting stage2', self.addr)
		#accept requests from everybody with lower priority id
		self.PlayAlive(self.greater)
		#argh have to use string here zerorpc doesnt handle lists correctly!
		self.elid = self.addr+","+str(self.incarn)+","+str(self.nextel)
		self.nextel +=1

		self.status = self.stati["Elec2"]
		self.acks = set()
		self.pendack = self.i
		self.ContinStage2()


	def onRecovery(self):
		self.incarn +=1
		self.StartStage2()


	 #############Periodical functions######################################

	#i am the leader so i will periodically call my followers
	def Periodically(self):
		while True:
			gevent.sleep(20)
			logging.debug("[%s] Periodically running status %s ldr %s", self.addr, self.status, self.ldr)
			if self.status == self.stati["Norm"] and self.ldr == self.addr:
				logging.debug("[%s] greater than me (lower prios)", self.greater)
				for remote_addr in self.greater:
					remote_prio = self.prio_by_addr(remote_addr)
					try:
						logging.debug('[%s] call onNorm  to %s with elid %s',self.addr, remote_addr, str(self.elid))
						if self.proxy_addr:
							self.proxy_conn.onNorm(self.addr, remote_addr, self.addr, self.elid)
						else:
							self.connections[remote_prio].onNorm(self.addr,self.elid)
					except zerorpc.TimeoutExpired:
						logging.debug('[%s] timeout trying to call onNorm  to %s ',self.addr, self.servers[remote_prio])


	def onNorm(self, remote_addr, elid):
		remote_prio = self.prio_by_addr(remote_addr)
		remote_prio_elid = self.prio_by_addr(self.id_by_elid(elid))

		#right caller but my status is not norm, should be called by leader with higher prio
		isNotNorm1 = self.status != self.stati["Norm"] and  remote_prio < remote_prio_elid
		#status is not norm but remote norm has a higher priority so we have to start a new election!
		isNotNorm2 = self.status == self.stati["Norm"] and remote_prio_elid  <  self.prio_by_addr(self.ldr)

		if isNotNorm1:
			logging.debug('[%s] ERROR got norm from %s but my status is %s',self.addr, remote_addr, self.status_by_nr(self.status))
		if isNotNorm2:
			logging.debug('[%s] ERROR got norm from %s but my learder prio is %s lower than %s',self.addr, remote_addr, self.prio_by_addr(self.ldr),self.prio_by_addr(remote_addr) )

		if isNotNorm1 or isNotNorm2:
			if self.proxy_addr:
				self.proxy_conn.onNotNorm(self.addr, remote_addr, self.addr, elid)
			else:
				self.connections[remote_prio].onNotNorm(self.addr,elid)
		else:
			logging.debug('[%s] my ldr %s called tell him we are fine', self.addr, remote_addr)


	def onNotNorm(self,remote_addr, elid):
		#when i am the leader and i get a not norm message from my follower
		if self.status != self.stati["Norm"]:
			logging.debug('[%s] ERROR %s reported not norm but my status is %s',self.addr, remote_addr, self.status_by_nr(self.status))
		if self.ldr != self.addr:
			logging.debug('[%s] ERROR %s reported not norm but my leader is %s ',self.addr, remote_addr, self.ldr)
		if self.elid != self.elid:
			logging.debug('[%s] ERROR %s reported not norm but my elid is %s ',self.addr, remote_addr, self.elid)

		#a follower doesnt feel normal so lets go to a new election
		if self.status == self.stati["Norm"] and self.ldr == self.addr  and self.elid == elid:
			logging.debug('[%s] got a not norm from %s starting new election ',self.addr, remote_addr)
			self.StartStage2()


	def start(self):
		self._cleanup = self.cleanup
		atexit.register(self._cleanup)
		self.pool = gevent.pool.Group()
		self.greenlet_recovery = self.pool.spawn(self.onRecovery)
		self.greenlet_reportstatus = self.pool.spawn(self.reportstatus)
		self.greenlet_FD = self.pool.spawn(self.FD)

		#create greenlet but don't start it
		self.greenlet_Periodically = gevent.Greenlet(self.Periodically)



#http://stackoverflow.com/questions/4547277/python-closures-and-classes
	def cleanup(self):
		logging.debug('[%s] argggh im dying... ',self.addr)

		url = "http://"+self.logserver+"/report"
		values = {
			  'addr' : self.addr,
			  'status' : "down",
			  'coordinator' : self.ldr,
			  'groupid' : self.elid
		}
		user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
		header = { 'User-Agent' : user_agent }
		data = urllib.urlencode(values)
		req = urllib2.Request(url, data, header)  # POST request doesn't not work
		urllib2.urlopen(req)
		logging.debug('[%s] ok sent goodbye to everyone... ',self.addr)


	def reportstatus(self):
		#TODO add address of logserver here and call reportstatus only when changed
	   	while True:
			gevent.sleep(1)
			if self.status == -1:
				continue
			url = "http://"+self.logserver+"/report"
			values = {
				 'addr' : self.addr,
				 'status' : self.status_by_nr(self.status),
				 'coordinator' : self.ldr,
				 'groupid' : self.elid
			}
			self.sendreqquest(url,values)


	def sendreqquest(self,url,values):
		user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
		header = { 'User-Agent' : user_agent }
		data = urllib.urlencode(values)
		req = urllib2.Request(url, data, header)  # POST request doesn't not work
		urllib2.urlopen(req)


if __name__ == '__main__':
		# Set up a specific logger with our desired output level

	my_logger = logging.getLogger('MyLogger')
	my_logger.setLevel(logging.DEBUG)

	http_handler = logging.handlers.HTTPHandler(
		'localhost:4711',
		'/',
		method='POST',
	)
	my_logger.addHandler(http_handler)
	logging = my_logger

	addr = sys.argv[1]

	logserver =""
	proxyserver =""
	if len(sys.argv) > 2:
		logserver =  sys.argv[2]

	if logserver =="":
		print "no logserver using default localhost:4711"
		logserver="localhost:4711"

	if len(sys.argv) > 3:
		proxyserver =  sys.argv[3]

	if proxyserver =="":
		print "no proxyserver using default localhost:4711"
		proxyserver="localhost:4712"

	#todo a little hacky
	tmp = addr.replace(".","")
	tmp = tmp.replace(":","")
	counterpath = tmp+".txt"
	counter = "0"
	if os.path.isfile(counterpath):
		f = open(counterpath)
		tmp = f.read()
		if tmp !="":
			counter = tmp

	counter =int(counter)+1
	f = open(counterpath,"w")
	f.write(str(counter))
	f.close()

	ne = NullElection(addr, logserver, proxyserver, counter, config_file="server_config")

	s = zerorpc.Server(ne)

	s.bind('tcp://' + addr)
	print "listening on addr",addr
	ne.start()
	# Start server
	logging.debug('%s Starting ZeroRPC Server' % addr)
	s.run()
