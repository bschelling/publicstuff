__author__ = 'schello'
import logging
import logging.handlers
import sys
import gevent
import zerorpc
import time
import urllib
import urllib2
import atexit
import os

''''
	problem: higher node calls merge first while lower priority node is sleeping in check (sell line
	while sleeping in check at the same time the node accepts the higher priority node to be its coordinator
	lower priority in check function wakes up STILL believing it is coordinator and calls merge -
	now lower priority node calls invite to higher priority mode
	higher priority node has lower priority mode in self up and is coordinator of lower priority node
	so it will forward the invitation to lower priority node!
	lower priority mode is in state election and will deny the request but still.....

	added check for if we ar still coordinator if we wake up after sleep in merge

	in timeout function check if status is normal missing?

'''''



class NullElection(object):
	def __init__(self, addr, logserver, proxyserver, counter, config_file="server_config"):

		self.counter = counter
		self.group = ""
		self.proxy_addr = proxyserver
		self.logserver = logserver

		self.addr = addr
		self.connections = []
		self.servers = []
		#all possible states checked here
		self.stati = {"Down": 0, "Election": 1, "Reorganisation": 2, "Normal": 3}


		#the coordinator of the current node
		self.coordinator = 0
		#TODO the definition of the task we will perform, list of participating nodes...
		self.definition = ""
		#TODO set the priority, this will never change so we can set it on init, use
		self.priority = 0
		#id of last node that caused us to halt
		self.haltnode = ""
		self.task_description = ""
		#this timeout is set to the last time we heard from the coordinator
		self.lastcontact = time.time()
		self.up = set()

		f = open(config_file, 'r')
		for line in f.readlines():
			line = line.rstrip()
			self.servers.append(line)

		print 'My addr: %s' % (self.addr)
		print 'Server list: %s' % (str(self.servers))
		print 'Logserver: %s' % (str(self.logserver))
		print 'Proxyserver: %s' % (str(self.proxy_addr))



		self.n = len(self.servers)

		if self.proxy_addr != "":
			self.proxy_conn = zerorpc.Client(timeout=10)
			self.proxy_conn.connect('tcp://' +self.proxy_addr)

		for i, server in enumerate(self.servers):
			if server == self.addr:
				self.i = i
				self.connections.append(self)
			else:
				if self.proxy_addr == "":
					c = zerorpc.Client(timeout=10)
					c.connect('tcp://' + server)
					self.connections.append(c)

		#set self status to undefined (-1) so that others lern we're not normal!
		self.status = -1



	#todo better datastructure to deal with prios and addresses?
	def prio_by_addr(self, addr):
		for i, server in enumerate(self.servers):
			#todo check proxy stuff get the last two digits only this ones are interesting
			if server[-2:] == addr[-2:]:
				return i
		logging.error('[%s] prio_by_addr couldnt find prio for %s!', self.addr, addr)
		raise Exception("prio_by_addr couldnt find prio for %s" % addr)


	#helper function to get human readable status
	def status_by_nr(self,status):
		return  self.stati.keys()[self.stati.values().index(status)]

	def set_coordinator(self, remote_addr):
		self.coordinator = remote_addr

	def  are_you_there(self, remote_addr, remote_addr_coord,  group_id):

		#used by remote nodes to see if we are the coordinator of group group_id and we think that remote_addr
		#is a member
		if self.group != group_id:
			logging.debug('[%s] are_you_there called by %s, tell him sorry mygroup %s is not your group %s', self.addr, remote_addr, self.group, group_id)
			return False

		# have to translate message here if i want to compare coordinator
		# if i am 9001 9000 will have me as 9101

		if self.coordinator != remote_addr_coord:
			logging.debug('[%s] are_you_there called by %s, tell him sorry my coordinator is %s and not %s', self.addr, remote_addr, self.coordinator,  remote_addr_coord)
			return False

		remote_prio = self.prio_by_addr(remote_addr)

		if remote_prio not in self.up:
			logging.debug('[%s] are_you_there called by %s, tell him sorry my up is %s but your prio is %s', self.addr, remote_addr, str(self.up), remote_prio)
			return False

		logging.debug('[%s] are_you_there() called by %s yes i am indeed', self.addr, remote_addr)
		return True

	def are_you_coordinator(self,remote_addr):
		#called by check to see if we are a coordinator
		is_coordinator = True if self.status == self.stati["Normal"] and self.coordinator == self.addr else False
		logging.debug('[%s] are_you_coordinator called by %s my answer %s my coordinator %s and my status %s', self.addr, remote_addr, is_coordinator, self.coordinator, self.status_by_nr(self.status))
		return is_coordinator


	def accept(self, remote_addr, group_id):
		#remote node remoteaddr accepts our invitation to join our group with group nr group_nr

		#can't accept when i'm not in state election
		if self.status != self.stati["Election"]:
			logging.debug('[%s] can not accept my state is %s ', self.addr, self.status)
			return

		#group number wrong
		if self.group != group_id:
			logging.debug('[%s] can not accept my group number is %s, proposed group number was ', self.addr, self.group, group_id)
			return

		#i am not the coordinator of my group, i can't accept anyone!
		if self.coordinator != self.addr:
			logging.debug('[%s] can not accept my coordinator is  %s', self.addr, self.coordinator)
			return

		remote_prio = self.prio_by_addr(remote_addr)
		self.up.add(remote_prio)
		logging.debug('[%s] accepted  %s in my group %s', self.addr, remote_addr, group_id)

	def invite(self, remote_addr, group_id):
		#called by merge, remote_addr invites us to join group with groupid
		if self.status != self.stati["Normal"]:
			logging.debug('[%s] DECCLINING invitation from %s with group_id %s my status %s', self.addr, remote_addr, group_id, self.status_by_nr(self.status))

			return
		logging.debug('[%s] received invitation while normal from %s calling stop now', self.addr, remote_addr)
		#here i kill my check greenlet
		self.stop(True)
		tempcoord = self.coordinator
		#todo check that self up contains priorities! why do we need tempset instead of self.up here?
		tempset = self.up
		self.status = self.stati["Election"]

		self.coordinator = remote_addr
		self.timeout_greenlet = self.pool.spawn(self.timeout)

		logging.debug('[%s] function invite: invitation accepted remote cooordinator %s',self.addr,remote_addr)
		self.group = group_id

		#if i was coordinator before, send invitation out to my group memebers

		if tempcoord == self.addr:
			for i in tempset:
				print tempset
				try:
					logging.debug('[%s] invite uplist member %s to join with group_id %s and coordinator %s', self.addr, self.servers[i], group_id, remote_addr)
					if self.proxy_addr:
						self.proxy_conn.invite(self.addr,remote_addr, remote_addr, group_id)
					else:
						self.connections[i].invite(remote_addr, group_id)

				except zerorpc.TimeoutExpired:
					logging.debug('[%s] inviting %s, timeout', self.addr, self.servers[i])

		remote_prio = self.prio_by_addr(remote_addr)
		logging.debug('[%s] function invite calling accept on remote_prio %s, ', self.addr, remote_prio)
		try:
			if self.proxy_addr:
				self.proxy_conn.accept(self.addr, remote_addr, self.addr, group_id)
			else:
				self.connections[remote_prio].accept(self.addr, group_id)

		except zerorpc.TimeoutExpired:
			logging.debug('[%s] sending accept to %s, timeout - entering recovery!', self.addr,remote_addr)
			self.recovery()
			return

		self.status = self.stati["Reorganisation"]


	def merge(self,coordinatorset):
		#try to form a new group with node i as coordinator and invite other coordinators from the tempset
		# do not disturb as long as we are in election we will not accept invitations from other nodes
		self.status = self.stati["Election"]
		#kill timeout greenlet but do not kill myself, i was called by check
		self.stop(False)

		self.counter +=1
		self.group = self.addr +":"+ str(self.counter)
		self.coordinator = self.addr
		tempset = self.up
		self.up = set()

		#call out for every coordinator out there to join me
		for i in coordinatorset:
			try:
				logging.debug('[%s] inviting  %s ', self.addr, self.servers[i])
				remote_addr = self.servers[i]
				if self.proxy_addr:
					self.proxy_conn.invite(self.addr,remote_addr,self.addr, self.group)
				else:
					self.connections[i].invite(self.addr, self.group)

			except zerorpc.TimeoutExpired:
				logging.debug('[%s] inviting %s, timeout', self.addr, self.servers[i])

		#since we have a new groupid now tell my followers to join the new group we just created
		for i in tempset:
			try:
				logging.debug('[%s] inviting  %s ', self.addr, self.servers[i])
				remote_addr = self.servers[i]
				if self.proxy_addr:
					self.proxy_conn.invite(self.addr,remote_addr,self.addr, self.group)
				else:
					self.connections[i].invite(self.addr, self.group)

			except zerorpc.TimeoutExpired:
				logging.debug('[%s] inviting %s, timeout', self.addr, self.servers[i])

		#Wait some time to give others the option to accept our invitation
		logging.debug('[%s] waiting for others to accept the invitation ', self.addr)
		gevent.sleep(5)
		self.status = self.stati["Reorganisation"]
		#todo do some reorganization work and distribute the task description to everybody in my up list
		#uplist is build up in accept!


		for i in self.up:
			try:
				remote_addr = self.servers[i]
				if self.proxy_addr:
					self.proxy_conn.ready(self.addr,remote_addr, self.addr, self.group, self.task_description)
				else:
					self.connections[i].ready(self.addr, self.group, self.task_description)

				logging.debug('[%s] calling ready on %s  ', self.addr, self.servers[i])
			except zerorpc.TimeoutExpired:
				logging.debug('[%s] calling ready %s, timeout - calling recovery', self.addr, self.servers[i])
				#TODO 10 call recovery here on the node in question!! not on myself!!!
				#not self.recovery()

		self.status = self.stati["Normal"]



	def check(self):
		#if i'm the coordinator and someone answers not normal we will have a new election
		# my state can change! so keep true in outer loop
		while True:
			gevent.sleep(3)
			if self.status == self.stati["Normal"] and self.coordinator == self.addr:
				logging.debug('[%s] im the coordinator, checking others', self.addr)
				gevent.sleep(1)
				tempset = set()
				#call any other node but myself to see if they are coordinators
				for i, server in enumerate(self.servers):
					if i == self.i:
						continue
					try:
						#storing result from are_you_there call will be true or false
						remote_addr = self.servers[i]
						if self.proxy_addr:
							is_coordinator = self.proxy_conn.are_you_coordinator(self.addr,remote_addr,self.addr)
						else:
							is_coordinator = self.connections[i].are_you_coordinator(self.addr)
						logging.debug('[%s] are_you_coordinator %s said %s', self.addr, server, is_coordinator)
						if is_coordinator == True:
							logging.debug('[%s] adding %s as %s to tempset', self.addr, server, i)
							tempset.add(i)

					except zerorpc.TimeoutExpired:
						#if we got a timeout here we just keep on going, don't care about our followers
						logging.debug('[%s] are_you_coordinator %s said timeout', self.addr, server)


				if len(tempset) == 0:
					logging.debug('[%s] no other coordinator found, check returns', self.addr)
					continue

				p = max(tempset)
				logging.debug('[%s] max priority my tempset is %s i am %d', self.addr, p, self.i)

				#nodes with the highest priority should call merge first, so wait a reasonable time here
				if self.i < p:
					print "sleep", (p-self.i)*10
					gevent.sleep((p-self.i)*10)

				if self.coordinator == self.addr:
					logging.debug('[%s] function check still coordinator after sleep  calling merge now!', self.addr)
					self.merge(tempset)
				else:
					#!!!I'm not coordinator, restart check if i become coordinator
					return
					#todo should we return here?
					#dgb =1


	#this function is called when a node has not heard from a coordinator in a long time
	def timeout(self):
		#todo check maybe use scheduler here?
		while True:
			gevent.sleep(5)
			my_coordinator = self.coordinator
			my_group = self.group

			if self.status  != self.stati["Normal"]:
				#logging.debug('[%s] timeout no use to call if my status is %s', self.addr, self.status_by_nr(self.status))
				gevent.sleep(2)
				continue

			if my_coordinator == self.addr:
				#logging.debug('[%s] timeout idle, I am coordinator status %s', self.addr, self.status_by_nr(self.status))
				continue
			else:
				coord_prio = self.prio_by_addr(my_coordinator)

				logging.debug('[%s] lets go check my coordinator %s', self.addr, my_coordinator)
				remote_addr = self.servers[coord_prio]
				logging.debug('[%s] lets go check my coordinator %s coordprios is %s', self.addr, my_coordinator, coord_prio)
				try:
					if self.proxy_addr:
						is_there  = self.proxy_conn.are_you_there(self.addr, remote_addr, self.addr, my_coordinator, my_group)
					else:
						is_there = self.connections[coord_prio].are_you_there(self.addr, my_coordinator, my_group)
					if not is_there:
						logging.debug('[%s] starting recovery! called my coordinator %s but isthere is %s', self.addr, self.coordinator, is_there )
						#timeout will be killed so have to spawn recovery!
						self.recovery_greenlet = self.pool.spawn(self.recovery)
						return

				except zerorpc.TimeoutExpired:
					logging.debug('[%s] starting recovery! called my coordinator %s after timeout', self.addr, self.coordinator )
					#timeout will be killed so have to spawn recovery!
					self.recovery_greenlet = self.pool.spawn(self.recovery)
					return


	def ready(self, remote_addr, group_id, task_description):

		logging.debug('>>>>> %s got ready from %s', self.addr, remote_addr)
		#new task distributed from coordinator = remote_addr
		if self.coordinator != remote_addr:
			logging.debug('%s :task_description ignored from %s WTF my coordinator was %s', self.addr, remote_addr,
				self.coordinator)
		if self.status != self.stati["Reorganisation"]:
			logging.debug('%s :task_description ignored from %s WTF my status was %s', self.addr, remote_addr, self.status_by_nr(self.status))

		if self.group != group_id:
			logging.debug('%s :task_description ignored from %s WTF my group_id was %s other group %s', self.addr, remote_addr, self.group, group_id)

		self.task_description = task_description
		#set status from reorganisation to normal
		self.status = self.stati["Normal"]
		logging.debug('%s :task_description accepted from %s now my status is %s', self.addr, remote_addr, self.status_by_nr(self.status))



	def stop(self, killcheck):
		if bool(self.check_greenlet) and killcheck :
			self.check_greenlet.kill()
			logging.debug('%s : todo stop received kill check?', self.addr)
		if bool(self.timeout_greenlet):
			self.timeout_greenlet.kill()
			logging.debug('%s : stop received killed timeout', self.addr)


	def recovery(self):
		logging.debug('%s : ++++++starting recovery +++++++', self.addr)
		#called on startup OR if we get a timeout in the merge function...
		self.status = self.stati["Election"]
		#kill timeout and check greenlet
		self.stop(True)
		self.counter +=1
		#group id is increased either by recovery or by merge
		self.group = self.addr +":"+ str(self.counter)
		self.coordinator = self.addr

		self.up = set()
		self.status = self.stati["Reorganisation"]
		#task description check
		self.task_description = "Do something"
		self.status = self.stati["Normal"]
		logging.debug('%s : recovery finished, self.status is %s self.coordinator is %s', self.addr, self.status_by_nr(self.status), self.coordinator)


		if bool(self.check_greenlet):
			logging.debug('%s : ++++++++++self.check_greenlet was running check why++++++++', self.addr, self.status_by_nr(self.status), self.coordinator)
		#restarting check
		logging.debug('%s : ++++++++++self.check_greenlet restarting++++++++', self.addr)

		if not bool(self.check_greenlet):
			self.check_greenlet = self.pool.spawn(self.check)


	#http://stackoverflow.com/questions/4547277/python-closures-and-classes
	def cleanup(self):
		#update counter here
		print "self addr..."+self.addr
		print "You are now leaving the Python sector."
		url = "http://"+self.logserver+"/report"
		values = {
			  'addr' : self.addr,
			  'status' : "down",
			  'coordinator' : self.coordinator,
			  'groupid' : self.group
		}
		user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
		header = { 'User-Agent' : user_agent }
		data = urllib.urlencode(values)
		req = urllib2.Request(url, data, header)  # POST request doesn't not work
		urllib2.urlopen(req)


	def reportstatus(self):
	   	while True:
			gevent.sleep(1)
			if self.status == -1:
				continue
			url = "http://"+self.logserver+"/report"
			values = {
				 'addr' : self.addr,
				 'status' : self.status_by_nr(self.status),
				 'coordinator' : self.coordinator,
				 'groupid' : self.group
			}
			self.sendreqquest(url,values)


	def sendreqquest(self,url,values):
		user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
		header = { 'User-Agent' : user_agent }
		data = urllib.urlencode(values)
		req = urllib2.Request(url, data, header)  # POST request doesn't not work
		urllib2.urlopen(req)

	def start(self):
		self._cleanup = self.cleanup
		atexit.register(self._cleanup)
		self.pool = gevent.pool.Group()
		#don't start the greenlets here, check will only be run if we are coordinator, timeout only if we are not coordinator anymore
		self.timeout_greenlet = gevent.Greenlet(self.check)
		self.check_greenlet = gevent.Greenlet(self.check)
		self.recovery_greenlet = self.pool.spawn(self.recovery)
		self.report_greenlet = self.pool.spawn(self.reportstatus)

if __name__ == '__main__':

	addr = sys.argv[1]

	logserver =""
	proxyserver =""
	addr =  sys.argv[1]
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

	my_logger = logging.getLogger('MyLogger')
	my_logger.setLevel(logging.DEBUG)

	http_handler = logging.handlers.HTTPHandler(
		'localhost:4711',
		'/',
		method='POST',
	)
	my_logger.addHandler(http_handler)

	#logging.basicConfig(level=logging.DEBUG)
	logging = my_logger


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
	print "addr", addr
	s.bind('tcp://' + addr)
	ne.start()
	# Start server
	logging.debug('[%s] Starting ZeroRPC Server' % addr)
	s.run()
