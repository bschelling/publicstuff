import logging
import logging.handlers
import sys
import gevent
import zerorpc
import time
import greenlet
import inspect
from gevent import monkey
# patches stdlib (including socket and ssl modules) to cooperate with other greenlets
monkey.patch_all()
import urllib2
import urllib
import atexit

''' if running over proxy make only our proxy address visible to others!'''
''' TODO use named tuples('ProposalID', ['number', 'uid'])'''



class NullElection(object):
	def __init__(self, addr, logserver, config_file='server_config'):
		self.addr = addr
		self.logserver = logserver
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

		f = open(config_file, 'r')
		for line in f.readlines():
			line = line.rstrip()
			self.servers.append(line)

		print 'My addr: %s' % (self.addr)
		print 'Server list: %s' % (str(self.servers))
		print 'Server list: %s' % (str(self.servers))
		print 'My logserver: %s' % (self.logserver)

		self.n = len(self.servers)
		#TODO we are running on port 90XX but our proxy address is 91XX
		#self.addr = self.addr.replace("90","91")
		for i, server in enumerate(self.servers):
			if server == self.addr:
				self.i = i
				self.connections.append(self)
			else:
				c = zerorpc.Client(timeout=10)
				c.connect('tcp://' + server)
				self.connections.append(c)

		#set self status to undefined (-1) so that others lern we're not normal!
		self.status = -1
		if self.i == -1:
			raise Exception("ip mismatch")


	#helper function to get human readable status
	def status_by_nr(self,status):
		return  self.stati.keys()[self.stati.values().index(status)]


	def set_coordinator(self, remote_addr):
		self.coordinator = remote_addr

	def prio_by_addr(self, addr):
			for i, server in enumerate(self.servers):
				print server,addr
				if server == addr:
					return i
			logging.error('%s prio_by_addr couldnt find prio for %s!', self.addr, addr)


	def are_you_normal(self, remote_addr):
	#if we heard from our coordinator reset the timeout
		if remote_addr == self.coordinator:
			self.lastcontact = time.time()
			normal = True if self.status == self.stati["Normal"] else False
			my_logger.debug('%s are_you_normal called by %s, tell him %s', self.addr, remote_addr, normal)
			return normal


	def are_you_there(self, remote_addr):
		#used by remote nodes to see if this node is operating, nothing actually to do at the node itself
		my_logger.debug('%s are_you_there() called by %s', self.addr, remote_addr)
		return True


	def check(self):
		#if i'm the coordinator and someone answers not normal we will have a new election
		# my state can change! so keep true in outer loop
		while True:
			gevent.sleep(1)
			if self.status == self.stati["Normal"] and self.coordinator == self.addr:
				my_logger.debug('%s im the coordinator, checking others', self.addr)
				gevent.sleep(1)
				for i, server in enumerate(self.servers):
					if i != self.i:
						try:
							#storing result from are_you_there call will be true or false
							result = self.connections[i].are_you_normal(self.addr)
							my_logger.debug('%s are_you_normal %s said %s', self.addr, server, result)
							if result == False:
								my_logger.debug('%s starting election', self.addr)
								self.election()
								return

						except zerorpc.TimeoutExpired:
							my_logger.debug('%s are_you_normal %s said timeout', self.addr, server)


	#this function is called when a node has not heard from a coordinator in a long time
	def timeout(self):
		#todo check maybe use scheduler here?
		while True:
			gevent.sleep(10)
			#this only makes sense if we are not coordinator ourselves!
			if self.coordinator != self.addr:
				if self.status == self.stati["Normal"] or self.status == self.stati["Reorganisation"]:
						elapsedTime = time.time() - self.lastcontact
						if elapsedTime > 10:
							prio_coord = self.prio_by_addr(self.coordinator)
							try:
								self.connections[prio_coord].are_you_normal(self.addr)
								my_logger.debug('%s called my coordinator %s after timeout %d', self.addr, self.coordinator, elapsedTime )
							except zerorpc.TimeoutExpired:
								my_logger.debug('%s starting election! called my coordinator %s after timeout %d', self.addr, self.coordinator, elapsedTime )
								self.election()

				else:
					#my status is not normal, call election?
					dummy =1
					my_logger.debug('%s : have not heard from calling election', self.addr)
					self.election()


	def ready(self, remote_addr, task_description):
		#new task distributed from coordinator = remote_addr
		if self.coordinator != remote_addr:
			my_logger.debug('%s :task_description ignored from %s WTF my coordinator was %s', self.addr, remote_addr,
				self.coordinator)
		if self.status != self.stati["Reorganisation"]:
			my_logger.debug('%s :task_description ignored from %s WTF my status was %s', self.addr, remote_addr, self.status)

		self.task_description = task_description
		#set status from reorganisation to normal
		self.status = self.stati["Normal"]
		my_logger.debug('%s :task_description accepted from %s now my status is %s', self.addr, remote_addr, self.status)
		#now that we have a new coordinator, watch him

		#http://greenlet.readthedocs.org/en/latest/
		if bool(self.grlt_timeout):
			my_logger.debug('%s :killing timeout greenlet', self.addr)
			self.grlt_timeout.kill()
			gevent.sleep(10)

		my_logger.debug('%s :restarting timeout greenlet', self.addr)
		self.lastcontact = time.time()
		self.grlt_timeout = self.pool.spawn(self.timeout)




	def halt(self, remote_addr):
		#halt all current processes, todo check greenlet kill, which greenlets have to be killed?
		my_logger.debug('%s : halt received from %s', self.addr, remote_addr)
		#set status to Election
		self.status = self.stati["Election"]
		#set haltnode to remote addr
		self.haltnode = remote_addr
		self.stop()


	def stop(self):
		my_logger.debug('%s : stop received', self.addr)



		''' TODO When this statement is executed by a procedure p we should
			stop the execution of all processes, allow processes to finish, for the bully algorithm
			 it should be sufficient to reset the timeout for the timeout function'''


	def new_coordinator(self, remote_addr):
		#inform all the nodes (except for ourselves) of the new coordinator
		#check if the caller really was the node that halted us and whether the state
		#is election
		if self.haltnode != remote_addr:
			my_logger.debug('%s :new_coordinator ignored from %s WTF my haltnode was %s', self.addr, remote_addr,
				self.haltnode)
			#todo should we check for something here?
			return
		elif self.status != self.stati["Election"]:
			my_logger.debug('%s :new_coordinator ignored from %s WTF my status was %s', self.addr, remote_addr, self.status)
			return

		self.coordinator = remote_addr

		#set status to organisation
		self.status = self.stati["Reorganisation"]
		my_logger.debug('%s :new_coordinator accepted from %s now in status %s', self.addr, remote_addr, self.status)


	def election(self):
		my_logger.debug('%s : >>>>>>> new election ', self.addr)
		#check if any higher nodes are up, if so return
		gevent.sleep(1)
		for i, server in enumerate(self.servers):
			try:
				if i > self.i:
					my_logger.debug('%s : election calling are_you_there to %s', self.addr, self.servers[i])
					result = self.connections[i].are_you_there(self.addr)
					#found a node with higher priority
					if result == True:
						my_logger.debug('%s :  answer to are_you_there from higher priority %s p %d',self.addr, server, i)
						return

			except zerorpc.TimeoutExpired:
				my_logger.debug('%s : are_you_there timeout from %s', self.addr, server)

		my_logger.debug('%s : i am coordinator', self.addr)
		self.status = self.stati["Election"]
		self.haltnode = self.addr
		self.up = []


		# send halt to all lower priority nodes
		for i in range(0, self.i):
			try:
				self.connections[i].halt(self.addr)
				#todo is it ok to keep the connection in self.up?
				my_logger.debug('%s : adding server %s to up list', self.addr, self.servers[i])
				self.up.append(self.servers[i])


			#todo check for answer in halt message
			except zerorpc.TimeoutExpired:
				my_logger.debug('%s : halt timeout from %s', self.addr, self.servers[i])

		#set myself to coordinator
		self.set_coordinator(self.addr)
		#set status to reorganisation
		self.status = self.stati["Reorganisation"]

		#give notice to the connections in self.up
		for server in self.up:
			my_logger.debug('%s : send new coordinator ', self.addr)
			try:
				prio = self.prio_by_addr(server)
				self.connections[prio].new_coordinator(self.addr)
			except zerorpc.TimeoutExpired:
				my_logger.debug('%s : TIMEOUT trying to distribute new coordinator to  %s', self.addr, server)

		task_description = "whatever"
		#todo check if self.up has changed! give notice to the connections in self.up
		for server in self.up:
			my_logger.debug('%s : set status of my followers and distribute tasklist here ', self.addr)
			try:
				prio = self.prio_by_addr(server)
				self.connections[prio].ready(self.addr, task_description)
			except zerorpc.TimeoutExpired:
				my_logger.debug('%s : TIMEOUT trying to trying to distribute task to %s',self.addr, server)

		#set status to normal
		self.status = self.stati["Normal"]
		my_logger.debug('%s : now everybody should be listening to me', self.addr)


	def recovery(self):
		self.haltnode = 0
		my_logger.debug('%s : calling election called from %s', self.addr, inspect.stack()[1][3])
		self.election()


	def start(self):
		self._cleanup = self.cleanup
		atexit.register(self._cleanup)
		self.pool = gevent.pool.Group()
		#create greenlet here, only start it if we have an coordinator assigned
		self.grlt_timeout = gevent.Greenlet(self.timeout)
		#We need a greenlet for recovery because otherwise we can't listen until recovery has finished!
		self.grlt_recovery = self.pool.spawn(self.recovery)
		self.grlt_check = self.pool.spawn(self.check)
		self.grlt_reportstatus = gevent.spawn(self.reportstatus)

	def end(self):
		atexit.unregister(self._cleanup)

	#http://stackoverflow.com/questions/4547277/python-closures-and-classes
	def cleanup(self):
		print "self addr..."+self.addr
		print "You are now leaving the Python sector."
		url = "http://"+self.logserver +"/report"
		values = {
			  'addr' : self.addr,
			  'status' : "down",
			  'coordinator' : self.coordinator,
			  'groupid' : 0

		}
		user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
		header = { 'User-Agent' : user_agent }
		data = urllib.urlencode(values)
		req = urllib2.Request(url, data, header)  # POST request doesn't not work
		urllib2.urlopen(req)


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
			 'coordinator' : self.coordinator,
			 'groupid' : 0
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
	logserver =""
	addr =  sys.argv[1]
	if len(sys.argv) > 2:
		logserver =  sys.argv[2]

	if logserver =="":
		print "no logserver using default localhost:4711"
		logserver="localhost:4711"

	http_handler = logging.handlers.HTTPHandler(
		logserver,
		'/',
		method='POST',
	)
	my_logger.addHandler(http_handler)
	ne = NullElection(addr, logserver)
	atexit.register(ne)
	s = zerorpc.Server(ne)
	print "addr", addr
	s.bind('tcp://' + addr)
	ne.start()
	# Start server
	my_logger.debug('%s Starting ZeroRPC Server' % addr)
	s.run()
