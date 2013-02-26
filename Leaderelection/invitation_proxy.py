__author__ = 'schello'
__author__ = 'schello'
import logging
import logging.handlers
import sys
import zerorpc
import time
import gevent
import ast



class NullElection(object):
	'''
		Line 182:                     self.connections[i].invite(remote_addr, group_id)
		   Line 190:             self.connections[remote_prio].accept(self.addr, group_id)
		   Line 218:                 self.connections[i].invite(self.addr, self.group)
		   Line 228:                 self.connections[i].invite(self.addr, self.group)
		   Line 243:                 self.connections[i].ready(self.addr, self.group, self.task_description)
		   Line 271:                         is_coordinator = self.connections[i].are_you_coordinator(self.addr)
		   Line 330:                     is_there = self.connections[coord_prio].are_you_there(self.addr, my_coordinator, my_group)
		'''

	def __init__(self, addr, config_file='server_config'):
		self.servers = []
		self.connections = []
		self.addr = addr
		self.blocklist = []
		f = open(config_file, 'r')
		for line in f.readlines():
			line = line.rstrip()
			self.servers.append(line)

		print 'My addr: %s' % (self.addr)
		print 'Server list: %s' % (str(self.servers))
		self.n = len(self.servers)
		for i, server in enumerate(self.servers):
			c = zerorpc.Client(timeout=10)
			c.connect('tcp://' + server)
			self.connections.append(c)


	def setblocklist(self,blocklist):

		print "blocklist received...",type(blocklist)
		self.blocklist = []
		for t in blocklist:
			x = ast.literal_eval(t)
			print x
			self.blocklist.append(x)

	def isblocked(self, addr_caller, remote_addr):
		#todo not very pythonic loock for list comp if time
		for t in self.blocklist:
			if addr_caller == t[0] and remote_addr == t[1] or addr_caller == t[1] and remote_addr == t[0]:
				print ">>>>>>>>>>>>blocked access from ",t[0],"to",t[1]
				return True
		return False

	def prio_by_addr(self, addr):
		for i, server in enumerate(self.servers):
			#todo check proxy stuff get the last two digits only this ones are interesting
			if server == addr:
				return i
		logging.error('[%s] prio_by_addr couldnt find prio for %s!', self.addr, addr)
		raise Exception("prio_by_addr couldnt find prio for %s" % addr)


	def are_you_coordinator(self, addr_caller, remote_addr, p_addr_caller):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return

		logging.debug("called coord")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].are_you_coordinator(p_addr_caller)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
			gevent.sleep(20)
			return

	def	are_you_there(self, addr_caller, remote_addr, p_addr, p_my_coordinator, p_my_group):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return


		logging.debug("are_you_there....")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].are_you_there(p_addr, p_my_coordinator, p_my_group)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
			gevent.sleep(20)
			return

	def invite(self, addr_caller, remote_addr, p_addr, p_group):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return
		logging.debug("invite called....")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].invite(p_addr, p_group)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
			gevent.sleep(20)
			return

	def ready(self, addr_caller, remote_addr, p_addr, p_group, p_description):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return
		logging.debug("ready called ready ....")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].ready(p_addr, p_group, p_description)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
			gevent.sleep(20)
			return

	def accept(self, addr_caller, remote_addr, p_addr, p_group_id):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return
		logging.debug("ready called ready ....")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].accept(p_addr, p_group_id)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling accept ', self.addr)
			gevent.sleep(20)
			return



if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG)
	addr =""
	if len(sys.argv) > 1:
		addr = sys.argv[1]
	if addr == "":
		addr = "127.0.0.1:4712"

	ne = NullElection(addr, config_file="server_config")
	s = zerorpc.Server(ne)
	print "addr", addr

	s.bind('tcp://' + addr)
	#ne.start()
	# Start server
	logging.debug('[%s] Starting ZeroRPC Server' % addr)
	s.run()