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
		print "blocklist received...",type(blocklist), len(blocklist)
		self.blocklist = []
		for t in blocklist:
			x = ast.literal_eval(t)
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
			if server[-2:] == addr[-2:]:
				return i
		logging.error('[%s] prio_by_addr couldnt find prio for %s!', self.addr, addr)
		raise Exception("prio_by_addr couldnt find prio for %s" % addr)


	def OnReportStatusToFD(self, addr_caller, remote_addr, p_addr_caller):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return
		logging.debug("called coord")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].OnReportStatusToFD(p_addr_caller)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
			gevent.sleep(20)
			return

	def onReject(self, addr_caller, remote_addr, p_remote_addr, p_elid):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return
		logging.debug("called coord")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].onReject(p_remote_addr, p_elid)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
			gevent.sleep(20)
			return

	def onAck(self, addr_caller, remote_addr, p_addr, p_elid):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return
		logging.debug("called coord")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].onAck(p_addr, p_elid)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
			gevent.sleep(20)
			return


	def onHalt(self, addr_caller, remote_addr, p_addr, p_elid):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return
		logging.debug("called coord")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].onHalt(p_addr, p_elid)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
			gevent.sleep(20)
			return


	def onLdr(self, addr_caller, remote_addr, p_addr, p_elid):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return
		logging.debug("called coord")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].onLdr(p_addr, p_elid)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
			gevent.sleep(20)
			return

	def onNorm(self, addr_caller, remote_addr, p_addr, p_elid):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return
		logging.debug("called coord")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].onNorm(p_addr, p_elid)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
			gevent.sleep(20)
			return

	def onNotNorm(self, addr_caller, remote_addr, p_addr, p_elid):
		if self.isblocked(addr_caller, remote_addr):
			gevent.sleep(20)
			return
		logging.debug("called coord")
		remote_prio = self.prio_by_addr(remote_addr)
		try:
			return self.connections[remote_prio].onNotNorm(p_addr, p_elid)
		except zerorpc.TimeoutExpired:
			logging.error('[%s] Timeout calling ', self.addr)
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