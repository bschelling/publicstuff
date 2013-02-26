__author__ = 'schello'
import sys
import os
import signal


def kill_servers(single_server):
	f = open("running_servers.log", 'r')
	for line in f.readlines():
		line = line.rstrip()
		server_pid = line.split("\t")
		#print "single","X"+single_server+"X","server_pid[0]", "X"+server_pid[0]+"X"
		if single_server != 0:
			if single_server == server_pid[0]:
				os.kill(int(server_pid[1]), signal.SIGINT)
				print "killing",server_pid[1],server_pid[0]
				return
			else:
				print "not right..."
		else:
			print "killing",server_pid[1],server_pid[0]
			os.kill(int(server_pid[1]), signal.SIGINT)

	print "done"



if __name__ == '__main__':
	single_server = 0
	if len(sys.argv) > 1:
		single_server = sys.argv[1]
	kill_servers(single_server)




