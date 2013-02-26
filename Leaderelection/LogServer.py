#!/usr/bin/python
"""WSGI server example"""
from gevent import wsgi
from cgi import FieldStorage
import time
import logging
from gevent import monkey; monkey.patch_all()
from pymongo import MongoClient
import datetime
import zerorpc
import sys

#http://compbio.cs.toronto.edu/repos/snowflock/xen-3.0.3/tools/python/logging/logging-0.4.9.2/test/logrecv.py
def makeDict(fs):
		dict = {}
		for mfs in fs.list:
			dict[mfs.name] = mfs.value
		for key in ["args", "exc_info", "exc_text", "lineno", "msecs", "created",
					"thread", "levelno", "relativeCreated"]:
			if dict.has_key(key):
				dict[key] = eval(dict[key])
		return dict

def sendblocklist(blist):
	#TODO CLEAN THAT
	c = zerorpc.Client(timeout=10)
	global server
	c.connect('tcp://' + server)
	try:
		c.setblocklist(blist)
	except zerorpc.TimeoutExpired:
		print ">>>>>>>>>>timeout sending to proxy"



def startServer(env, start_response):
	start_response('200 OK', [('Content-Type', 'text/html')])

	fs = FieldStorage(
		fp=env['wsgi.input'],
		environ=env,
		keep_blank_values=True
	)

	#go away we have no favicon
	if env['PATH_INFO'] == '/favicon.ico':
		return ["Hello World!\r\n"]

	#get info from working nodes here and show
	if env['PATH_INFO'] == '/status':
		doc ="<pre>"
		blocklist = fs.getvalue("blocklist")
		print blocklist
		if blocklist =="1":
			doc +="Resetting blocklist<hr>"
			sendblocklist([])
		else:
			if type(blocklist) is list:
				doc +="Blocking:"+str(blocklist) +"<hr>"
				sendblocklist(blocklist)

		global connection
		db = connection['bully']
		rows = db.bully.find()
		doc +="addr\tcoordinator\tgroupid\tstatus\tdate\r\n"
		items = []
		for row in rows:
			items.append(str(row["addr"]))
			#doc = doc + str(row) +"\r\n"
			time = row["date"].strftime("%H:%M:%S:%f")
			doc += row["addr"] + "\t"+ row["coordinator"]+"\t" +row["groupid"] +"\t"+row["status"]+"\t"+ str(time) +"<br>"

		doc += "<hr>"
		combinations = [(items[i],items[j]) for i in range(len(items)) for j in range(i+1, len(items))]

		doc +="<form action=/status method=post>"
		doc +="<fieldset><legend>Block Connections</legend>"
		for i in combinations:
			doc = doc + str(i)+"<input name='blocklist' type=checkbox value=\""+str(i)+"\"><br>"

		doc += "<input type=submit></form>"
		doc +="<form action=/status method=post>"
		doc += "<input type=submit value=reset><input type=hidden name=blocklist value=1>"
		doc += "</form>"
		doc +="</fieldset>"

		doc +="<script>//ta=setTimeout(\"window.location='/status'\",5000);document.onclick=function(){window.clearTimeout(ta)}</script>"
		doc +="<a href=/status>refresh</a>"

		return [str(doc)+"\r\n"]




	#get info from working nodes here and write to db
	if env['PATH_INFO'] == '/report':
		global connection
		db = connection['bully']
		entry = {
				 "status":fs.getvalue("status"),
				 "coordinator": fs.getvalue("coordinator"),
				 "groupid":fs.getvalue("groupid"),
				 "date": datetime.datetime.utcnow()
		}

		db.bully.update({"addr": fs.getvalue("addr")},{"$set" :entry }, upsert=True)
		#print fs.getvalue("addr")+" "+fs.getvalue("status")+" "+fs.getvalue("coordinator")
		return ["Hello World!\r\n"]

	if env['PATH_INFO'] == '/':
		dict = makeDict(fs)

		ch = logging.StreamHandler()
		fh = logging.FileHandler('spam.log')

		formatter = logging.Formatter('[%(asctime)s] %(message)s %(funcName)s:%(lineno)d')
		ch.setFormatter(formatter)
		fh.setFormatter(formatter)

		logger = logging.getLogger("mylogger")
		logger.addHandler(ch)
		logger.addHandler(fh)

		record = logging.LogRecord(None, None, "", 0, "", (), None)
		record.__dict__.update(dict)

		ch.handle(record)
		fh.handle(record)

		return ["Hello World!\r\n"]

	else:
		start_response('404 Not Found', [('Content-Type', 'text/plain')])
		return ['Not Found\r\n']

if __name__ == '__main__':
	port = 0
	if len(sys.argv) > 1:
		port = int(sys.argv[1])
	if port == 0:
		port = 4711

	global server
	server = ""
	#address of proxy server
	if len(sys.argv) > 2:
		server = sys.argv[2]

	if server =="":
		server = "127.0.0.1:4712"

	print "useing proxy..."+server


	#reset logfile on startup
	f = open("spam.log","w")
	f.write("")
	f.close()
	#todo try catch - is using global the right way to do this?
	connection = MongoClient()
	db = connection['bully']
	db.bully.remove()

	print 'Serving on ',port
	myserver =wsgi.WSGIServer(('', port), startServer, log = None)
	myserver.serve_forever()



