import os
import re
import sys
import time
import signal
import socket
import commands
import optparse
import datetime
import cPickle as pickle

from dq2.common import log as logging
from dq2.common import stomp
from config import panda_config
from brokerage.SiteMapper import SiteMapper
from dataservice import DataServiceUtils
from dataservice.DDMHandler import DDMHandler

import yaml
import logging
logging.basicConfig(level = logging.DEBUG)

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('datasetCallbackListener')

# keep PID
pidFile = '%s/dataset_callback_listener.pid' % panda_config.logdir

# overall timeout value
overallTimeout = 60 * 59

# expiration time
expirationTime = datetime.datetime.utcnow() + datetime.timedelta(minutes=overallTimeout)


# kill whole process
def catch_sig(sig, frame):
    try:
        os.remove(pidFile)
    except:
        pass
    # kill
    _logger.debug('terminating ...')
    commands.getoutput('kill -9 -- -%s' % os.getpgrp())
    # exit
    sys.exit(0)
                                        

# callback listener
class DatasetCallbackListener(stomp.ConnectionListener):

    def __init__(self,conn,tb,sm):
        # connection
        self.conn = conn
        # task buffer
        self.taskBuffer = tb
        # site mapper
        self.siteMapper = sm

        
    def on_error(self,headers,body):
        _logger.error("on_error : %s" % headers['message'])


    def on_disconnected(self,headers,body):
        _logger.error("on_disconnected : %s" % headers['message'])
                        

    def on_message(self, headers, message):
        try:
            dsn = 'UNKNOWN'
            # send ack
            id = headers['message-id']
            self.conn.ack({'message-id':id})
	    # convert message form str to dict
            messageDict = yaml.load(message)
            # check event type
            if not messageDict['event_type'] in ['datasetlock_ok']:
                _logger.debug('%s skip' % messageDict['event_type'])
                return
	    _logger.debug('%s start' % messageDict['event_type'])  
            messageObj = messageDict['payload']
            # only for _dis or _sub
	    dsn = messageObj['name']
	    if (re.search('_dis\d+$',dsn) == None) and (re.search('_sub\d+$',dsn) == None):
		_logger.debug('%s is not _dis or _sub dataset, skip' % dsn)
		return
            # take action
	    scope = messageObj['scope']
	    site  = messageObj['rse']
	    _logger.debug('%s site=%s type=%s' % (dsn, site, messageDict['event_type']))
	    thr = DDMHandler(self.taskBuffer,None,site,dsn,scope)
	    thr.start()
	    thr.join()
	    _logger.debug('done %s' % dsn)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("on_message : %s %s" % (errtype,errvalue))
        

# main
def main(backGround=False): 
    _logger.debug('starting ...')
    # register signal handler
    signal.signal(signal.SIGINT, catch_sig)
    signal.signal(signal.SIGHUP, catch_sig)
    signal.signal(signal.SIGTERM,catch_sig)
    signal.signal(signal.SIGALRM,catch_sig)
    signal.alarm(overallTimeout)
    # forking    
    pid = os.fork()
    if pid != 0:
        # watch child process
        os.wait()
        time.sleep(1)
    else:    
        # main loop
        from taskbuffer.TaskBuffer import taskBuffer
        # check certificate
        certName = '/data/atlpan/pandasv1_usercert.pem'
        #certName = '/etc/grid-security/hostcert.pem'
        _logger.debug('checking certificate {0}'.format(certName))
        certOK,certMsg = DataServiceUtils.checkCertificate(certName)
        if not certOK:
            _logger.error('bad certificate : {0}'.format(certMsg))
        # initialize cx_Oracle using dummy connection
        from taskbuffer.Initializer import initializer
        initializer.init()
        # instantiate TB
        taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
        # instantiate sitemapper
        siteMapper = SiteMapper(taskBuffer)
        # ActiveMQ params
	queue = '/topic/rucio.events'
        ssl_opts = {'use_ssl' : True,
                    'ssl_cert_file' : certName,
                    'ssl_key_file'  : '/data/atlpan/pandasv1_userkey.pem'}
        # resolve multiple brokers
        brokerList = socket.gethostbyname_ex('atlasddm-mb.cern.ch')[-1]
	# set listener
        for tmpBroker in brokerList:
            try:
                clientid = 'PANDA-' + socket.getfqdn() + '-' + tmpBroker
                _logger.debug('setting listener %s to broker %s' % (clientid, tmpBroker))
                conn = stomp.Connection(host_and_ports = [(tmpBroker, 6162)], **ssl_opts)
                conn.set_listener('DatasetCallbackListener', DatasetCallbackListener(conn,taskBuffer,siteMapper))
                conn.start()
                conn.connect(headers = {'client-id': clientid})
                conn.subscribe(destination=queue, ack='client-individual')
                if not conn.is_connected():
                    _logger.error("connection failure to %s" % tmpBroker)
                _logger.debug('listener %s is up and running' % clientid)
            except:     
                errtype,errvalue = sys.exc_info()[:2]
                _logger.error("failed to set listener on %s : %s %s" % (tmpBroker,errtype,errvalue))
                catch_sig(None,None)
            
# entry
if __name__ == "__main__":
    optP = optparse.OptionParser(conflict_handler="resolve")
    options,args = optP.parse_args()
    try:
        # time limit
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(seconds=overallTimeout-180)
        # get process list
        scriptName = sys.argv[0]
        out = commands.getoutput('env TZ=UTC ps axo user,pid,lstart,args | grep %s' % scriptName)
        for line in out.split('\n'):
            items = line.split()
            # owned process
            if not items[0] in ['sm','atlpan','root']: # ['os.getlogin()']: doesn't work in cron
                continue
            # look for python
            if re.search('python',line) == None:
                continue
            # PID
            pid = items[1]
            # start time
            timeM = re.search('(\S+\s+\d+ \d+:\d+:\d+ \d+)',line)
            startTime = datetime.datetime(*time.strptime(timeM.group(1),'%b %d %H:%M:%S %Y')[:6])
            # kill old process
            if startTime < timeLimit:
                _logger.debug("old process : %s %s" % (pid,startTime))
                _logger.debug(line)            
                commands.getoutput('kill -9 %s' % pid)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        _logger.error("kill process : %s %s" % (errtype,errvalue))
    # main loop    
    main()
