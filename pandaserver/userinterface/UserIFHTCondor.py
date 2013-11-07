'''
provide web interface to users

'''

import re
import sys
import time
import json
import types
import cPickle as pickle
import jobdispatcher.Protocol as Protocol
import brokerage.broker
import taskbuffer.ProcessGroups
from config import panda_config
from taskbuffer.JobSpecHTCondor import JobSpecHTCondor
from taskbuffer.WrappedPickle import WrappedPickle
from brokerage.SiteMapper import SiteMapper
from pandalogger.PandaLogger import PandaLogger
from RbLauncher import RbLauncher
from ReBroker import ReBroker
from taskbuffer import PrioUtil
from dataservice.DDM import dq2Info

from taskbuffer.TaskBuffer import taskBuffer


# logger
_logger = PandaLogger().getLogger('UserIFHTCondor')


# main class     
class UserIFHTCondor:
    # constructor
    def __init__(self):
        self.taskBuffer = None
        self.taskBuffer = taskBuffer
        self.taskBuffer.init('dbname', 'dbpass')
        self.init(self.taskBuffer)


    # initialize
    def init(self,taskBuffer):
        self.taskBuffer = taskBuffer


    # add jobs
    def addHTCondorJobs(self, jobsStr, user, host, userFQANs):
        """
            addHTCondorJobs
            args:
                jobsStr: list of HTCondorJobSpecs
                user: DN of the user adding HTCondor job via this API
                host: remote host of the request
                userFQANs: FQANs of the user's proxy
            returns:
                pickle of list of tuples with CondorID and PandaID
        """
        _logger.debug('mark')
        try:
            _logger.debug('mark')
            # deserialize jobspecs
            jobs = WrappedPickle.loads(jobsStr)
            _logger.debug('mark')
            _logger.debug("addHTCondorJobs %s len:%s FQAN:%s" % (user, len(jobs), str(userFQANs)))
            maxJobs = 5000
            if len(jobs) > maxJobs:
                _logger.error("too many jobs %s, more than %s" % (len(jobs), maxJobs))
                jobs = jobs[:maxJobs]
            _logger.debug('mark')
        except:
            _logger.debug('mark')
            type, value, traceBack = sys.exc_info()
            _logger.error("addHTCondorJobs : %s %s" % (type, value))
            jobs = []
        _logger.debug('mark')
        _logger.debug('jobs= %s' % str(jobs))
        # store jobs
        ret = self.taskBuffer.storeHTCondorJobs(jobs, user, fqans=userFQANs)
        _logger.debug('mark')
        _logger.debug("addHTCondorJobs %s ->:%s" % (user, len(ret)))
        # serialize 
        return pickle.dumps(ret)


    # update jobs
    def updateHTCondorJobs(self, jobsStr, user, host, userFQANs):
        """
            updateHTCondorJobs
            args:
                jobsStr: the list of dictionaries with HTCondorJobSpecs properties 
                    to be updated. 
                    CondorID key has to be present in every dictionary.
                user: DN of the user adding HTCondor job via this API
                host: remote host of the request
                userFQANs: FQANs of the user's proxy
            returns:
                pickle of list of tuples with CondorID and PandaID
        """
        _logger.debug('mark')
        try:
            _logger.debug('mark')
            # deserialize jobspecs
#            jobs = WrappedPickle.loads(jobsStr)
            jobs = pickle.loads(jobsStr)
            _logger.debug('mark')
            _logger.debug("updateHTCondorJobs %s len:%s FQAN:%s" % (user, len(jobs), str(userFQANs)))
            maxJobs = 5000
            if len(jobs) > maxJobs:
                _logger.error("too many jobs %s, more than %s" % (len(jobs), maxJobs))
                jobs = jobs[:maxJobs]
            _logger.debug('mark')
        except:
            _logger.debug('mark')
            type, value, traceBack = sys.exc_info()
            _logger.error("updateHTCondorJobs : %s %s" % (type, value))
            jobs = []
        _logger.debug('mark')
        _logger.debug('jobs= %s' % str(jobs))
        # store jobs
        ret = self.taskBuffer.updateHTCondorJobs(jobs, user, fqans=userFQANs)
        _logger.debug('mark')
        _logger.debug("updateHTCondorJobs %s ->:%s" % (user, len(ret)))
        # serialize
        return pickle.dumps(ret)


# Singleton
userIF = UserIFHTCondor()
del UserIFHTCondor


# get FQANs
def _getFQAN(req):
    fqans = []
    for tmpKey,tmpVal in req.subprocess_env.iteritems():
        # compact credentials
        if tmpKey.startswith('GRST_CRED_'):
            # VOMS attribute
            if tmpVal.startswith('VOMS'):
                # FQAN
                fqan = tmpVal.split()[-1]
                # append
                fqans.append(fqan)
        # old style         
        elif tmpKey.startswith('GRST_CONN_'):
            tmpItems = tmpVal.split(':')
            # FQAN
            if len(tmpItems)==2 and tmpItems[0]=='fqan':
                fqans.append(tmpItems[-1])
    # return
    return fqans


# get DN
def _getDN(req):
    realDN = ''
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        realDN = req.subprocess_env['SSL_CLIENT_S_DN']
        # remove redundant CN
        realDN = re.sub('/CN=limited proxy','',realDN)
        realDN = re.sub('/CN=proxy(/CN=proxy)+','/CN=proxy',realDN)
    return realDN


## check role
#def _isProdRoleATLAS(req):
#    # check role
#    prodManager = False
#    # get FQANs
#    fqans = _getFQAN(req)
#    # loop over all FQANs
#    for fqan in fqans:
#        # check production role
#        for rolePat in ['/atlas/usatlas/Role=production','/atlas/Role=production']:
#            if fqan.startswith(rolePat):
#                return True
#    return False



"""
web service interface

"""

# security check
def isSecure(req):
    # check security
    if not Protocol.isSecure(req):
        return False
    # disable limited proxy
    if '/CN=limited proxy' in req.subprocess_env['SSL_CLIENT_S_DN']:
        _logger.warning("access via limited proxy : %s" % req.subprocess_env['SSL_CLIENT_S_DN'])
        return False
    return True


# add jobs
def addHTCondorJobs(req, jobs):
    """
        addHTCondorJobs
        args:
            jobs: the list of HTCondorJobSpecs
        returns:
            response of userIF.addHTCondorJobs
    """
    _logger.debug('mark')
    # check security
    if not isSecure(req):
        _logger.debug('mark')
        return False
    _logger.debug('mark')
    # get DN
    user = None
    _logger.debug('mark')
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        _logger.debug('mark')
        user = _getDN(req)
        _logger.debug('mark')
    _logger.debug('mark')
    # get FQAN
    fqans = _getFQAN(req)
    _logger.debug('mark')
    # hostname
    host = req.get_remote_host()
    _logger.debug('mark')
    return userIF.addHTCondorJobs(jobs, user, host, fqans)


# update jobs
def updateHTCondorJobs(req, jobs):
    """
        updateHTCondorJobs
        args:
            jobs: the list of dictionaries with HTCondorJobSpecs properties 
                    to be updated. 
                    CondorID key has to be present in every dictionary.
        returns:
            response of userIF.addHTCondorJobs
    """
    _logger.debug('mark')
    # check security
    if not isSecure(req):
        _logger.debug('mark')
        return False
    _logger.debug('mark')
    # get DN
    user = None
    _logger.debug('mark')
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        _logger.debug('mark')
        user = _getDN(req)
        _logger.debug('mark')
    _logger.debug('mark')
    # get FQAN
    fqans = _getFQAN(req)
    _logger.debug('mark')
    # hostname
    host = req.get_remote_host()
    _logger.debug('mark')
    return userIF.updateHTCondorJobs(jobs, user, host, fqans)


