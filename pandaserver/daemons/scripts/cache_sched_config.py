#! /usr/bin/env python
#
# Dump schedconfig on a per-queue basis into cache files
#
#

import sys
import os
import shutil
import json

from argparse import ArgumentParser
from copy import deepcopy

from pandaserver.config import panda_config
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread


# logger
_logger = PandaLogger().getLogger('cache_sched_config')


class cacheSchedConfig:
    '''
    Class to dump schedconfig on a per-queue basis into cache files
    '''
    def __init__(self, tbuf):
        self.tbuf = tbuf
        self.queueData = None
        self.cloudStatus = None
        # Define this here, but could be more flexible...
        self.queueDataFields = {
                                # Note that json dumps always use sort_keys=True; for pilot format
                                # the order defined here is respected
                                'pilot' : ['appdir', 'allowdirectaccess', 'cloud', 'datadir', 'dq2url', 'copytool', 'copytoolin', 
                                           'copysetup', 'copysetupin', 'ddm', 'se', 'sepath', 'seprodpath', 'envsetup', 'envsetupin', 
                                           'region', 'copyprefix', 'copyprefixin', 'lfcpath', 'lfcprodpath', 'lfchost', 'lfcregister', 
                                           'sein', 'wntmpdir', 'proxy', 'retry', 'recoverdir', 'space', 'memory', 'cmtconfig', 'status', 
                                           'setokens', 'glexec', 'seopt', 'gatekeeper', 'pcache', 'maxinputsize', 'timefloor', 
                                           'corecount', 'faxredirector', 'allowfax', 'maxtime', 'maxwdir',],
                                'factory' : ['site', 'siteid', 'nickname', 'cloud', 'status', 'jdl', 'queue', 'localqueue', 'nqueue', 
                                             'environ', 'proxy', 'glexec', 'depthboost', 'idlepilotsupression', 'pilotlimit', 'transferringlimit', 
                                             'memory', 'maxtime', 'system', 'fairsharepolicy','autosetup_pre','autosetup_post'],
                                # None is magic here and really means "all"
                                'all' : None,
                                }

    def query_column_sql(self, sql, varMap=None, arraySize=100):
        res = self.tbuf.querySQL(sql, varMap, arraySize=arraySize)
        retList = []
        for panda_queue, data, in res:
            if isinstance(data, str):
                dictData = json.loads(data)
            elif isinstance(data, dict):
                dictData = data
            else:
                dictData = json.loads(data.read())
            dictData['siteid'] = panda_queue
            retList.append(dictData)
        return retList
    
    def getStucturedQueueStatus(self):
        self.getQueueData()
        self.getCloudStatus()
        self.maskQueuesByCloud()

    def getQueueData(self, site = None, queue = None):
        # Dump schedconfig in a single query (it's not very big)
        varDict = {}
        sql = 'SELECT panda_queue, data from {0}.SCHEDCONFIG_JSON'.format(panda_config.schemaPANDA)
        if site:
            sql += ' where panda_queue=:site'
            varDict[':site'] = site
            self.queueData = self.query_column_sql(sql, varDict)
        elif queue:
            sql += ' where panda_queue=:queue'
            varDict[':queue'] = queue
            self.queueData = self.query_column_sql(sql, varDict)
        else:
            self.queueData = self.query_column_sql(sql)
        
    def getCloudStatus(self):
        sql = 'SELECT name, status from {0}.CLOUDCONFIG'.format(panda_config.schemaMETA)
        r = self.tbuf.querySQL(sql, None)
        self.cloudStatus = dict()
        for row in r:
            self.cloudStatus[row[0]] = row[1]

    def maskQueuesByCloud(self):
        '''Force queue status to offline if the cloud is offline'''
        for queue in self.queueData:
            try:
                if self.cloudStatus[queue['cloud']] == 'offline':
                    queue['status'] = 'offline'
                    _logger.info('Queue %s forced offline (cloud = %s is offline)' % (queue['nickname'], queue['cloud']))
            except KeyError:
                _logger.error('No valid cloud status for queue %s (cloud = %s)' % (queue['nickname'], queue['cloud']))

    def dumpSingleQueue(self, queueDict, dest='/tmp', outputSet='all', format='txt'):
        try:
            file = os.path.join(dest, queueDict['nickname'] + "." + outputSet + "." + format)
            output = open(file, "w")
            outputFields = self.queueDataFields[outputSet]
            if outputFields == None:
                outputFields = queueDict.keys()
            if format == 'txt':
                for outputField in outputFields:
                    output.write(outputField + "=" + str(queueDict[outputField]))
            if format == 'pilot':
                outputStr = ''
                for outputField in outputFields:
                    if outputField in queueDict and queueDict[outputField]:
                        outputStr += outputField + "=" + str(queueDict[outputField]) + "|"
                    else:
                        outputStr += outputField + "=|"
                output.write(outputStr[:-1])
            if format == 'json':
                dumpMe = {}
                for outputField in outputFields:
                    if outputField in queueDict:
                        val = queueDict[outputField]
                    else:
                        val = ''
                    dumpMe[outputField] = val
                json.dump(self.queueDictPythonise(dumpMe), output, sort_keys=True, indent=4)
            output.close()
            # a copy of the file, when makes sense, with filename based on siteid
            newfile = os.path.join(dest, queueDict['siteid'] + "." + outputSet + "." + format)
            if newfile != file:
                shutil.copy(file, newfile)
        except Exception:
            raise

    def dumpQueues(self, queueArray, dest='/tmp', outputSet='all', format='txt'):
        for queueDict in queueArray:
            self.dumpSingleQueue(queueArray, dest, outputSet, format)

    def queueDictPythonise(self, queueDict, deepCopy=True):
        '''Turn queue dictionary with SQL text fields into a more stuctured python representation'''
        if deepCopy:
            structDict = deepcopy(queueDict)
        else:
            structDict = queueDict

        if 'releases' in structDict and structDict['releases'] != None:
            if isinstance(structDict['releases'], str):
                structDict['releases'] = structDict['releases'].split('|')
        # TODO - Change this into Ricardo's ISO dateTime in UTC?
        for timeKey in 'lastmod', 'tspace':
            if timeKey in structDict:
                structDict[timeKey] = structDict[timeKey].isoformat()
        return structDict

    def dumpAllSchedConfig(self, queueArray=None, dest='/tmp'):
        '''Dumps all of schedconfig into a single json file - allows clients to retrieve a
        machine readable version of schedconfig efficiently'''
        file = os.path.join(dest, "schedconfig.all.json")
        if queueArray == None:
            queueArray = self.queueData
        output = open(file, "w")
        dumpMe = {}
        for queueDict in queueArray:
            dumpMe[queueDict['nickname']] = {}
            for k in queueDict:
                v = queueDict[k]
                dumpMe[queueDict['nickname']][k] = v
            dumpMe[queueDict['nickname']] = self.queueDictPythonise(dumpMe[queueDict['nickname']])
        json.dump(dumpMe, output, sort_keys=True, indent=4)
        self.dump_pilot_gdp_config(dest)

    def dump_pilot_gdp_config(self, dest='/tmp'):
        app = 'pilot'
        dump_me = {}
        sql = 'SELECT key, component, vo from {}.config where app=:app'.format(panda_config.schemaPANDA)
        r = self.tbuf.querySQL(sql, {':app': app})
        for key, component, vo in r:
            dump_me.setdefault(vo, {})
            value = self.tbuf.getConfigValue(component, key, app, vo)
            dump_me[vo][key] = value
        # dump
        _logger.debug("pilot GDP config: {}".format(str(dump_me)))
        with open(os.path.join(dest, 'pilot_gdp_config.json'), 'w') as f:
            json.dump(dump_me, f, sort_keys=True, indent=4)


def main(argv=tuple(), tbuf=None, **kwargs):
    _logger.debug('start')

    parser = ArgumentParser()
    parser.add_argument("-o", "--output", dest="dirname",
                      help="write cache outputs to DIR", metavar="DIR")
    args = parser.parse_args()

    # instantiate TB
    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd,
                        nDBConnection=1, useTimeout=True, requester=requester_id)
    else:
        taskBuffer = tbuf

    # dump
    cacher = cacheSchedConfig(tbuf=taskBuffer)
    cacher.getStucturedQueueStatus()
    
    for queue in cacher.queueData:
        cacher.dumpSingleQueue(queue, dest=args.dirname, outputSet='pilot', format='pilot')
        cacher.dumpSingleQueue(queue, dest=args.dirname, outputSet='pilot', format='json')
        cacher.dumpSingleQueue(queue, dest=args.dirname, outputSet='all', format='json')
        cacher.dumpSingleQueue(queue, dest=args.dirname, outputSet='factory', format='json')
    
    _logger.debug('dumped json files for each queue')
        
    # Big dumper
    cacher.dumpAllSchedConfig(dest=args.dirname)
    _logger.debug('dumped schedconfig.all.json')

    # stop taskBuffer if created inside this script
    if tbuf is None:
        taskBuffer.cleanup(requester=requester_id)
    
    _logger.debug('done')


if __name__ == "__main__":
    main()
