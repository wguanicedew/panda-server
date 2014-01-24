"""
WrappedCursor for a generic database connection proxy

"""

import re
import os
import sys
# import time
# import fcntl
# import types
# import random
# import urllib
import socket
# import datetime
# import commands
import traceback
import warnings
try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None
try:
    import MySQLdb
except ImportError:
    MySQLdb = None
# import ErrorCode
# import SiteSpec
# import CloudSpec
# import PrioUtil
# import ProcessGroups
# from JobSpec  import JobSpec
# from FileSpec import FileSpec
# from DatasetSpec import DatasetSpec
# from CloudTaskSpec import CloudTaskSpec
from pandalogger.PandaLogger import PandaLogger
from config import panda_config
# from brokerage.PandaSiteIDs import PandaSiteIDs

warnings.filterwarnings('ignore')

# logger
_logger = PandaLogger().getLogger('DBProxyWrappedCursor')
#_logger = PandaLogger().getLogger('DBProxy')

# proxy
class WrappedCursor(object):
    # connection object
    conn = None
    # cursor object
    cur = None
    # use special error codes for reconnection in querySQL
    useOtherError = False
    # backend
    backend = 'oracle'
    # schema name, PANDA
    schemanamebase = 'ATLAS_PANDA'
    # schema name, PANDAMETA
    schemanamemeta = 'ATLAS_PANDAMETA'
    # schema name, GRISLI
    schemanamegris = 'ATLAS_GRISLI'
    # schema name, PANDAARCH
    schemanamearch = 'ATLAS_PANDAARCH'


    # constructor
    def __init__(self, connection, \
                        useOtherError=False, \
                        backend='oracle', \
                        schemanamebase='ATLAS_PANDA', \
                        schemanamemeta='ATLAS_PANDAMETA', \
                        schemanamegris='ATLAS_GRISLI', \
                        schemanamearch='ATLAS_PANDAARCH' \
                        ):
        # connection object
        self.conn = connection
        # cursor object
        if self.conn is not None:
            self.cur = self.conn.cursor()
        # statement
        statement = ''
        # use special error codes for reconnection in querySQL
        self.useOtherError = useOtherError
        # backend
        self.backend = backend
        # schema name, PANDA
        self.schemanamebase = schemanamebase
        # schema name, PANDAMETA
        self.schemanamemeta = schemanamemeta
        # schema name, GRISLI
        self.schemanamegris = schemanamegris
        # schema name, PANDAARCH
        self.schemanamearch = schemanamearch
        _logger.debug('schemanamebase=' + self.schemanamebase)
        _logger.debug('schemanamemeta=' + self.schemanamemeta)
        _logger.debug('schemanamegris=' + self.schemanamegris)
        _logger.debug('schemanamearch=' + self.schemanamearch)
        # arraysize
        self.arraysize = 1000
        if self.cur is not None:
            self.cur.arraysize = 1000
        # imported cx_Oracle, MySQLdb?
        _logger.info('cx_Oracle=%s' % str(cx_Oracle))
        _logger.info('MySQLdb=%s' % str(MySQLdb))


    # __setattr__
    def __setattr__(self, name, value):
        super(WrappedCursor, self).__setattr__(name, value)

    # __getattr__
    def __getattr__(self, name):
        return super(WrappedCursor, self).__getattr__(name)

    # __iter__
    def __iter__(self):
        return iter(self.cur)

    # serialize
    def __str__(self):
        return 'WrappedCursor[%(conn)s]' % ({'conn': self.conn})


    # execute query on cursor
    def execute(self, sql, varDict=None, cur=None  # , returningInto=None
                ):
        if varDict is None:
            varDict = {}
        # returningInto is None or is in [{'returning': 'PandaID', 'into': ':newPandaID'}, {'returning': 'row_ID', 'into': ':newRowID'}]
        if cur is None:
            cur = self.cur
        ret = None
        if self.backend == 'oracle':
#            if returningInto is not None:
#                sql += self._returningIntoOracle(returningInto, varDict, cur)
            _logger.debug("execute : SQL     %s " % sql)
            _logger.debug("execute : varDict %s " % varDict)
            ret = cur.execute(sql, varDict)
        elif self.backend == 'mysql':
            print "DEBUG execute : original SQL     %s " % sql
            print "DEBUG execute : original varDict %s " % varDict
            # CURRENT_DATE interval
#            sql = re.sub("CURRENT_DATE\s*-\s*(\d+|:[^\s\)]+)", "DATE_SUB(CURDATE(),INTERVAL \g<1> DAYS)", sql)
            sql = re.sub("CURRENT_DATE\s*-\s*(\d+|:[^\s\)]+)", "DATE_SUB(CURRENT_DATE,INTERVAL \g<1> DAY)", sql)

#            # CURRENT_DATE
#            sql = re.sub('CURRENT_DATE', 'CURDATE()', sql)

            # SYSDATE interval
            sql = re.sub("SYSDATE\s*-\s*(\d+|:[^\s\)]+)", "DATE_SUB(SYSDATE,INTERVAL \g<1> DAY)", sql)

            # SYSDATE
            sql = re.sub('SYSDATE', 'SYSDATE()', sql)

            # EMPTY_CLOB()
            sql = re.sub('EMPTY_CLOB\(\)', "''", sql)

            # ROWNUM
            sql = re.sub("(?i)(AND)*\s*ROWNUM\s*<=\s*(\d+)", " LIMIT \g<2>", sql)
            sql = re.sub("(?i)(WHERE)\s*LIMIT\s*(\d+)", " LIMIT \g<2>" , sql)

            # RETURNING INTO
            returningInto = None
            m = re.search("RETURNING ([^\s]+) INTO ([^\s]+)", sql, re.I)
            if m is not None:
                returningInto = [{'returning': m.group(1), 'into': m.group(2)}]
                self._returningIntoMySQLpre(returningInto, varDict, cur)
                sql = re.sub(m.group(0), '', sql)
            # schema names
            sql = re.sub('ATLAS_PANDA\.', self.schemanamebase + '.', sql)
            sql = re.sub('ATLAS_PANDAMETA\.', self.schemanamemeta + '.', sql)
            sql = re.sub('ATLAS_GRISLI\.', self.schemanamegris + '.', sql)
            sql = re.sub('ATLAS_PANDAARCH\.', self.schemanamearch + '.', sql)

            # bind variables
            newVarDict = {}
            # make sure that :prodDBlockToken will not be replaced by %(prodDBlock)sToken
            keys = sorted(varDict.keys(), key=lambda s:-len(str(s)))
            for key in keys:
                val = varDict[key]
                if key[0] == ':':
                    newKey = key[1:]
                    sql = sql.replace(key, '%(' + newKey + ')s')
                else:
                    newKey = key
                    sql = sql.replace(':' + key, '%(' + newKey + ')s')
                newVarDict[newKey] = val
            try:
                # from PanDA monitor it is hard to log queries sometimes, so let's debug with hardcoded query dumps
                import time
                if os.path.exists('/data/atlpan/oracle/panda/monitor/logs/write_queries.txt'):
                    f = open('/data/atlpan/oracle/panda/monitor/logs/mysql_queries_WrappedCursor.txt', 'a')
                    f.write('mysql|%s|%s|%s\n' % (str(time.time()), str(sql), str(newVarDict)))
                    f.close()
            except:
                pass
            _logger.debug("execute : SQL     %s " % str(sql))
            _logger.debug("execute : varDict %s " % str(newVarDict))
            print "DEBUG execute : SQL     %s " % sql
            print "DEBUG execute : varDict %s " % newVarDict
            ret = cur.execute(sql, newVarDict)
            if returningInto is not None:
                ret = self._returningIntoMySQLpost(returningInto, varDict, cur)
        return ret


    def _returningIntoOracle(self, returningInputData, varDict, cur, dryRun=False):
        # returningInputData=[{'returning': 'PandaID', 'into': ':newPandaID'}, {'returning': 'row_ID', 'into': ':newRowID'}]
        result = ''
        if returningInputData is not None:
            try:
                valReturning = str(',').join([x['returning'] for x in returningInputData])
                listInto = [x['into'] for x in returningInputData]
                valInto = str(',').join(listInto)
                # assuming that we use RETURNING INTO only for PandaID or row_ID columns
                if not dryRun:
                    for x in listInto:
                        varDict[x] = cur.var(cx_Oracle.NUMBER)
                result = ' RETURNING %(returning)s INTO %(into)s ' % {'returning': valReturning, 'into': valInto}
            except:
                pass
        return result


    def _returningIntoMySQLpre(self, returningInputData, varDict, cur):
        # returningInputData=[{'returning': 'PandaID', 'into': ':newPandaID'}, {'returning': 'row_ID', 'into': ':newRowID'}]
        if returningInputData is not None:
            try:
                # get rid of "returning into" items in varDict
                listInto = [x['into'] for x in returningInputData]
                for x in listInto:
                    try:
                        del varDict[x]
                    except KeyError:
                        pass
                if len(returningInputData) == 1:
                    # and set original value in varDict to null, let auto_increment do the work
                    listReturning = [x['returning'] for x in returningInputData]
                    for x in listReturning:
                        varDict[':' + x] = None
            except:
                pass

    def _returningIntoMySQLpost(self, returningInputData, varDict, cur):
        # returningInputData=[{'returning': 'PandaID', 'into': ':newPandaID'}, {'returning': 'row_ID', 'into': ':newRowID'}]
        result = long(0)
        if len(returningInputData) == 1:
            ret = self.cur.execute(""" SELECT LAST_INSERT_ID() """)
            result, = self.cur.fetchone()
            if returningInputData is not None:
                try:
                    # update of "returning into" items in varDict
                    listInto = [x['into'] for x in returningInputData]
                    for x in listInto:
                        try:
                            varDict[x] = long(result)
                        except KeyError:
                            pass
                except:
                    pass
        return result


    # fetchall
    def fetchall(self):
        if self.cur.arraysize != self.arraysize:
            self.cur.arraysize = self.arraysize
            return self.fetchmany(self.arraysize)
        else:
            return self.cur.fetchall()


    # fetchmany
    def fetchmany(self, arraysize=1000):
        self.arraysize = arraysize
        self.cur.arraysize = arraysize
        return self.cur.fetchmany()


    # fetchall
    def fetchone(self):
        return self.cur.fetchone()


    # var
    def var(self, dataType, *args, **kwargs):
        if self.backend == 'mysql':
            return type(dataType, (dataType,), 0)
        else:  #backend == 'oracle'
            return self.cur.var(datatype, *args, **kwargs)


    # next
    def next(self):
        return self.fetchone()


    # close
    def close(self):
        return self.cur.close()

    # prepare
    def prepare(self, statement):
        self.statement = statement

    # executemany
    def executemany(self, sql, params):
        if sql is None:
            sql = self.statement
        for paramsItem in params:
            self.execute(sql, paramsItem)

    # get_description
    @property
    def description(self):
        return self.cur.description

    # rowcount
    @property
    def rowcount(self):
        return self.cur.rowcount


