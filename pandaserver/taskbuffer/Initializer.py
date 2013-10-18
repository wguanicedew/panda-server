import sys
# try:
#     import cx_Oracle
# except:
#     pass
import cx_Oracle
try:
    import MySQLdb
except:
    pass
from threading import Lock

from config import panda_config

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('Initializer')

# initialize cx_Oracle using dummy connection to avoid "Unable to acquire Oracle environment handle"
class Initializer:
    def __init__(self):
        self.lock = Lock()
        self.first = True

    def init(self):
        _logger.debug("init new=%s" % self.first)
        # do nothing when nDBConnection is 0
        if panda_config.nDBConnection == 0:
            return True
        # lock
        self.lock.acquire()
        if self.first:
            self.first = False
            try:
                _logger.debug("connect")
                if hasattr(panda_config, 'dbengine'):
                    _logger.debug("panda_config.dbengine: " + panda_config.dbengine)
                # connect
                if panda_config.dbengine == 'mysql':
                    conn = self._connectTestMySQL()
                else:
                    conn = self._connectTestOracle()
                # close
                conn.close()
                _logger.debug("done")
            except:
                self.lock.release()
                type, value, traceBack = sys.exc_info()
                _logger.error("connect : %s %s" % (type, value))
                return False
        # release    
        self.lock.release()
        return True


    def _connectTestOracle(self):
        return cx_Oracle.connect(dsn=panda_config.dbhost, user=panda_config.dbuser,
                                 password=panda_config.dbpasswd, threaded=True)


    def _connectTestMySQL(self):
        return MySQLdb.connect(host=panda_config.dbhostmysql, db=panda_config.dbnamemysql, \
                               port=panda_config.dbportmysql, connect_timeout=panda_config.dbtimeout, \
                               user=panda_config.dbusermysql, passwd=panda_config.dbpasswdmysql)

# singleton
initializer = Initializer()
del Initializer
