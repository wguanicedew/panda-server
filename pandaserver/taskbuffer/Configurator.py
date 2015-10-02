from config import panda_config
from pandalogger.PandaLogger import PandaLogger
from brokerage.SiteMapper import SiteMapper
from taskbuffer.TaskBuffer import taskBuffer as task_buffer
import urllib2
import json
import sys
# logger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

class Configurator():
    
    def __init__(self):
        task_buffer.init(panda_config.dbhost, panda_config.dbpasswd, panda_config.nDBConnection ,True)
        site_mapper = SiteMapper(task_buffer)


    #Internal caching of a result. Use only for information 
    #with low update frequency and low memory footprint
    def memoize(f):
        memo = {}
        kwd_mark = object()
        def helper(self, *args, **kwargs):
            now = datetime.datetime.now()
            key = args + (kwd_mark,) + tuple(sorted(kwargs.items()))
            if key not in memo or memo[key]['timestamp'] < now - datetime.timedelta(hours=1):
                memo[key] = {}
                memo[key]['value'] = f(self, *args, **kwargs)
                memo[key]['timestamp'] = now
            return memo[key]['value']
        return helper

    @memoize
    def get_nuclei(self):
        return self.__get_tier1s_agis()


    def get_satellites(self, site=None, task_id=None):
        return []


    def __get_tier1s_schedconfig(self):
        """
        First implementation: return the Tier1s...
        """
        #TODO: discuss with Tadashi what format he wants, if he wants sitenames, queues, SE...
        tier1s = []
        for cloud_name in site_mapper.getCloudList():
            # get cloud
            cloud_spec = site_mapper.getCloud(cloud_name)
            # get T1
            t1_name = cloud_spec['source']
            
            tier1s.append(t1_name)
        return tier1s


    # update endpoint dict
    def __get_tier1s_agis(self):
        # get json
        try:
            logger.debug('start')
            jsonStr = ''
            #TODO: read the documentation and see if there is some filtering option to avoid getting the whole grid information
            #TODO: I guess it also needs to return CERN in the list
            response = urllib2.urlopen('http://atlas-agis-api.cern.ch/request/site/query/?json&tier_level=1&vo_name=atlas')
            json_str = response.read()
            
            #This loads 
            tier1s = json.loads(json_str)
            
            tier1_names = []
            for entry in tier1s:
                tier1_names.append(entry['rc_site'])
            
            return tier1_names
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errStr = 'failed to update EP with {0} {1} jsonStr={2}'.format(errtype.__name__,
                                                                           errvalue,
                                                                           jsonStr)
            tmpLog.error(errStr)
        return


if __name__ == "__main__":
    configurator = Configurator()
    nuclei = configurator.get_nuclei()
    print nuclei
