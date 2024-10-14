# Standalone script for first testing of error classification rules in the database

import sys

from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import taskBuffer

# instantiate task buffer
requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1, requester=requester_id)

try:
    # possibility to specify job as an argument
    job_id = sys.argv[1]
except IndexError:
    # define some default job ID that we know is in the database
    job_id = 123456789

# get the job from the database
# JobSpec definition: https://github.com/PanDAWMS/panda-server/blob/master/pandaserver/taskbuffer/JobSpec.py
job_spec = taskBuffer.peekJobs([job_id])[0]
if not job_spec:
    print(f"Job with ID {job_id} not found")
else:
    print(f"Got job with ID {job_spec.PandaID} and status {job_spec.jobStatus}")

# load the error classification rules from the database
sql = "SELECT error_source, error_code, error_diag, error_class FROM ATLASPANDA.ERROR_CLASSIFICATION"
var_map = {}  # no variables in this query, but the API requires a dictionary
status, results = taskBuffer.querySQLS(sql)
if not results:
    print("There are no error classification rules in the database or the query failed")
else:
    print(f"Got following rules: {results}")

# classify the error...