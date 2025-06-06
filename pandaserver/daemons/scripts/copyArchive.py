import datetime
import math
import os
import re
import sys
import time
import traceback

import requests
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread
from urllib3.exceptions import InsecureRequestWarning

import pandaserver.userinterface.Client as Client
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.jobdispatcher.Watcher import Watcher
from pandaserver.taskbuffer import EventServiceUtils

# logger
_logger = PandaLogger().getLogger("copyArchive")


# main
def main(argv=tuple(), tbuf=None, **kwargs):
    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)

    # password
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

    _logger.debug("===================== start =====================")

    # memory checker
    def _memoryCheck(str):
        try:
            proc_status = "/proc/%d/status" % os.getpid()
            procfile = open(proc_status)
            name = ""
            vmSize = ""
            vmRSS = ""
            # extract Name,VmSize,VmRSS
            for line in procfile:
                if line.startswith("Name:"):
                    name = line.split()[-1]
                    continue
                if line.startswith("VmSize:"):
                    vmSize = ""
                    for item in line.split()[1:]:
                        vmSize += item
                    continue
                if line.startswith("VmRSS:"):
                    vmRSS = ""
                    for item in line.split()[1:]:
                        vmRSS += item
                    continue
            procfile.close()
            _logger.debug(f"MemCheck - {os.getpid()} Name={name} VSZ={vmSize} RSS={vmRSS} : {str}")
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error(f"memoryCheck() : {type} {value}")
            _logger.debug(f"MemCheck - {os.getpid()} unknown : {str}")
        return

    _memoryCheck("start")

    # instantiate TB
    from pandaserver.taskbuffer.TaskBuffer import taskBuffer

    taskBuffer.init(
        panda_config.dbhost,
        panda_config.dbpasswd,
        nDBConnection=1,
        useTimeout=True,
        requester=requester_id,
    )

    # instantiate sitemapper
    siteMapper = SiteMapper(taskBuffer)

    # kick merging jobs
    _logger.debug("Kick merging session")
    try:
        # get min PandaID for failed jobs in Active table
        sql = (
            "SELECT j.PandaID, j.jediTaskID, f.destinationDBlock "
            f"FROM {panda_config.schemaPANDA}.jobsActive4 j, {panda_config.schemaPANDA}.filesTable4 f "
            "WHERE f.PandaID=j.PandaID AND j.prodSourceLabel=:prodSourceLabel AND j.jobStatus=:jobStatus "
            "AND f.type=:type "
        )
        var_map = {":jobStatus": "merging", ":prodSourceLabel": "user", ":type": "log"}
        status, res = taskBuffer.querySQLS(sql, var_map)
        destination_blocks = {}
        if res is not None:
            # collect destination blocks
            for panda_id, task_id, destination_block in res:
                if destination_block in destination_blocks:
                    continue
                destination_blocks[destination_block] = (panda_id, task_id)
            # loop over all destination blocks
            for destination_block, (panda_id, task_id) in destination_blocks.items():
                # check
                tmp_log = LogWrapper(_logger, f"kick_merge < jediTaskID={task_id} dest={destination_block} >")
                tmp_log.debug("check")
                sqlC = f"WITH w AS (SELECT PandaID FROM {panda_config.schemaPANDA}.filesTable4 WHERE destinationDBlock=:destinationDBlock) "
                sqlC += "SELECT COUNT(*) FROM ("
                sqlC += "SELECT j.PandaID FROM ATLAS_PANDA.jobsActive4 j, w "
                sqlC += "WHERE j.PandaID=w.PandaID  "
                sqlC += "AND NOT j.jobStatus IN (:jobStatus1,:jobStatus2) "
                sqlC += "UNION "
                sqlC += "SELECT j.PandaID FROM ATLAS_PANDA.jobsDefined4 j, w "
                sqlC += "WHERE j.PandaID=w.PandaID  "
                sqlC += "AND NOT j.jobStatus IN (:jobStatus1,:jobStatus2) "
                sqlC += ") "
                var_map = {}
                var_map[":jobStatus1"] = "failed"
                var_map[":jobStatus2"] = "merging"
                var_map[":destinationDBlock"] = destination_block
                statC, resC = taskBuffer.querySQLS(sqlC, var_map)
                # finalize if all jobs have processed
                if resC is not None:
                    num_unprocessed = resC[0][0]
                    tmp_log.debug(f"{num_unprocessed} unprocessed jobs")
                    if num_unprocessed == 0:
                        jobSpecs = taskBuffer.peekJobs(
                            [panda_id],
                            fromDefined=False,
                            fromArchived=False,
                            fromWaiting=False,
                        )
                        jobSpec = jobSpecs[0]
                        if jobSpec is None:
                            tmp_log.debug(f"skip PandaID={panda_id} not found in jobsActive")
                            continue
                        if jobSpec.produceUnMerge():
                            # collect sub datasets
                            sub_ds_names = set()
                            sub_ds_list = []
                            killed_for_bad_record = False
                            for tmpFileSpec in jobSpec.Files:
                                if tmpFileSpec.type in ["log", "output"] and re.search("_sub\d+$", tmpFileSpec.destinationDBlock) is not None:
                                    if tmpFileSpec.destinationDBlock in sub_ds_names:
                                        continue
                                    sub_ds_names.add(tmpFileSpec.destinationDBlock)
                                    datasetSpec = taskBuffer.queryDatasetWithMap({"name": tmpFileSpec.destinationDBlock})
                                    # kill jobs since sub dataset is missing due to failures in setupper etc
                                    if datasetSpec is None:
                                        tmp_log.debug(f"sub dataset {tmpFileSpec.destinationDBlock} is missing")
                                        sql_missing = "SELECT PandaID FROM ATLAS_PANDA.filesTable4 WHERE destinationDBlock=:destinationDBlock "
                                        var_map = {":destinationDBlock": tmpFileSpec.destinationDBlock}
                                        _, res_missing = taskBuffer.querySQLS(sql_missing, var_map)
                                        missing_ids = [p for p, in res_missing]
                                        tmp_log.debug(f"missing {tmpFileSpec.destinationDBlock} to kill {missing_ids}")
                                        Client.killJobs(missing_ids, 2)
                                        killed_for_bad_record = True
                                        break
                                    elif datasetSpec.status == "deleted":
                                        tmp_log.debug(f"sub dataset {tmpFileSpec.destinationDBlock} is deleted")
                                        sql_deleted = (
                                            "SELECT j.PandaID "
                                            "FROM ATLAS_PANDA.jobsActive4 j, ATLAS_PANDA.filesTable4 f "
                                            "WHERE j.PandaID=f.PandaID AND j.jobStatus=:jobStatus "
                                            "AND f.destinationDBlock=:destinationDBlock "
                                        )
                                        var_map = {":jobStatus": "merging", ":destinationDBlock": tmpFileSpec.destinationDBlock}
                                        _, res_deleted = taskBuffer.querySQLS(sql_deleted, var_map)
                                        deleted_ids = [p for p, in res_deleted]
                                        tmp_log.debug(f"deleted {tmpFileSpec.destinationDBlock} to kill {deleted_ids}")
                                        Client.killJobs(deleted_ids, 2)
                                        killed_for_bad_record = True
                                        break
                                    else:
                                        sub_ds_list.append(datasetSpec)
                            # update unmerged datasets to trigger merge job generation
                            if not killed_for_bad_record and sub_ds_list:
                                # check dataset status
                                all_defined = True
                                for datasetSpec in sub_ds_list:
                                    if datasetSpec.status != "defined":
                                        all_defined = False
                                        tmp_log.debug(f"skip to update unmerged datasets since {datasetSpec.name} is {datasetSpec.status}")
                                        break
                                if all_defined:
                                    tmp_log.debug(f"update unmerged datasets {[d.name for d in sub_ds_list]}")
                                    taskBuffer.updateUnmergedDatasets(jobSpec, sub_ds_list)
                else:
                    tmp_log.debug("number of unprocessed jobs unknown")
    except Exception as e:
        _logger.error(f"Kick merging failed with {str(e)} {traceback.format_exc()}")

    # finalize failed jobs
    _logger.debug("check stuck merging jobs")
    try:
        timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=2)
        # get PandaIDs
        var_map = {}
        var_map[":prodSourceLabel"] = "managed"
        var_map[":jobStatus"] = "merging"
        var_map[":timeLimit"] = timeLimit
        sql = "SELECT distinct jediTaskID FROM ATLAS_PANDA.jobsActive4 "
        sql += "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus and modificationTime<:timeLimit "
        tmp, res = taskBuffer.querySQLS(sql, var_map)

        for (jediTaskID,) in res:
            var_map = {}
            var_map[":jediTaskID"] = jediTaskID
            var_map[":dsType"] = "trn_log"
            sql = "SELECT datasetID FROM ATLAS_PANDA.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type=:dsType AND nFilesUsed=nFilesTobeUsed "
            tmpP, resD = taskBuffer.querySQLS(sql, var_map)
            for (datasetID,) in resD:
                var_map = {}
                var_map[":jediTaskID"] = jediTaskID
                var_map[":fileStatus"] = "ready"
                var_map[":datasetID"] = datasetID
                sql = "SELECT PandaID FROM ATLAS_PANDA.JEDI_Dataset_Contents "
                sql += "WHERE jediTaskID=:jediTaskID AND datasetid=:datasetID AND status=:fileStatus AND PandaID=OutPandaID AND rownum<=1 "
                tmpP, resP = taskBuffer.querySQLS(sql, var_map)
                if resP == []:
                    continue
                PandaID = resP[0][0]
                var_map = {}
                var_map[":PandaID"] = PandaID
                var_map[":fileType"] = "log"
                sql = "SELECT d.status FROM ATLAS_PANDA.filesTable4 f,ATLAS_PANDA.datasets d WHERE PandaID=:PandaID AND f.type=:fileType AND d.name=f.destinationDBlock "
                tmpS, resS = taskBuffer.querySQLS(sql, var_map)
                if resS is not None:
                    (subStatus,) = resS[0]
                    if subStatus in ["completed"]:
                        jobSpecs = taskBuffer.peekJobs(
                            [PandaID],
                            fromDefined=False,
                            fromArchived=False,
                            fromWaiting=False,
                        )
                        jobSpec = jobSpecs[0]
                        sub_ds_names = set()
                        sub_ds_list = []
                        for tmpFileSpec in jobSpec.Files:
                            if tmpFileSpec.type in ["log", "output"] and re.search("_sub\d+$", tmpFileSpec.destinationDBlock) is not None:
                                if tmpFileSpec.destinationDBlock in sub_ds_names:
                                    continue
                                sub_ds_names.add(tmpFileSpec.destinationDBlock)
                                datasetSpec = taskBuffer.queryDatasetWithMap({"name": tmpFileSpec.destinationDBlock})
                                sub_ds_list.append(datasetSpec)
                        _logger.debug(f"update unmerged datasets for jediTaskID={jediTaskID} PandaID={PandaID}")
                        taskBuffer.updateUnmergedDatasets(jobSpec, sub_ds_list, updateCompleted=True)
    except Exception:
        errType, errValue = sys.exc_info()[:2]
        _logger.error(f"check for stuck merging jobs failed with {errType} {errValue}")

    # get sites to skip various timeout
    var_map = {}
    var_map[":status"] = "paused"
    sql = "SELECT /* use_json_type */ panda_queue FROM ATLAS_PANDA.schedconfig_json scj WHERE scj.data.status=:status "
    sitesToSkipTO = set()
    status, res = taskBuffer.querySQLS(sql, var_map)
    for (siteid,) in res:
        sitesToSkipTO.add(siteid)
    _logger.debug(f"PQs to skip timeout : {','.join(sitesToSkipTO)}")

    sitesToDisableReassign = set()
    # get sites to disable reassign
    for siteName in siteMapper.siteSpecList:
        siteSpec = siteMapper.siteSpecList[siteName]
        if siteSpec.capability == "ucore" and not siteSpec.is_unified:
            continue
        if siteSpec.disable_reassign():
            sitesToDisableReassign.add(siteName)
    _logger.debug(f"PQs to disable reassign : {','.join(sitesToDisableReassign)}")

    _memoryCheck("watcher")

    _logger.debug("Watcher session")

    # get the list of workflows
    sql = "SELECT /* use_json_type */ DISTINCT scj.data.workflow FROM ATLAS_PANDA.schedconfig_json scj WHERE scj.data.status='online' "
    status, res = taskBuffer.querySQLS(sql, {})
    workflow_timeout_map = {}
    for (workflow,) in res + [("production",), ("analysis",)]:
        timeout = taskBuffer.getConfigValue("watcher", f"HEARTBEAT_TIMEOUT_{workflow}", "pandaserver", "atlas")
        if timeout is not None:
            workflow_timeout_map[workflow] = timeout
        elif workflow in ["production", "analysis"]:
            workflow_timeout_map[workflow] = 2

    workflows = list(workflow_timeout_map)

    _logger.debug(f"timeout : {str(workflow_timeout_map)}")

    # check heartbeat for analysis jobs
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=workflow_timeout_map["analysis"])
    var_map = {}
    var_map[":modificationTime"] = timeLimit
    var_map[":prodSourceLabel1"] = "panda"
    var_map[":prodSourceLabel2"] = "user"
    var_map[":jobStatus1"] = "running"
    var_map[":jobStatus2"] = "starting"
    var_map[":jobStatus3"] = "stagein"
    var_map[":jobStatus4"] = "stageout"
    sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE (prodSourceLabel=:prodSourceLabel1 OR prodSourceLabel=:prodSourceLabel2) "
    sql += "AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3,:jobStatus4) AND modificationTime<:modificationTime"
    status, res = taskBuffer.querySQLS(sql, var_map)
    if res is None:
        _logger.debug(f"# of Anal Watcher : {res}")
    else:
        _logger.debug(f"# of Anal Watcher : {len(res)}")
        for (id,) in res:
            _logger.debug(f"Anal Watcher {id}")
            thr = Watcher(taskBuffer, id, single=True, sleepTime=60, sitemapper=siteMapper)
            thr.start()
            thr.join()

    # check heartbeat for analysis jobs in transferring
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=workflow_timeout_map["analysis"])
    var_map = {}
    var_map[":modificationTime"] = timeLimit
    var_map[":prodSourceLabel1"] = "panda"
    var_map[":prodSourceLabel2"] = "user"
    var_map[":jobStatus1"] = "transferring"
    sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
    sql += "AND jobStatus=:jobStatus1 AND modificationTime<:modificationTime"
    status, res = taskBuffer.querySQLS(sql, var_map)
    if res is None:
        _logger.debug(f"# of Transferring Anal Watcher : {res}")
    else:
        _logger.debug(f"# of Transferring Anal Watcher : {len(res)}")
        for (id,) in res:
            _logger.debug(f"Trans Anal Watcher {id}")
            thr = Watcher(taskBuffer, id, single=True, sleepTime=60, sitemapper=siteMapper)
            thr.start()
            thr.join()

    # check heartbeat for sent jobs
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=30)
    var_map = {}
    var_map[":jobStatus"] = "sent"
    var_map[":modificationTime"] = timeLimit
    status, res = taskBuffer.querySQLS(
        "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND modificationTime<:modificationTime",
        var_map,
    )
    if res is None:
        _logger.debug(f"# of Sent Watcher : {res}")
    else:
        _logger.debug(f"# of Sent Watcher : {len(res)}")
        for (id,) in res:
            _logger.debug(f"Sent Watcher {id}")
            thr = Watcher(taskBuffer, id, single=True, sleepTime=30, sitemapper=siteMapper)
            thr.start()
            thr.join()

    # check heartbeat for 'holding' analysis/ddm jobs
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=3)
    # get XMLs
    xmlIDs = set()
    # xmlFiles = os.listdir(panda_config.logdir)
    # for file in xmlFiles:
    #     match = re.search('^(\d+)_([^_]+)_.{36}$',file)
    #     if match is not None:
    #         id = match.group(1)
    #         xmlIDs.append(int(id))
    job_output_report_list = taskBuffer.listJobOutputReport()
    if job_output_report_list is not None:
        for panda_id, job_status, attempt_nr, time_stamp in job_output_report_list:
            xmlIDs.add(int(panda_id))
    sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND (modificationTime<:modificationTime OR (endTime IS NOT NULL AND endTime<:endTime)) AND (prodSourceLabel=:prodSourceLabel1 OR prodSourceLabel=:prodSourceLabel2) AND stateChangeTime != modificationTime"
    var_map = {}
    var_map[":modificationTime"] = timeLimit
    var_map[":endTime"] = timeLimit
    var_map[":jobStatus"] = "holding"
    var_map[":prodSourceLabel1"] = "panda"
    var_map[":prodSourceLabel2"] = "user"

    status, res = taskBuffer.querySQLS(sql, var_map)
    if res is None:
        _logger.debug(f"# of Holding Anal/DDM Watcher : {res}")
    else:
        _logger.debug(f"# of Holding Anal/DDM Watcher : {len(res)} - XMLs : {len(xmlIDs)}")
        for (id,) in res:
            _logger.debug(f"Holding Anal/DDM Watcher {id}")
            if int(id) in xmlIDs:
                _logger.debug(f"   found XML -> skip {id}")
                continue
            thr = Watcher(taskBuffer, id, single=True, sleepTime=180, sitemapper=siteMapper)
            thr.start()
            thr.join()

    # check heartbeat for high prio production jobs
    timeOutVal = 3
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=timeOutVal)
    sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND currentPriority>:pLimit "
    sql += "AND (modificationTime<:modificationTime OR (endTime IS NOT NULL AND endTime<:endTime))"
    var_map = {}
    var_map[":modificationTime"] = timeLimit
    var_map[":endTime"] = timeLimit
    var_map[":jobStatus"] = "holding"
    var_map[":pLimit"] = 800
    status, res = taskBuffer.querySQLS(sql, var_map)
    if res is None:
        _logger.debug(f"# of High prio Holding Watcher : {res}")
    else:
        _logger.debug(f"# of High prio Holding Watcher : {len(res)}")
        for (id,) in res:
            _logger.debug(f"High prio Holding Watcher {id}")
            thr = Watcher(
                taskBuffer,
                id,
                single=True,
                sleepTime=60 * timeOutVal,
                sitemapper=siteMapper,
            )
            thr.start()
            thr.join()

    # check heartbeat for production jobs
    timeOutVal = taskBuffer.getConfigValue("job_timeout", "TIMEOUT_holding", "pandaserver")
    if not timeOutVal:
        timeOutVal = 48
    timeOutVal *= 60
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=timeOutVal)
    sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND (modificationTime<:modificationTime OR (endTime IS NOT NULL AND endTime<:endTime))"
    var_map = {}
    var_map[":modificationTime"] = timeLimit
    var_map[":endTime"] = timeLimit
    var_map[":jobStatus"] = "holding"
    status, res = taskBuffer.querySQLS(sql, var_map)
    if res is None:
        _logger.debug(f"# of Holding Watcher with timeout {timeOutVal}min: {str(res)}")
    else:
        _logger.debug(f"# of Holding Watcher with timeout {timeOutVal}min: {len(res)}")
        for (id,) in res:
            _logger.debug(f"Holding Watcher {id}")
            thr = Watcher(taskBuffer, id, single=True, sleepTime=timeOutVal, sitemapper=siteMapper)
            thr.start()
            thr.join()

    # check heartbeat for production jobs
    sql = (
        "SELECT /* use_json_type */ PandaID, jobStatus, j.computingSite FROM ATLAS_PANDA.jobsActive4 j "
        "LEFT JOIN ATLAS_PANDA.schedconfig_json s ON j.computingSite=s.panda_queue "
        "WHERE jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3,:jobStatus4) "
        "AND modificationTime<:modificationTime "
    )
    for workflow in workflows:
        if workflow == "analysis":
            continue
        var_map = {}
        var_map[":jobStatus1"] = "running"
        var_map[":jobStatus2"] = "starting"
        var_map[":jobStatus3"] = "stagein"
        var_map[":jobStatus4"] = "stageout"
        sqlX = sql
        if workflow == "production":
            if len(workflows) > 2:
                sqlX += "AND (s.data.workflow IS NULL OR s.data.workflow NOT IN ("
                for ng_workflow in workflows:
                    if ng_workflow in ["production", "analysis"]:
                        continue
                    tmp_key = f":w_{ng_workflow}"
                    var_map[tmp_key] = ng_workflow
                    sqlX += f"{tmp_key},"
                sqlX = sqlX[:-1]
                sqlX += ")) "
        else:
            tmp_key = f":w_{workflow}"
            sqlX += f"AND s.data.workflow={tmp_key} "
            var_map[tmp_key] = workflow
        timeOutVal = workflow_timeout_map[workflow]
        timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=timeOutVal)
        var_map[":modificationTime"] = timeLimit
        status, res = taskBuffer.querySQLS(sqlX, var_map)
        if res is None:
            _logger.debug(f"# of General Watcher with workflow={workflow}: {res}")
        else:
            _logger.debug(f"# of General Watcher with workflow={workflow}: {len(res)}")
            for pandaID, jobStatus, computingSite in res:
                if computingSite in sitesToSkipTO:
                    _logger.debug(f"skip General Watcher for PandaID={pandaID} at {computingSite} since timeout is disabled for {jobStatus}")
                    continue
                _logger.debug(f"General Watcher {pandaID}")
                thr = Watcher(
                    taskBuffer,
                    pandaID,
                    single=True,
                    sleepTime=60 * timeOutVal,
                    sitemapper=siteMapper,
                )
                thr.start()
                thr.join()

    _memoryCheck("reassign")

    # kill long-waiting jobs in defined table
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=7)
    status, res = taskBuffer.querySQLS(
        "SELECT PandaID,cloud,prodSourceLabel FROM ATLAS_PANDA.jobsDefined4 WHERE creationTime<:creationTime",
        {":creationTime": timeLimit},
    )
    jobs = []
    dashFileMap = {}
    if res is not None:
        for pandaID, cloud, prodSourceLabel in res:
            # collect PandaIDs
            jobs.append(pandaID)
    if len(jobs):
        _logger.debug(f"killJobs for Defined ({str(jobs)})")
        Client.killJobs(jobs, 2)

    # kill long-waiting jobs in active table
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=7)
    var_map = {}
    var_map[":jobStatus"] = "activated"
    var_map[":creationTime"] = timeLimit
    status, res = taskBuffer.querySQLS(
        "SELECT PandaID from ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND creationTime<:creationTime",
        var_map,
    )
    jobs = []
    if res is not None:
        for (id,) in res:
            jobs.append(id)
    if len(jobs):
        _logger.debug(f"killJobs for Active ({str(jobs)})")
        Client.killJobs(jobs, 2)

    # fast rebrokerage at PQs where Nq/Nr overshoots
    _logger.debug("fast rebrokerage at PQs where Nq/Nr overshoots")
    try:
        ratioLimit = taskBuffer.getConfigValue("rebroker", "FAST_REBRO_THRESHOLD_NQNR_RATIO")
        fractionLimit = taskBuffer.getConfigValue("rebroker", "FAST_REBRO_THRESHOLD_NQUEUE_FRAC")
        if not ratioLimit:
            ratioLimit = 3
        if not fractionLimit:
            fractionLimit = 0.3

        # get overloaded PQs
        sql = (
            "SELECT COMPUTINGSITE,JOBSTATUS,GSHARE,SUM(NJOBS) FROM ATLAS_PANDA.{} "
            "WHERE workqueue_id NOT IN "
            "(SELECT queue_id FROM ATLAS_PANDA.jedi_work_queue WHERE queue_function = 'Resource') "
            "AND computingsite NOT IN "
            "(SELECT pandaqueuename FROM ATLAS_PANDA.HARVESTER_Slots) GROUP BY COMPUTINGSITE,JOBSTATUS,GSHARE "
        )

        statsPerShare = {}
        statsPerPQ = {}
        for table in ["JOBS_SHARE_STATS", "JOBSDEFINED_SHARE_STATS"]:
            status, res = taskBuffer.querySQLS(sql.format(table), {})
            for computingSite, jobStatus, gshare, nJobs in res:
                statsPerShare.setdefault(gshare, {"nq": 0, "nr": 0})
                statsPerPQ.setdefault(computingSite, {})
                statsPerPQ[computingSite].setdefault(gshare, {"nq": 0, "nr": 0})
                if jobStatus in ["defined", "assigned", "activated", "starting"]:
                    statsPerPQ[computingSite][gshare]["nq"] += nJobs
                    statsPerShare[gshare]["nq"] += nJobs
                elif jobStatus == "running":
                    statsPerPQ[computingSite][gshare]["nr"] += nJobs
                    statsPerShare[gshare]["nr"] += nJobs

        # check
        sql = (
            "SELECT * FROM ("
            "SELECT * FROM ("
            "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
            "WHERE computingSite=:computingSite "
            "AND gshare=:gshare AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3,:jobStatus4) "
            "UNION "
            "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
            "WHERE computingSite=:computingSite "
            "AND gshare=:gshare AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3,:jobStatus4) "
            ") ORDER BY PandaID "
            ") WHERE rownum<:nRows "
        )
        nQueueLimitMap = {}
        for computingSite, shareStat in statsPerPQ.items():
            for gshare, nStat in shareStat.items():
                # get limit
                if gshare not in nQueueLimitMap:
                    key = f"FAST_REBRO_THRESHOLD_NQUEUE_{gshare}"
                    nQueueLimitMap[gshare] = taskBuffer.getConfigValue("rebroker", key)
                nQueueLimit = nQueueLimitMap[gshare]
                if not nQueueLimit:
                    dry_run = True
                    nQueueLimit = 10
                else:
                    dry_run = False
                ratioCheck = nStat["nr"] * ratioLimit < nStat["nq"]
                statCheck = nStat["nq"] > nQueueLimit
                fracCheck = nStat["nq"] > statsPerShare[gshare]["nq"] * fractionLimit
                _logger.debug(
                    "{} in {} : nQueue({})>nRun({})*{}: {},"
                    " nQueue>nQueueThreshold({}):{}, nQueue>nQueue_total({})*{}:{}".format(
                        computingSite,
                        gshare,
                        nStat["nq"],
                        nStat["nr"],
                        ratioLimit,
                        ratioCheck,
                        nQueueLimit,
                        statCheck,
                        statsPerShare[gshare]["nq"],
                        fractionLimit,
                        fracCheck,
                    )
                )
                if ratioCheck and statCheck and fracCheck:
                    _logger.debug(f"{computingSite} overshoot in {gshare}")
                    if not dry_run:
                        # calculate excess
                        excess = min(
                            nStat["nq"] - nStat["nr"] * ratioLimit,
                            nStat["nq"] - nQueueLimit,
                        )
                        excess = min(
                            excess,
                            nStat["nq"] - statsPerShare[gshare]["nq"] * fractionLimit,
                        )
                        excess = int(math.ceil(excess))
                        var_map = {}
                        var_map[":computingSite"] = computingSite
                        var_map[":gshare"] = gshare
                        var_map[":jobStatus1"] = "defined"
                        var_map[":jobStatus2"] = "assigned"
                        var_map[":jobStatus3"] = "activated"
                        var_map[":jobStatus4"] = "starting"
                        var_map[":nRows"] = excess
                        status, res = taskBuffer.querySQLS(sql, var_map)
                        jediJobs = [p for p, in res]
                        _logger.debug(f"got {len(jediJobs)} jobs to kill excess={excess}")
                        if jediJobs:
                            nJob = 100
                            iJob = 0
                            while iJob < len(jediJobs):
                                _logger.debug(f"reassignJobs for JEDI at Nq/Nr overshoot site {computingSite} ({str(jediJobs[iJob:iJob + nJob])})")
                                Client.killJobs(jediJobs[iJob : iJob + nJob], 10, keepUnmerged=True)
                                iJob += nJob
    except Exception as e:
        _logger.error(f"failed with {str(e)} {traceback.format_exc()}")

    # reassign activated jobs in inactive sites
    inactiveTimeLimitSite = 2
    inactiveTimeLimitJob = 4
    inactivePrioLimit = 800
    timeLimitSite = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=inactiveTimeLimitSite)
    timeLimitJob = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=inactiveTimeLimitJob)
    # get PandaIDs
    sql = "SELECT distinct computingSite FROM ATLAS_PANDA.jobsActive4 "
    sql += "WHERE prodSourceLabel=:prodSourceLabel "
    sql += "AND ((modificationTime<:timeLimit AND jobStatus=:jobStatus1) "
    sql += "OR (stateChangeTime<:timeLimit AND jobStatus=:jobStatus2)) "
    sql += "AND lockedby=:lockedby AND currentPriority>=:prioLimit "
    sql += "AND NOT processingType IN (:pType1) AND relocationFlag<>:rFlag1 "
    var_map = {}
    var_map[":prodSourceLabel"] = "managed"
    var_map[":jobStatus1"] = "activated"
    var_map[":jobStatus2"] = "starting"
    var_map[":lockedby"] = "jedi"
    var_map[":timeLimit"] = timeLimitJob
    var_map[":prioLimit"] = inactivePrioLimit
    var_map[":pType1"] = "pmerge"
    var_map[":rFlag1"] = 2
    stDS, resDS = taskBuffer.querySQLS(sql, var_map)
    sqlSS = "SELECT laststart FROM ATLAS_PANDAMETA.siteData "
    sqlSS += "WHERE site=:site AND flag=:flag AND hours=:hours "
    sqlPI = "SELECT PandaID,eventService,attemptNr FROM ATLAS_PANDA.jobsActive4 "
    sqlPI += "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) "
    sqlPI += "AND (modificationTime<:timeLimit OR stateChangeTime<:timeLimit) "
    sqlPI += "AND lockedby=:lockedby AND currentPriority>=:prioLimit "
    sqlPI += "AND computingSite=:site AND NOT processingType IN (:pType1) AND relocationFlag<>:rFlag1 "
    for (tmpSite,) in resDS:
        if tmpSite in sitesToDisableReassign:
            _logger.debug(f"skip reassignJobs at inactive site {tmpSite} since reassign is disabled")
            continue
        # check if the site is inactive
        var_map = {}
        var_map[":site"] = tmpSite
        var_map[":flag"] = "production"
        var_map[":hours"] = 3
        stSS, resSS = taskBuffer.querySQLS(sqlSS, var_map)
        if resSS is not None and len(resSS) > 0:
            last_start = resSS[0][0]
        else:
            last_start = None
        site_status = siteMapper.getSite(tmpSite).status
        if stSS is True and ((last_start is not None and last_start < timeLimitSite) or site_status in ["offline", "test"]):
            # get jobs
            var_map = {}
            var_map[":prodSourceLabel"] = "managed"
            var_map[":jobStatus1"] = "activated"
            var_map[":jobStatus2"] = "starting"
            var_map[":lockedby"] = "jedi"
            var_map[":timeLimit"] = timeLimitJob
            var_map[":prioLimit"] = inactivePrioLimit
            var_map[":site"] = tmpSite
            var_map[":pType1"] = "pmerge"
            var_map[":rFlag1"] = 2
            stPI, resPI = taskBuffer.querySQLS(sqlPI, var_map)
            jediJobs = []
            # reassign
            _logger.debug(f"reassignJobs for JEDI at inactive site {tmpSite} laststart={last_start} status={site_status}")
            if resPI is not None:
                for pandaID, eventService, attemptNr in resPI:
                    if eventService in [EventServiceUtils.esMergeJobFlagNumber]:
                        _logger.debug(f"retrying es merge {pandaID} at inactive site {tmpSite}")
                        taskBuffer.retryJob(
                            pandaID,
                            {},
                            getNewPandaID=True,
                            attemptNr=attemptNr,
                            recoverableEsMerge=True,
                        )
                    jediJobs.append(pandaID)
            if len(jediJobs) != 0:
                nJob = 100
                iJob = 0
                while iJob < len(jediJobs):
                    _logger.debug(f"reassignJobs for JEDI at inactive site {tmpSite} ({jediJobs[iJob:iJob + nJob]})")
                    Client.killJobs(jediJobs[iJob : iJob + nJob], 51, keepUnmerged=True)
                    iJob += nJob

    # reassign defined jobs in defined table
    timeoutValue = taskBuffer.getConfigValue("job_timeout", "TIMEOUT_defined", "pandaserver")
    if not timeoutValue:
        timeoutValue = 4
    timeoutValue *= 60
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=timeoutValue)
    # get PandaIDs
    status, res = taskBuffer.lockJobsForReassign(
        "ATLAS_PANDA.jobsDefined4",
        timeLimit,
        ["defined"],
        ["managed", "test"],
        [],
        [],
        [],
        True,
    )
    jediJobs = []
    if res is not None:
        for id, lockedby in res:
            if lockedby == "jedi":
                jediJobs.append(id)

    # reassign
    _logger.debug(f"reassignJobs for JEDI defined jobs -> #{len(jediJobs)}")
    if len(jediJobs) != 0:
        nJob = 100
        iJob = 0
        while iJob < len(jediJobs):
            _logger.debug(f"reassignJobs for JEDI defined jobs ({jediJobs[iJob:iJob + nJob]})")
            Client.killJobs(jediJobs[iJob : iJob + nJob], 51, keepUnmerged=True)
            iJob += nJob

    # reassign stalled defined build and non-JEDI jobs
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=timeoutValue)
    var_map = {}
    var_map[":jobStatus"] = "defined"
    var_map[":prodSourceLabel_p"] = "panda"
    var_map[":timeLimit"] = timeLimit
    status, res = taskBuffer.querySQLS(
        "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE ((prodSourceLabel=:prodSourceLabel_p AND transformation LIKE '%build%') OR "
        "lockedBy IS NULL) AND jobStatus=:jobStatus AND creationTime<:timeLimit ORDER BY PandaID",
        var_map,
    )
    jobs = []
    if res is not None:
        for (id,) in res:
            jobs.append(id)
    # kill
    if len(jobs):
        Client.killJobs(jobs, 2)
        _logger.debug(f"reassign stalled defined build and non-JEDI jobs with timeout {timeoutValue}min ({str(jobs)})")

    # reassign long-waiting jobs in defined table
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=12)
    status, res = taskBuffer.lockJobsForReassign("ATLAS_PANDA.jobsDefined4", timeLimit, [], ["managed"], [], [], [], True)
    jediJobs = []
    if res is not None:
        for id, lockedby in res:
            if lockedby == "jedi":
                jediJobs.append(id)
            else:
                jobs.append(id)
    # reassign
    _logger.debug(f"reassignJobs for long JEDI in defined table -> #{len(jediJobs)}")
    if len(jediJobs) != 0:
        nJob = 100
        iJob = 0
        while iJob < len(jediJobs):
            _logger.debug(f"reassignJobs for long JEDI in defined table ({jediJobs[iJob:iJob + nJob]})")
            Client.killJobs(jediJobs[iJob : iJob + nJob], 51, keepUnmerged=True)
            iJob += nJob

    # reassign too long activated jobs in active table
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=2)
    status, res = taskBuffer.lockJobsForReassign(
        "ATLAS_PANDA.jobsActive4",
        timeLimit,
        ["activated"],
        ["managed"],
        [],
        [],
        [],
        True,
        onlyReassignable=True,
        getEventService=True,
    )
    jediJobs = []
    if res is not None:
        for pandaID, lockedby, eventService, attemptNr, computingSite in res:
            if computingSite in sitesToDisableReassign:
                _logger.debug(f"skip reassignJobs for long activated PandaID={pandaID} since disabled at {computingSite}")
                continue
            if lockedby == "jedi":
                if eventService in [EventServiceUtils.esMergeJobFlagNumber]:
                    _logger.debug("retrying {0} in long activated" % pandaID)
                    taskBuffer.retryJob(
                        pandaID,
                        {},
                        getNewPandaID=True,
                        attemptNr=attemptNr,
                        recoverableEsMerge=True,
                    )
                jediJobs.append(pandaID)

    _logger.debug(f"reassignJobs for long activated JEDI in active table -> #{len(jediJobs)}")
    if len(jediJobs) != 0:
        nJob = 100
        iJob = 0
        while iJob < len(jediJobs):
            _logger.debug(f"reassignJobs for long activated JEDI in active table ({jediJobs[iJob:iJob + nJob]})")
            Client.killJobs(jediJobs[iJob : iJob + nJob], 51, keepUnmerged=True)
            iJob += nJob

    # reassign too long starting jobs in active table
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=48)
    status, res = taskBuffer.lockJobsForReassign(
        "ATLAS_PANDA.jobsActive4",
        timeLimit,
        ["starting"],
        ["managed"],
        [],
        [],
        [],
        True,
        onlyReassignable=True,
        useStateChangeTime=True,
        getEventService=True,
    )
    jediJobs = []
    if res is not None:
        for pandaID, lockedby, eventService, attemptNr, computingSite in res:
            if computingSite in sitesToDisableReassign:
                _logger.debug(f"skip reassignJobs for long starting PandaID={pandaID} since disabled at {computingSite}")
                continue
            if lockedby == "jedi":
                jediJobs.append(pandaID)

    _logger.debug(f"reassignJobs for long starting JEDI in active table -> #{len(jediJobs)}")
    if len(jediJobs) != 0:
        nJob = 100
        iJob = 0
        while iJob < len(jediJobs):
            _logger.debug(f"reassignJobs for long stating JEDI in active table ({jediJobs[iJob:iJob + nJob]})")
            Client.killJobs(jediJobs[iJob : iJob + nJob], 51, keepUnmerged=True)
            iJob += nJob

    # kill too long-standing analysis jobs in active table
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=7)
    var_map = {}
    var_map[":prodSourceLabel1"] = "test"
    var_map[":prodSourceLabel2"] = "panda"
    var_map[":prodSourceLabel3"] = "user"
    var_map[":modificationTime"] = timeLimit
    status, res = taskBuffer.querySQLS(
        "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE (prodSourceLabel=:prodSourceLabel1 OR prodSourceLabel=:prodSourceLabel2 OR prodSourceLabel=:prodSourceLabel3) AND modificationTime<:modificationTime ORDER BY PandaID",
        var_map,
    )
    jobs = []
    if res is not None:
        for (id,) in res:
            jobs.append(id)
    # kill
    if len(jobs):
        Client.killJobs(jobs, 2)
        _logger.debug(f"killJobs for Anal Active ({str(jobs)})")

    # kill too long pending jobs
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=1)
    var_map = {}
    var_map[":jobStatus"] = "pending"
    var_map[":creationTime"] = timeLimit
    status, res = taskBuffer.querySQLS(
        "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE jobStatus=:jobStatus AND creationTime<:creationTime ",
        var_map,
    )
    jobs = []
    if res is not None:
        for (id,) in res:
            jobs.append(id)
    # kill
    if len(jobs):
        if len(jobs):
            nJob = 100
            iJob = 0
            while iJob < len(jobs):
                _logger.debug(f"killJobs for Pending ({str(jobs[iJob:iJob + nJob])})")
                Client.killJobs(jobs[iJob : iJob + nJob], 4)
                iJob += nJob

    # kick waiting ES merge jobs which were generated from fake co-jumbo
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=10)
    var_map = {}
    var_map[":jobStatus"] = "waiting"
    var_map[":creationTime"] = timeLimit
    var_map[":esMerge"] = EventServiceUtils.esMergeJobFlagNumber
    sql = "SELECT PandaID,computingSite FROM ATLAS_PANDA.jobsDefined4 WHERE jobStatus=:jobStatus AND creationTime<:creationTime "
    sql += "AND eventService=:esMerge ORDER BY jediTaskID "
    status, res = taskBuffer.querySQLS(sql, var_map)
    jobsMap = {}
    if res is not None:
        for id, site in res:
            if site not in jobsMap:
                jobsMap[site] = []
            jobsMap[site].append(id)
    # kick
    if len(jobsMap):
        for site in jobsMap:
            jobs = jobsMap[site]
            nJob = 100
            iJob = 0
            while iJob < len(jobs):
                _logger.debug(f"kick waiting ES merge ({str(jobs[iJob:iJob + nJob])})")
                Client.killJobs(jobs[iJob : iJob + nJob], 2)
                iJob += nJob

    # kill too long waiting jobs
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=1)
    var_map = {}
    var_map[":jobStatus"] = "waiting"
    var_map[":creationTime"] = timeLimit
    var_map[":coJumbo"] = EventServiceUtils.coJumboJobFlagNumber
    sql = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE jobStatus=:jobStatus AND creationTime<:creationTime "
    sql += "AND (eventService IS NULL OR eventService<>:coJumbo) "
    status, res = taskBuffer.querySQLS(sql, var_map)
    jobs = []
    if res is not None:
        for (id,) in res:
            jobs.append(id)
    # kill
    if len(jobs):
        if len(jobs):
            nJob = 100
            iJob = 0
            while iJob < len(jobs):
                _logger.debug(f"killJobs for Waiting ({str(jobs[iJob:iJob + nJob])})")
                Client.killJobs(jobs[iJob : iJob + nJob], 4)
                iJob += nJob

    # kill too long running ES jobs
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=24)
    var_map = {}
    var_map[":jobStatus1"] = "running"
    var_map[":jobStatus2"] = "starting"
    var_map[":timeLimit"] = timeLimit
    var_map[":esJob"] = EventServiceUtils.esJobFlagNumber
    var_map[":coJumbo"] = EventServiceUtils.coJumboJobFlagNumber
    sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus IN (:jobStatus1,:jobStatus2) AND stateChangeTime<:timeLimit "
    sql += "AND eventService IN (:esJob,:coJumbo) AND currentPriority>=900 "
    status, res = taskBuffer.querySQLS(sql, var_map)
    jobs = []
    if res is not None:
        for (id,) in res:
            jobs.append(id)
    # kill
    if len(jobs):
        nJob = 100
        iJob = 0
        while iJob < len(jobs):
            _logger.debug(f"killJobs for long running ES jobs ({str(jobs[iJob:iJob + nJob])})")
            Client.killJobs(
                jobs[iJob : iJob + nJob],
                2,
                keepUnmerged=True,
                jobSubStatus="es_toolong",
            )
            iJob += nJob

    # kill too long running ES merge jobs
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=24)
    var_map = {}
    var_map[":jobStatus1"] = "running"
    var_map[":jobStatus2"] = "starting"
    var_map[":timeLimit"] = timeLimit
    var_map[":esMergeJob"] = EventServiceUtils.esMergeJobFlagNumber
    sql = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus IN (:jobStatus1,:jobStatus2) AND stateChangeTime<:timeLimit "
    sql += "AND eventService=:esMergeJob "
    status, res = taskBuffer.querySQLS(sql, var_map)
    jobs = []
    if res is not None:
        for (id,) in res:
            jobs.append(id)
    # kill
    if len(jobs):
        nJob = 100
        iJob = 0
        while iJob < len(jobs):
            _logger.debug(f"killJobs for long running ES merge jobs ({str(jobs[iJob:iJob + nJob])})")
            Client.killJobs(jobs[iJob : iJob + nJob], 2)
            iJob += nJob

    # rebrokerage
    _logger.debug("Rebrokerage start")

    # get timeout value
    timeoutVal = taskBuffer.getConfigValue("rebroker", "ANALY_TIMEOUT")
    if timeoutVal is None:
        timeoutVal = 12
    _logger.debug(f"timeout value : {timeoutVal}h")
    try:
        normalTimeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=timeoutVal)
        sortTimeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=3)
        sql = (
            "WITH p AS ("
            "SELECT MIN(PandaID) PandaID,jobDefinitionID,prodUserName,prodUserID,computingSite,jediTaskID,processingType,workingGroup "
            "FROM ATLAS_PANDA.jobsActive4 "
            "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
            "AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3) "
            "AND jobsetID IS NOT NULL AND lockedBy=:lockedBy "
            "GROUP BY jobDefinitionID,prodUserName,prodUserID,computingSite,jediTaskID,processingType,workingGroup "
            ") "
            "SELECT /*+ INDEX (s JOBS_STATUSLOG_PANDAID_IDX) */ "
            "p.jobDefinitionID,p.prodUserName,p.prodUserID,p.computingSite,s.modificationTime,p.jediTaskID,p.processingType,p.workingGroup "
            "FROM p, ATLAS_PANDA.jobs_statuslog s "
            "WHERE s.PandaID=p.PandaID AND s.jobStatus=:s_jobStatus AND s.modificationTime<:modificationTime "
        )
        var_map = {}
        var_map[":prodSourceLabel1"] = "user"
        var_map[":prodSourceLabel2"] = "panda"
        var_map[":modificationTime"] = sortTimeLimit
        var_map[":lockedBy"] = "jedi"
        var_map[":jobStatus1"] = "activated"
        var_map[":jobStatus2"] = "dummy"
        var_map[":jobStatus3"] = "starting"
        var_map[":s_jobStatus"] = "activated"
        # get jobs older than threshold
        ret, res = taskBuffer.querySQLS(sql, var_map)
        resList = []
        keyList = set()
        if res is not None:
            for tmpItem in res:
                (
                    jobDefinitionID,
                    prodUserName,
                    prodUserID,
                    computingSite,
                    maxTime,
                    jediTaskID,
                    processingType,
                    workingGroup,
                ) = tmpItem
                tmpKey = (jediTaskID, jobDefinitionID)
                keyList.add(tmpKey)
                resList.append(tmpItem)
        # get stalled assigned job
        sqlA = "SELECT jobDefinitionID,prodUserName,prodUserID,computingSite,MAX(creationTime),jediTaskID,processingType,workingGroup "
        sqlA += "FROM ATLAS_PANDA.jobsDefined4 "
        sqlA += "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND jobStatus IN (:jobStatus1,:jobStatus2) "
        sqlA += "AND creationTime<:modificationTime AND lockedBy=:lockedBy "
        sqlA += "GROUP BY jobDefinitionID,prodUserName,prodUserID,computingSite,jediTaskID,processingType,workingGroup "
        var_map = {}
        var_map[":prodSourceLabel1"] = "user"
        var_map[":prodSourceLabel2"] = "panda"
        var_map[":modificationTime"] = sortTimeLimit
        var_map[":lockedBy"] = "jedi"
        var_map[":jobStatus1"] = "assigned"
        var_map[":jobStatus2"] = "defined"
        retA, resA = taskBuffer.querySQLS(sqlA, var_map)
        if resA is not None:
            for tmpItem in resA:
                (
                    jobDefinitionID,
                    prodUserName,
                    prodUserID,
                    computingSite,
                    maxTime,
                    jediTaskID,
                    processingType,
                    workingGroup,
                ) = tmpItem
                tmpKey = (jediTaskID, jobDefinitionID)
                if tmpKey not in keyList:
                    keyList.add(tmpKey)
                    resList.append(tmpItem)
        # sql to check recent activity
        sql = "SELECT PandaID,stateChangeTime,jobStatus FROM %s "
        sql += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
        sql += "AND computingSite=:computingSite AND jediTaskID=:jediTaskID "
        sql += "AND jobStatus NOT IN (:jobStatus1,:jobStatus2,:jobStatus3) "
        sql += "AND stateChangeTime>:modificationTime "
        sql += "AND rownum <= 1"
        # sql to get associated jobs with jediTaskID
        sqlJJ = "SELECT PandaID FROM %s "
        sqlJJ += "WHERE jediTaskID=:jediTaskID AND jobStatus IN (:jobS1,:jobS2,:jobS3,:jobS4,:jobS5) "
        sqlJJ += "AND jobDefinitionID=:jobDefID AND computingSite=:computingSite "
        timeoutMap = {}
        if resList != []:
            recentRuntimeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=3)
            # loop over all user/jobID combinations
            iComb = 0
            nComb = len(resList)
            _logger.debug(f"total combinations = {nComb}")
            for (
                jobDefinitionID,
                prodUserName,
                prodUserID,
                computingSite,
                maxModificationTime,
                jediTaskID,
                processingType,
                workingGroup,
            ) in resList:
                # check if jobs with the jobID have run recently
                var_map = {}
                var_map[":jediTaskID"] = jediTaskID
                var_map[":computingSite"] = computingSite
                var_map[":prodUserName"] = prodUserName
                var_map[":jobDefinitionID"] = jobDefinitionID
                var_map[":modificationTime"] = recentRuntimeLimit
                var_map[":jobStatus1"] = "closed"
                var_map[":jobStatus2"] = "failed"
                var_map[":jobStatus3"] = "starting"
                _logger.debug(f" rebro:{iComb}/{nComb}:ID={jobDefinitionID}:{prodUserName} jediTaskID={jediTaskID} site={computingSite}")
                iComb += 1
                hasRecentJobs = False
                # check site
                if not siteMapper.checkSite(computingSite):
                    _logger.debug(f"    -> skip unknown site={computingSite}")
                    continue
                # check site status
                tmpSiteStatus = siteMapper.getSite(computingSite).status
                if tmpSiteStatus not in ["offline", "test"]:
                    if workingGroup:
                        if workingGroup not in timeoutMap:
                            tmp_timeoutVal = taskBuffer.getConfigValue("rebroker", f"ANALY_TIMEOUT_{workingGroup}")
                            if tmp_timeoutVal:
                                timeoutMap[workingGroup] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(
                                    hours=tmp_timeoutVal
                                )
                            else:
                                timeoutMap[workingGroup] = normalTimeLimit
                        tmp_normalTimeLimit = timeoutMap[workingGroup]
                    else:
                        tmp_normalTimeLimit = normalTimeLimit
                    # use normal time limit for normal site status
                    if maxModificationTime > tmp_normalTimeLimit:
                        _logger.debug(f"    -> skip wait for normal timelimit={tmp_normalTimeLimit}<maxModTime={maxModificationTime}")
                        continue
                    for tableName in [
                        "ATLAS_PANDA.jobsActive4",
                        "ATLAS_PANDA.jobsArchived4",
                    ]:
                        retU, resU = taskBuffer.querySQLS(sql % tableName, var_map)
                        if resU is None:
                            # database error
                            raise RuntimeError("failed to check modTime")
                        if resU != []:
                            # found recent jobs
                            hasRecentJobs = True
                            _logger.debug(f"    -> skip due to recent activity {resU[0][0]} to {resU[0][2]} at {resU[0][1]}")
                            break
                else:
                    _logger.debug(f"    -> immediate rebro due to site status={tmpSiteStatus}")
                if hasRecentJobs:
                    # skip since some jobs have run recently
                    continue
                else:
                    if jediTaskID is None:
                        _logger.debug("    -> rebro for normal task : no action")
                    else:
                        _logger.debug("    -> rebro for JEDI task")
                        killJobs = []
                        var_map = {}
                        var_map[":jediTaskID"] = jediTaskID
                        var_map[":jobDefID"] = jobDefinitionID
                        var_map[":computingSite"] = computingSite
                        var_map[":jobS1"] = "defined"
                        var_map[":jobS2"] = "assigned"
                        var_map[":jobS3"] = "activated"
                        var_map[":jobS4"] = "dummy"
                        var_map[":jobS5"] = "starting"
                        for tableName in [
                            "ATLAS_PANDA.jobsDefined4",
                            "ATLAS_PANDA.jobsActive4",
                        ]:
                            retJJ, resJJ = taskBuffer.querySQLS(sqlJJ % tableName, var_map)
                            for (tmpPandaID,) in resJJ:
                                killJobs.append(tmpPandaID)
                        # reverse sort to kill buildJob in the end
                        killJobs.sort()
                        killJobs.reverse()
                        # kill to reassign
                        taskBuffer.killJobs(killJobs, "JEDI", "51", True)
    except Exception as e:
        _logger.error(f"rebrokerage failed with {str(e)} : {traceback.format_exc()}")

    # kill too long running jobs
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=21)
    status, res = taskBuffer.querySQLS(
        "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE creationTime<:creationTime",
        {":creationTime": timeLimit},
    )
    jobs = []
    if res is not None:
        for (id,) in res:
            jobs.append(id)
    # kill
    if len(jobs):
        nJob = 100
        iJob = 0
        while iJob < len(jobs):
            # set tobekill
            _logger.debug(f"killJobs for Running ({jobs[iJob:iJob + nJob]})")
            Client.killJobs(jobs[iJob : iJob + nJob], 2)
            # run watcher
            for id in jobs[iJob : iJob + nJob]:
                thr = Watcher(
                    taskBuffer,
                    id,
                    single=True,
                    sitemapper=siteMapper,
                    sleepTime=60 * 24 * 21,
                )
                thr.start()
                thr.join()
                time.sleep(1)
            iJob += nJob
            time.sleep(10)

    # kill too long throttled jobs
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=7)
    var_map = {}
    var_map[":jobStatus"] = "throttled"
    var_map[":creationTime"] = timeLimit
    status, res = taskBuffer.querySQLS(
        "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus AND creationTime<:creationTime ",
        var_map,
    )
    jobs = []
    if res is not None:
        for (id,) in res:
            jobs.append(id)
    # kill
    if len(jobs):
        Client.killJobs(jobs, 2)
        _logger.debug(f"killJobs for throttled ({str(jobs)})")

    # check if merge job is valid
    _logger.debug("kill invalid pmerge")
    var_map = {}
    var_map[":processingType"] = "pmerge"
    var_map[":timeLimit"] = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=30)
    sql = "SELECT PandaID,jediTaskID FROM ATLAS_PANDA.jobsDefined4 WHERE processingType=:processingType AND modificationTime<:timeLimit "
    sql += "UNION "
    sql += "SELECT PandaID,jediTaskID FROM ATLAS_PANDA.jobsActive4 WHERE processingType=:processingType AND modificationTime<:timeLimit "
    status, res = taskBuffer.querySQLS(sql, var_map)
    nPmerge = 0
    badPmerge = 0
    _logger.debug(f"check {len(res)} pmerge")
    for pandaID, jediTaskID in res:
        nPmerge += 1
        isValid, tmpMsg = taskBuffer.isValidMergeJob(pandaID, jediTaskID)
        if isValid is False:
            _logger.debug(f"kill pmerge {pandaID} since {tmpMsg} gone")
            taskBuffer.killJobs(
                [pandaID],
                f"killed since pre-merge job {tmpMsg} gone",
                "52",
                True,
            )
            badPmerge += 1
    _logger.debug(f"killed invalid pmerge {badPmerge}/{nPmerge}")

    # cleanup of jumbo jobs
    _logger.debug("jumbo job cleanup")
    res = taskBuffer.cleanupJumboJobs()
    _logger.debug(res)

    _memoryCheck("delete XML")

    # delete old files in DA cache
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=7)
    files = os.listdir(panda_config.cache_dir)
    for file in files:
        # skip special test file
        if file == "sources.72c48dc5-f055-43e5-a86e-4ae9f8ea3497.tar.gz":
            continue
        if file == "sources.090f3f51-fc81-4e80-9749-a5e4b2bd58de.tar.gz":
            continue
        try:
            # get timestamp
            timestamp = datetime.datetime.fromtimestamp(os.stat(f"{panda_config.cache_dir}/{file}").st_mtime)
            # delete
            if timestamp < timeLimit:
                _logger.debug(f"delete {file} ")
                os.remove(f"{panda_config.cache_dir}/{file}")
        except Exception:
            pass

    _memoryCheck("delete core")

    # delete core
    dirName = f"{panda_config.logdir}/.."
    for file in os.listdir(dirName):
        if file.startswith("core."):
            _logger.debug(f"delete {file} ")
            try:
                os.remove(f"{dirName}/{file}")
            except Exception:
                pass

    # sandbox
    _logger.debug("Touch sandbox")
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=1)
    sqlC = (
        "SELECT hostName,fileName,creationTime,userName FROM ATLAS_PANDAMETA.userCacheUsage "
        "WHERE creationTime>:timeLimit AND creationTime>modificationTime "
        "AND (fileName like 'sources%' OR fileName like 'jobO%') "
    )
    sqlU = "UPDATE ATLAS_PANDAMETA.userCacheUsage SET modificationTime=CURRENT_DATE " "WHERE userName=:userName AND fileName=:fileName "
    status, res = taskBuffer.querySQLS(sqlC, {":timeLimit": timeLimit})
    if res is None:
        _logger.error("failed to get files")
    elif len(res) > 0:
        _logger.debug(f"{len(res)} files to touch")
        for hostName, fileName, creationTime, userName in res:
            base_url = f"https://{hostName}:{panda_config.pserverport}"
            _logger.debug(f"touch {fileName} on {hostName} created at {creationTime}")
            s, o = Client.touchFile(base_url, fileName)
            _logger.debug(o)
            if o == "True":
                var_map = dict()
                var_map[":userName"] = userName
                var_map[":fileName"] = fileName
                taskBuffer.querySQLS(sqlU, var_map)

    _logger.debug("Check sandbox")
    timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=1)
    expireLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=30)
    sqlD = "DELETE FROM ATLAS_PANDAMETA.userCacheUsage WHERE userName=:userName AND fileName=:fileName "
    nRange = 100
    for i in range(nRange):
        _logger.debug(f"{nRange}/{i} {len(res)} files to check")
        res = taskBuffer.getLockSandboxFiles(timeLimit, 1000)
        if res is None:
            _logger.error("failed to get files")
            break
        elif len(res) == 0:
            break
        for userName, hostName, fileName, creationTime, modificationTime in res:
            url = f"https://{hostName}:{panda_config.pserverport}/cache/{fileName}"
            _logger.debug(f"checking {url} created at {creationTime}")
            toDelete = False
            try:
                x = requests.head(url, verify=False)
                _logger.debug(f"code {x.status_code}")
                if x.status_code == 404:
                    _logger.debug("delete")
                    toDelete = True
            except Exception as e:
                _logger.debug(f"failed with {str(e)}")
                if creationTime < expireLimit:
                    toDelete = True
                    _logger.debug(f"delete due to creationTime={creationTime}")
            # update or delete
            var_map = dict()
            var_map[":userName"] = userName
            var_map[":fileName"] = fileName
            if toDelete:
                taskBuffer.querySQLS(sqlD, var_map)
            else:
                _logger.debug("keep")

    _memoryCheck("end")

    # stop taskBuffer if created inside this script
    taskBuffer.cleanup(requester=requester_id)

    _logger.debug("===================== end =====================")


# run
if __name__ == "__main__":
    main(argv=sys.argv)
