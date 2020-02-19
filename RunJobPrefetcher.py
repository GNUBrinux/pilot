# Class definition:
#   RunJobPrefetcher:  module for prefetching and sending files to Athena
#   Instances are generated with RunJobFactory via pUtil::getRunJob()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from RunJob import RunJob                       #Parent RunJob class
from pUtil import writeToFileWithStatus

# Standard python modules
import os
import re
import sys
import random
import time
import atexit
import signal
import commands
import traceback
import uuid
from optparse import OptionParser
from json import loads, dump
from shutil import copy2
from xml.dom import minidom

# Pilot modules
import Job
import Node
import Site
import pUtil
import RunJobUtilities
import Mover as mover
from JobRecovery import JobRecovery
from FileStateClient import getFilesOfState
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from PilotErrors import PilotErrors
from StoppableThread import StoppableThread
from pUtil import tolog, isAnalysisJob, readpar, createLockFile, getDatasetDict,\
     tailPilotErrorDiag, getExperiment, getEventService,\
     getSiteInformation, getGUID
from FileHandling import getExtension, addToOSTransferDictionary, getCPUTimes, getReplicaDictionaryFromXML
from EventRanges import downloadEventRanges, updateEventRange, updateEventRanges
from movers.base import BaseSiteMover

try:
    from PilotYamplServer import PilotYamplServer as MessageServer
except Exception, e:
    MessageServer = None
    tolog("RunJobPrefetcher caught exception: %s" % str(e))
    
class RunJobPrefetcher(RunJob):
    
    # private data members
    __runjob = "RunJobPrefetcher"                # String defining the sub class
    __instance = None                            # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                      # PilotErrors object
    __errorCode = 0                              # Error code, e.g. set by stage-out method
    __experiment = "ATLAS"                       # Current experiment (can be set with pilot option -F <experiment>)
    __pilotserver = "localhost"                  # Default server
    __pilotport = 88888                          # Default port
    __failureCode = None                         # Set by signal handler when user/batch system kills the job
    __pworkdir = "/tmp"                          # Site work dir used by the parent
    __logguid = None                             # GUID for the log file
    __pilotlogfilename = "pilotlog.txt"          # Default pilotlog filename
    __stageinretry = None                        # Number of stage-in tries
    __stageoutretry = None                       # Number of stage-out tries
    __pilot_initdir = ""                         # location of where the pilot is untarred and started
    __proxycheckFlag = True                      # True (default): perform proxy validity checks, False: no check
    __globalPilotErrorDiag = ""                  # Global pilotErrorDiag used with signal handler (only)
    __globalErrorCode = 0                        # Global error code used with signal handler (only)
    __inputDir = ""                              # Location of input files (source for mv site mover)
    __outputDir = ""                             # Location of output files (destination for mv site mover)
    __taskID = ""                                # TaskID (needed for OS transfer file and eventually for job metrics)
    __event_loop_running = False                 # Is the event loop running?
    __output_files = []                          # A list of all files that have been successfully staged-out, used by createFileMetadata()
    __guid_list = []                             # Keep track of downloaded GUIDs
    __lfn_list = []                              # Keep track of downloaded LFNs
    __eventRange_dictionary = {}                 # eventRange_dictionary[event_range_id] = [path, cpu, wall]
    __eventRangeID_dictionary = {}               # eventRangeID_dictionary[event_range_id] = True (corr. output file has been transferred)
    __stageout_queue = []                        # Queue for files to be staged-out; files are added as they arrive and removed after they have been staged-out
    __pfc_path = ""                              # The path to the pool file catalog
    __message_server_payload = None              # Message server for the payload
   __message_server_prefetcher = None           # Message server for Prefetcher
    __message_thread_payload = None              # Thread for listening to messages from the payload
   __message_thread_prefetcher = None           # Thread for listening to messages from the Prefetcher
   __message_server_event = None                    # Message server for runjobevent
   __message_thread_event = None                    # Thread for listening to messages from runjobevent
    __status = True                              # Global job status; will be set to False if an event range or stage-out fails
    __athenamp_is_ready = False                  # True when an AthenaMP worker is ready to process an event range
   __prefetcher_is_ready = False                # True when Prefetcher is ready to receive an event range
   __prefetcher_has_finished = False            # True when Prefetcher has updated an event range which then should be sent to AthenaMP
    __asyncOutputStager_thread = None            #
    __asyncOutputStager_thread_sleep_time = 600  #
    __analysisJob = False                        # True for analysis job
    __jobSite = None                             # Site object
    __siteInfo = None                            # site information
    __node = None
    __job = None                                 # Job object
    __cache = ""                                 # Cache URL, e.g. used by LSST
    __metadata_filename = ""                     # Full path to the metadata file
    __yamplChannelNamePayload = None             # Yampl channel name used by the payload (AthenaMP)
   __yamplChannelNamePrefetcher = None          # Yampl channel name used by the Prefetcher
    __useEventIndex = True                       # Should Event Index be used? If not, a TAG file will be created
    __tokenextractor_input_list_filenane = ""    #
    __sending_event_range = False                # True while event range is being sent to payload
    __current_event_range = ""                   # Event range being sent to payload
    __updated_lfn = ""                           # Updated LFN sent from the Prefetcher
    __useTokenExtractor = False                  # Should the TE be used?
   __usePrefetcher = False                      # Should the Prefetcher be used
    __inFilePosEvtNum = False                    # Use event number ranges relative to in-file position
    __pandaserver = ""                   # Full PanDA server url incl. port and sub dirs
    
    # ES zip
    __esToZip = True
    __multipleBuckets = None
    __numBuckets = 1
    __stageoutStorages = None
    __max_wait_for_one_event = 360	# 6 hours, 360 minutes
    __min_events = 1
    __allowPrefetchEvents = True

    # calculate cpu time, os.times() doesn't report correct value for preempted jobs
    __childProcs = []
    __child_cpuTime = {}

    # record processed events
    __nEvents = 0
    __nEventsW = 0
    __nEventsFailed = 0
    __nEventsFailedStagedOut = 0
    __nStageOutFailures = 0
    __nStageOutSuccessAfterFailure = 0
    __isLastStageOutFailed = False

    # error fatal code
    __esFatalCode = None
    __isKilled = False

    # external stagout time(time after athenaMP terminated)
    __external_stagout_time = 0

    # allow read/download remote inputs if closest RSE is in downtime
    __allow_remote_inputs = False

    # input files
    __input_files = {}
    
# Getter and setter methods

    def getNEvents(self):
        return self.__nEvents, self.__nEventsW, self.__nEventsFailed, self.__nEventsFailedStagedOut

    def getSubStatus(self):
        if not self.__eventRangeID_dictionary:
            return 'no_events'
        if self.__esFatalCode:
            return 'pilot_fatal'
        if self.__nEventsFailed:
            if self.__nEventsFailed < self.__nEventsW:
                return 'partly_failed'
            elif self.__nEventsW == 0:
                return 'all_failed' # 'all_failed'
            else:
                return 'mostly_failed'
        else:
            return 'all_success'

    def getStageOutDetail(self):
        retStr = 'Stageout summary:'
        if 'primary' in self.__stageoutStorages and self.__stageoutStorages['primary']:
           retStr += "primary storage('%s' at '%s'): [success %s, failed %s]" % (self.__stageoutStorages['primary']['activity'],
                                                                                 self.__stageoutStorages['primary']['endpoint'],
                                                                                 self.__stageoutStorages['primary']['success'],
                                                                                 self.__stageoutStorages['primary']['failed'])
        if 'failover' in self.__stageoutStorages and self.__stageoutStorages['failover']:
           retStr += "failover storage('%s' at '%s'): [success %s, failed %s]" % (self.__stageoutStorages['failover']['activity'],
                                                                                  self.__stageoutStorages['failover']['endpoint'],
                                                                                  self.__stageoutStorages['failover']['success'],
                                                                                  self.__stageoutStorages['failover']['failed'])
        return retStr
        
    def setFinalESStatus(self, job):
        if self.__nEventsW < 1 and self.__nStageOutFailures >= 3:
            job.subStatus = 'pilot_failed'
            job.pilotErrorDiag = "Too many stageout failures. (%s)" % self.getStageOutDetail()
            job.result[0] = "failed"
            job.result[2] = self.__error.ERR_ESRECOVERABLE
            job.jobState = "failed"
        elif not self.__eventRangeID_dictionary:
            job.subStatus = 'pilot_noevents'  # 'no_events'
            job.pilotErrorDiag = "Pilot got no events"
            job.result[0] = "failed"
            job.result[2] = self.__error.ERR_NOEVENTS
            job.jobState = "failed"
        elif self.__eventRangeID_dictionary and self.__nEventsW < 1:
            job.subStatus = 'pilot_failed'  # 'no_running_events'
            job.pilotErrorDiag = "Pilot didn't run any events"
            job.result[0] = "failed"
            job.result[2] = self.__error.ERR_UNKNOWN
            job.jobState = "failed"
        elif self.__esFatalCode:
            job.subStatus = 'pilot_failed'
            job.pilotErrorDiag = "AthenaMP fatal error happened. (%s)" % self.getStageOutDetail()
            job.result[0] = "failed"
            job.result[2] = self.__esFatalCode
            job.jobState = "failed"
        elif self.__nEventsFailed:
            if self.__nEventsW == 0:
                job.subStatus = 'pilot_failed' # all failed
                job.pilotErrorDiag = "All events failed. (%s, other failure: %s)" % (self.getStageOutDetail(), self.__nEventsFailed - self.__nEventsFailedStagedOut)
                job.result[0] = "failed"
                job.result[2] = self.__error.ERR_ESRECOVERABLE
                job.jobState = "failed"
            elif self.__nEventsFailed < self.__nEventsW:
                job.subStatus = 'partly_failed'
                job.pilotErrorDiag = "Part of events failed. (%s, other failure: %s)" % (self.getStageOutDetail(), self.__nEventsFailed - self.__nEventsFailedStagedOut)
                job.result[0] = "failed"
                job.result[2] = self.__error.ERR_ESRECOVERABLE
                job.jobState = "failed"
            else:
                job.subStatus = 'mostly_failed' 
                job.pilotErrorDiag = "Most of events failed. (%s, other failure: %s)" % (self.getStageOutDetail(), self.__nEventsFailed - self.__nEventsFailedStagedOut)
                job.result[0] = "failed"
                job.result[2] = self.__error.ERR_ESRECOVERABLE
                job.jobState = "failed"
        else:
            job.subStatus = 'all_success'
            job.jobState = "finished"
            job.pilotErrorDiag = "AllSuccess. (%s)" % self.getStageOutDetail()

    def getESFatalCode(self):
        return self.__esFatalCode

    def getExperiment(self):
        """ Getter for __experiment """

        return self.__experiment

    def setExperiment(self, experiment):
        """ Setter for __experiment """

        self.__experiment = experiment

    def getPilotServer(self):
        """ Getter for __pilotserver """

        return self.__pilotserver

    def setPilotServer(self, pilotserver):
        """ Setter for __pilotserver """

        self.__pilotserver = pilotserver

    def getPilotPort(self):
        """ Getter for __pilotport """

        return self.__pilotport

    def setPilotPort(self, pilotport):
        """ Setter for __pilotport """

        self.__pilotport = pilotport

    def getFailureCode(self):
        """ Getter for __failureCode """

        return self.__failureCode

    def setFailureCode(self, code):
        """ Setter for __failureCode """

        self.__failureCode = code

    def getParentWorkDir(self):
        """ Getter for __pworkdir """

        return self.__pworkdir

    def setParentWorkDir(self, pworkdir):
        """ Setter for __pworkdir """

        self.__pworkdir = pworkdir
        super(RunJobEvent, self).setParentWorkDir(pworkdir)

    def getLogGUID(self):
        """ Getter for __logguid """

        return self.__logguid

    def setLogGUID(self, logguid):
        """ Setter for __logguid """

        self.__logguid = logguid

    def getPilotLogFilename(self):
        """ Getter for __pilotlogfilename """

        return self.__pilotlogfilename

    def setPilotLogFilename(self, pilotlogfilename):
        """ Setter for __pilotlogfilename """

        self.__pilotlogfilename = pilotlogfilename

    def getStageInRetry(self):
        """ Getter for __stageinretry """

        return self.__stageinretry

    def setStageInRetry(self, stageinretry):
        """ Setter for __stageinretry """

        self.__stageinretry = stageinretry
        super(RunJobEvent, self).setStageInRetry(stageinretry)

    def getStageOutRetry(self):
        """ Getter for __stageoutretry """

        return self.__stageoutretry

    def setStageOutRetry(self, stageoutretry):
        """ Setter for __stageoutretry """

        self.__stageoutretry = stageoutretry

    def getPilotInitDir(self):
        """ Getter for __pilot_initdir """

        return self.__pilot_initdir

    def setPilotInitDir(self, pilot_initdir):
        """ Setter for __pilot_initdir """

        self.__pilot_initdir = pilot_initdir
        super(RunJobEvent, self).setPilotInitDir(pilot_initdir)

    def getProxyCheckFlag(self):
        """ Getter for __proxycheckFlag """

        return self.__proxycheckFlag

    def setProxyCheckFlag(self, proxycheckFlag):
        """ Setter for __proxycheckFlag """

        self.__proxycheckFlag = proxycheckFlag

    def getGlobalPilotErrorDiag(self):
        """ Getter for __globalPilotErrorDiag """

        return self.__globalPilotErrorDiag

    def setGlobalPilotErrorDiag(self, pilotErrorDiag):
        """ Setter for __globalPilotErrorDiag """

        self.__globalPilotErrorDiag = pilotErrorDiag

    def getGlobalErrorCode(self):
        """ Getter for __globalErrorCode """

        return self.__globalErrorCode

    def setGlobalErrorCode(self, code):
        """ Setter for __globalErrorCode """

        self.__globalErrorCode = code

    def getErrorCode(self):
        """ Getter for __errorCode """

        return self.__errorCode

    def setErrorCode(self, code):
        """ Setter for __errorCode """

        self.__errorCode = code

    def getInputDir(self):
        """ Getter for __inputDir """

        return self.__inputDir

    def setInputDir(self, inputDir):
        """ Setter for __inputDir """

        self.__inputDir = inputDir
        super(RunJobEvent, self).setInputDir(inputDir)

    def getOutputDir(self):
        """ Getter for __outputDir """

        return self.__outputDir

    def setOutputDir(self, outputDir):
        """ Setter for __outputDir """

        self.__outputDir = outputDir

    def getEventLoopRunning(self):
        """ Getter for __event_loop_running """

        return self.__event_loop_running

    def setEventLoopRunning(self, event_loop_running):
        """ Setter for __event_loop_running """

        self.__event_loop_running = event_loop_running

    def getOutputFiles(self):
        """ Getter for __output_files """

        return self.__output_files

    def setOutputFiles(self, output_files):
        """ Setter for __output_files """

        self.__output_files = output_files

    def getGUIDList(self):
        """ Getter for __guid_list """

        return self.__guid_list

    def setGUIDList(self, guid_list):
        """ Setter for __guid_list """

        self.__guid_list = guid_list

    def getLFNList(self):
        """ Getter for __lfn_list """

        return self.__lfn_list

    def setLFNList(self, lfn_list):
        """ Setter for __lfn_list """

        self.__lfn_list = lfn_list

    def getUpdatedLFN(self):
        """ Getter for __updated_lfn """

        return self.__updated_lfn

    def setUpdatedLFN(self, updated_lfn):
        """ Setter for __updated_lfn """

        self.__updated_lfn = updated_lfn

    def getEventRangeDictionary(self):
        """ Getter for __eventRange_dictionary """

        return self.__eventRange_dictionary

    def setEventRangeDictionary(self, eventRange_dictionary):
        """ Setter for __eventRange_dictionary """

        self.__eventRange_dictionary = eventRange_dictionary

    def getEventRangeIDDictionary(self):
        """ Getter for __eventRangeID_dictionary """

        return self.__eventRangeID_dictionary

    def setEventRangeIDDictionary(self, eventRangeID_dictionary):
        """ Setter for __eventRangeID_dictionary """

        self.__eventRangeID_dictionary = eventRangeID_dictionary

    def getStageOutQueue(self):
        """ Getter for __stageout_queue """

        return self.__stageout_queue

    def setStageOutQueue(self, stageout_queue):
        """ Setter for __stageout_queue """

        self.__stageout_queue = stageout_queue

    def getPoolFileCatalogPath(self):
        """ Getter for __pfc_path """

        return self.__pfc_path

    def setPoolFileCatalogPath(self, pfc_path):
        """ Setter for __pfc_path """

        self.__pfc_path = pfc_path

    def getMessageServerPayload(self):
        """ Getter for __message_server_payload """

        return self.__message_server_payload

    def setMessageServerPayload(self, message_server):
        """ Setter for __message_server_payload """

        self.__message_server_payload = message_server

    def getMessageServerPrefetcher(self):
        """ Getter for __message_server_prefetcher """

        return self.__message_server_prefetcher

    def setMessageServerPrefetcher(self, message_server):
        """ Setter for __message_server_prefetcher """

        self.__message_server_prefetcher = message_server

    def getMessageThreadPayload(self):
        """ Getter for __message_thread_payload """

        return self.__message_thread_payload

    def setMessageThreadPayload(self, message_thread_payload):
        """ Setter for __message_thread_payload """

        self.__message_thread_payload = message_thread_payload

    def getMessageThreadPrefetcher(self):
        """ Getter for __message_thread_prefetcher """

        return self.__message_thread_prefetcher

    def setMessageThreadPrefetcher(self, message_thread_prefetcher):
        """ Setter for __message_thread_prefetcher """

        self.__message_thread_prefetcher = message_thread_prefetcher 

    def isAthenaMPReady(self):
        """ Getter for __athenamp_is_ready """

        return self.__athenamp_is_ready

    def setAthenaMPIsReady(self, athenamp_is_ready):
        """ Setter for __athenamp_is_ready """

        self.__athenamp_is_ready = athenamp_is_ready

    def isPrefetcherReady(self):
        """ Getter for __prefetcher_is_ready """

        return self.__prefetcher_is_ready

    def setPrefetcherIsReady(self, prefetcher_is_ready):
        """ Setter for __prefetcher_is_ready """

        self.__prefetcher_is_ready = prefetcher_is_ready

    def prefetcherHasFinished(self):
        """ Getter for __prefetcher_has_finished """

        return self.__prefetcher_has_finished

    def setPrefetcherHasFinished(self, prefetcher_has_finished):
        """ Setter for __prefetcher_has_finished """

        self.__prefetcher_has_finished = prefetcher_has_finished 

    def getAsyncOutputStagerThread(self):
        """ Getter for __asyncOutputStager_thread """

        return self.__asyncOutputStager_thread

    def setAsyncOutputStagerThread(self, asyncOutputStager_thread):
        """ Setter for __asyncOutputStager_thread """

        self.__asyncOutputStager_thread = asyncOutputStager_thread

    def getAnalysisJob(self):
        """ Getter for __analysisJob """

        return self.__analysisJob

    def setAnalysisJob(self, analysisJob):
        """ Setter for __analysisJob """

        self.__analysisJob = analysisJob

    def getCache(self):
        """ Getter for __cache """

        return self.__cache

    def setCache(self, cache):
        """ Setter for __cache """

        self.__cache = cache

    def getMetadataFilename(self):
        """ Getter for __cache """

        return self.__metadata_filename

    def setMetadataFilename(self, event_range_id):
        """ Setter for __metadata_filename """

        self.__metadata_filename = os.path.join(self.__job.workdir, "metadata-%s.xml" % (event_range_id))

    def getJobSite(self):
        """ Getter for __jobSite """

        return self.__jobSite

    def setJobSite(self, jobSite):
        """ Setter for __jobSite """

        self.__jobSite = jobSite

    def setJobNode(self, node):
        self.__node = node

    def getYamplChannelNamePayload(self):
        """ Getter for __yamplChannelNamePayload """

        return self.__yamplChannelNamePayload

    def setYamplChannelNamePayload(self, yamplChannelNamePayload):
        """ Setter for __yamplChannelNamePayload """

        self.__yamplChannelNamePayload = yamplChannelNamePayload

    def getYamplChannelNamePrefetcher(self):
        """ Getter for __yamplChannelNamePrefetcher """

        return self.__yamplChannelNamePrefetcher

    def setYamplChannelNamePrefetcher(self, yamplChannelNamePrefetcher):
        """ Setter for __yamplChannelNamePrefetcher """

        self.__yamplChannelNamePrefetcher = yamplChannelNamePrefetcher

    def getStatus(self):
        """ Getter for __status """

        return self.__status

    def setStatus(self, status):
        """ Setter for __status """

        self.__status = status

    def isSendingEventRange(self):
        """ Getter for __sending_event_range """

        return self.__sending_event_range

    def setSendingEventRange(self, sending_event_range):
        """ Setter for __sending_event_range """

        self.__sending_event_range = sending_event_range

    def getCurrentEventRange(self):
        """ Getter for __current_event_range """

        return self.__current_event_range

    def setCurrentEventRange(self, current_event_range):
        """ Setter for __current_event_range """

        self.__current_event_range = current_event_range

    def getMaxWaitOneEvent(self):
        """ Getter for __max_wait_for_one_event """

        return self.__max_wait_for_one_event

    def getMinEvents(self):
        """ Getter for __min_events """
        return self.__min_events

    def shouldBeAborted(self):
        """ Should the job be aborted? """

        if os.path.exists(os.path.join(self.__job.workdir, "ABORT")):
            return True
        else:
            return False

    def setAbort(self):
        """ Create the ABORT lock file """

        createLockFile(False, self.__job.workdir, lockfile="ABORT")

    def shouldBeKilled(self):
        """ Does the TOBEKILLED lock file exist? """

        path = os.path.join(self.__job.workdir, "TOBEKILLED")
        if os.path.exists(path):
            tolog("path exists: %s" % (path))
            return True
        else:
            tolog("path does not exist: %s" % (path))
            return False

    def setToBeKilled(self):
        """ Create the TOBEKILLED lock file"""

        createLockFile(False, self.__job.workdir, lockfile="TOBEKILLED")

    # Get/setters for the job object

    def getJob(self):
        """ Getter for __job """

        return self.__job

    def setJob(self, job):
        """ Setter for __job """

        self.__job = job

        # Reset the outFilesGuids list since guids will be generated by this module
        self.__job.outFilesGuids = []

    def getJobWorkDir(self):
        """ Getter for workdir """

        return self.__job.workdir

    def setJobWorkDir(self, workdir):
        """ Setter for workdir """

        self.__job.workdir = workdir

    def getJobID(self):
        """ Getter for jobId """

        return self.__job.jobId

    def setJobID(self, jobId):
        """ Setter for jobId """

        self.__job.jobId = jobId

    def getJobDataDir(self):
        """ Getter for datadir """

        return self.__job.datadir

    def setJobDataDir(self, datadir):
        """ Setter for datadir """

        self.__job.datadir = datadir

    def getJobTrf(self):
        """ Getter for trf """

        return self.__job.trf

    def setJobTrf(self, trf):
        """ Setter for trf """

        self.__job.trf = trf

    def getJobResult(self):
        """ Getter for result """

        return self.__job.result

    def setJobResult(self, result, pilot_failed=False):
        """ Setter for result """

        self.__job.result = result
        if pilot_failed:
            self.setFinalESStatus(self.__job)

    def getJobState(self):
        """ Getter for jobState """

        return self.__job.jobState

    def setJobState(self, jobState):
        """ Setter for jobState """

        self.__job.jobState = jobState

    def getJobStates(self):
        """ Getter for job states """

        return self.__job.result

    def setJobStates(self, states):
        """ Setter for job states """

        self.__job.result = states
        self.__job.currentState = states[0]

    def getTaskID(self):
        """ Getter for TaskID """

        return self.__taskID

    def setTaskID(self, taskID):
        """ Setter for taskID """

        self.__taskID = taskID

    def getJobOutFiles(self):
        """ Getter for outFiles """

        return self.__job.outFiles

    def setJobOutFiles(self, outFiles):
        """ Setter for outFiles """

        self.__job.outFiles = outFiles

    def getTokenExtractorInputListFilename(self):
        """ Getter for __tokenextractor_input_list_filenane """

        return self.__tokenextractor_input_list_filenane

    def setTokenExtractorInputListFilename(self, tokenextractor_input_list_filenane):
        """ Setter for __tokenextractor_input_list_filenane """

        self.__tokenextractor_input_list_filenane = tokenextractor_input_list_filenane

    def useEventIndex(self):
        """ Should the Event Index be used? """

        return self.__useEventIndex

    def setUseEventIndex(self, jobPars):
        """ Set the __useEventIndex variable to a boolean value """

        if "--createTAGFileForES" in jobPars:
            value = False
        else:
            value = True
        self.__useEventIndex = value

    def useTokenExtractor(self):
        """ Should the Token Extractor be used? """

        return self.__useTokenExtractor

    def setUseTokenExtractor(self, setup):
        """ Set the __useTokenExtractor variable to a boolean value """
        # Decision is based on info in the setup string

        self.__useTokenExtractor = 'TokenScatterer' in setup or 'UseTokenExtractor=True' in setup.replace("  ","").replace(" ","")

        if self.__useTokenExtractor:
            tolog("Token Extractor is needed")
        else:
            tolog("Token Extractor is not needed")

    def usePrefetcher(self):
        """ Should the Prefetcher be used? """

        return self.__usePrefetcher

    def setUsePrefetcher(self, usePrefetcher):
        """ Set the __usePrefetcher variable to a boolean value """

        self.__usePrefetcher = usePrefetcher

    def getInFilePosEvtNum(self):
        """ Should the event range numbers relative to in-file position be used? """

        return self.__inFilePosEvtNum

    def setInFilePosEvtNum(self, inFilePosEvtNum):
        """ Set the __inFilePosEvtNum variable to a boolean value """

        self.__inFilePosEvtNum = inFilePosEvtNum

    def getPanDAServer(self):
        """ Getter for __pandaserver """

        return self.__pandaserver

    def setPanDAServer(self, pandaserver):
        """ Setter for __pandaserver """

        self.__pandaserver = pandaserver

    def getAllowPrefetchEvents(self):
        return self.__allowPrefetchEvents

    def setAllowPrefetchEvents(self, allowPrefetchEvents):
        self.__allowPrefetchEvents = allowPrefetchEvents

    def init_guid_list(self):
        """ Init guid and lfn list for staged in files"""

        for guid in self.__job.inFilesGuids:
            self.__guid_list.append(guid)
        for lfn in self.__job.inFiles:
            self.__lfn_list.append(lfn)

    def init_input_files(self, job):
        """ Init input files list"""
        self.__input_files = job.get_stagedIn_files()

    def add_input_file(self, scope, name, pfn):
        """ Add a file to input files """
        self.__input_files['%s:%s' % (scope, name)] = pfn

    def get_input_files(self):
        return self.__input_files

    def addPFNsToEventRanges(self, eventRanges):
        """ Add the pfn's to the event ranges """
        # If an event range is file related, we need to add the pfn to the event range

        if self.getInFilePosEvtNum():
            for eventRange in eventRanges:
                key = '%s:%s' % (eventRange['scope'], eventRange['LFN'])
                if key in self.__input_files:
                    eventRange['PFN'] = self.__input_files[key]
                else:
                    eventRange['PFN'] = eventRange['LFN']
       	return eventRanges

    def addPFNToEventRange(self, eventRange):
        """ Add the pfn to an event range """
        # If an event range is file related, we need to add the pfn to the event range

        key = '%s:%s' % (eventRange['scope'], eventRange['LFN'])
        if key in self.__input_files:
            eventRange['PFN'] = self.__input_files[key]
        else:
            eventRange['PFN'] = eventRange['LFN']
        return eventRange

    def setAsyncOutputStagerSleepTime(self, sleep_time=600):
        self.__asyncOutputStager_thread_sleep_time = sleep_time

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        uuidgen = commands.getoutput('uuidgen')
        self.__yamplChannelNamePayload = "EventService_EventRanges-%s" % (uuidgen)
        self.__yamplChannelNamePrefetcher = "EventService_Prefetcher-%s" % (uuidgen)

    # is this necessary? doesn't exist in RunJob
    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJobEvent, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getRunJob(self):
        """ Return a string with the experiment name """

        return self.__runjob

    def getRunJobFileName(self):
        """ Return the filename of the module """

        return super(RunJobEvent, self).getRunJobFileName()

    def allowLoopingJobKiller(self):
        """ Should the pilot search for looping jobs? """

        # The pilot has the ability to monitor the payload work directory. If there are no updated files within a certain
        # time limit, the pilot will consider the as stuck (looping) and will kill it. The looping time limits are set
        # in environment.py (see e.g. loopingLimitDefaultProd)

        return True

    def startMessageThreadPrefetcher(self):
        """ Start the message thread for the prefetcher """

        self.__message_thread_prefetcher.start()

    def stopMessageThreadPrefetcher(self):
        """ Stop the message thread for the prefetcher """

        self.__message_thread_prefetcher.stop()

    def joinMessageThreadPrefetcher(self):
        """ Join the message thread for the prefetcher """

        self.__message_thread_prefetcher.join()

    def payloadListener(self):
        """ Listen for messages from the payload """

        # Note: this is run as a thread

        # Listen for messages as long as the thread is not stopped
        while not self.__message_thread_payload.stopped():

            try:
                # Receive a message
                tolog("Waiting for a new message")
                size, buf = self.__message_server_payload.receive()
                while size == -1 and not self.__message_thread_payload.stopped():
                    time.sleep(1)
                    size, buf = self.__message_server_payload.receive()
                tolog("Received new message from Payload: %s" % (buf))

                max_wait = 600
                i = 0
                if self.__sending_event_range:
                    tolog("Will wait for current event range to finish being sent (pilot not yet ready to process new request)")
                while self.__sending_event_range:
                    # Wait until previous send event range has completed (to avoid racing condition), but wait maximum 60 seconds then fail job
                    time.sleep(0.1)
                    if i > max_wait:
                        # Abort with error
                        buf = "ERR_FATAL_STUCK_SENDING %s: Stuck sending event range to payload; new message: %s" % (self.__current_event_range, buf)
                        break
                    i += 1
                if i > 0:
                    tolog("Delayed %d s for send message to complete" % (i*10))

                if not "Ready for" in buf:
                    if self.__eventRangeID_dictionary.keys():
                        try:
                            keys = self.__eventRangeID_dictionary.keys()
                            key = keys[0]
                            tolog("Faking error for range = %s" % (key))
                            buf = "ERR_TE_RANGE %s: Range contains wrong positional number 5001" % (key)
                        except Exception,e:
                            tolog("No event ranges yet:%s" % (e))
                    #buf = "ERR_TE_FATAL Range-2: CURL curl_easy_perform() failed! Couldn't resolve host name"
                    #buf = "ERR_TE_FATAL 5211313-2452346274-2058479689-3-8: URL No tokens for GUID 00224B03-8005-E849-BCD5-D8F8F764B630"

                # Interpret the message and take the appropriate action
                if "Ready for events" in buf:
                    buf = ""
                    #tolog("AthenaMP is ready for events")
                    self.__athenamp_is_ready = True

                elif buf.startswith('/'):
                    # tolog("Received file and process info from client: %s" % (buf))

                    self.__nEvents += 1

                    # Extract the information from the message
                    paths, event_range_id, cpu, wall = self.interpretMessage(buf)
                    if paths and paths not in self.__stageout_queue:
                        # Add the extracted info to the event range dictionary
                        self.__eventRange_dictionary[event_range_id] = [paths, cpu, wall]

                        # Add the file to the stage-out queue
                        self.__stageout_queue.append(paths)
                        # tolog("File %s has been added to the stage-out queue (length = %d)" % (paths, len(self.__stageout_queue)))

                elif buf.startswith('['):
                    tolog("Received an updated event range message from Prefetcher: %s" % (buf))
                    self.__current_event_range = buf

                    # Set the boolean to True since Prefetcher is now ready (finished with the current event range)
                    runJob.setPrefetcherIsReady(True)

                elif buf.startswith('ERR'):
                    tolog("Received an error message: %s" % (buf))

                    # Extract the error acronym and the error diagnostics
                    error_acronym, event_range_id, error_diagnostics = self.extractErrorMessage(buf)
                    if event_range_id != "":
                        tolog("!!WARNING!!2144!! Extracted error acronym %s and error diagnostics \'%s\' for event range %s" % (error_acronym, error_diagnostics, event_range_id))

                        error_code = None
                        event_status = 'fatal'
                        # Was the error fatal? If so, the pilot should abort
                        if "FATAL" in error_acronym:
                            tolog("!!WARNING!!2146!! A FATAL error was encountered, prepare to finish")

                            # Fail the job
                            if error_acronym == "ERR_TE_FATAL" and "URL Error" in error_diagnostics:
                                error_code = self.__error.ERR_TEBADURL
                            elif error_acronym == "ERR_TE_FATAL" and "resolve host name" in error_diagnostics:
                                error_code = self.__error.ERR_TEHOSTNAME
                            elif error_acronym == "ERR_TE_FATAL" and "Invalid GUID length" in error_diagnostics:
                                error_code = self.__error.ERR_TEINVALIDGUID
                            elif error_acronym == "ERR_TE_FATAL" and "No tokens for GUID" in error_diagnostics:
                                error_code = self.__error.ERR_TEWRONGGUID
                            elif error_acronym == "ERR_TE_FATAL":
                                error_code = self.__error.ERR_TEFATAL
                            else:
                                error_code = self.__error.ERR_ESFATAL
                            self.__esFatalCode = error_code
                        else:
                            error_code = self.__error.ERR_UNKNOWN

                        # Time to update the server
                        self.__nEventsFailed += 1
                        msg = updateEventRange(event_range_id, [], self.__job.jobId, status=event_status, errorCode=error_code, pandaProxySecretKey=self.__job.pandaProxySecretKey)
                        if msg != "":
                            tolog("!!WARNING!!2145!! Problem with updating event range: %s" % (msg))
                        else:
                            tolog("Updated server for failed event range")

                        if error_code:
                            result = ["failed", 0, error_code]
                            tolog("Setting error code: %d" % (error_code))
                            self.setJobResult(result, pilot_failed=True)

                            # ..
                    else:
                        tolog("!!WARNING!!2245!! Extracted error acronym %s and error diagnostics \'%s\' (event range could not be extracted - cannot update server)" % (error_acronym, error_diagnostics))

                else:
                    tolog("Pilot received message:%s" % buf)
            except Exception, e:
                tolog("Caught exception:%s" % e)
            time.sleep(1)

        tolog("Payload listener has finished")

    def getPrefetcherProcess(self, thisExperiment, setup, input_file, stdout=None, stderr=None):
        """ Execute the Prefetcher """
        # The input file corresponds to a remote input file (full path)

        # Prefix of the local file names
        prefix = "localRange.pool.root"
        options = "'--inputEVNTFile' %s '--outputEVNT_MRGFile' %s '--eventService=True' '--preExec' 'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"'" % (input_file, prefix, self.__yamplChannelNamePrefetcher)

        # Define the command
        cmd = "%s export ATHENA_PROC_NUMBER=1; EVNTMerge_tf.py %s" % (setup, options)

        # Execute and return the Prefetcher subprocess object
        return self.getSubprocess(thisExperiment, cmd, stdout=stdout, stderr=stderr)
    
    def createMessageServer(self, prefetcher=False):
        """ Create the message server socket object """
        # Create a message server for the payload by default, otherwise for Prefetcher if prefetcher is set

        status = False

        # Create the server socket
        if MessageServer:
            if prefetcher:
                self.__message_server_prefetcher = MessageServer(socketname=self.__yamplChannelNamePrefetcher, context='local')

                # is the server alive?
                if not self.__message_server_prefetcher.alive():
                    # destroy the object
                    tolog("!!WARNING!!3333!! Message server for Prefetcher is not alive")
                    self.__message_server_prefetcher = None
                else:
                    status = True
            else:
                self.__message_server_payload = MessageServer(socketname=self.__yamplChannelNamePayload, context='local')

                # is the server alive?
                if not self.__message_server_payload.alive():
                    # destroy the object
                    tolog("!!WARNING!!3333!! Message server for the payload is not alive")
                    self.__message_server_payload = None
                else:
                    status = True
        else:
            tolog("!!WARNING!!3333!! MessageServer object is not available")

        return status

    def sendMessage(self, message, prefetcher=False):
        """ Send a message """
        # Message will be sent to the payload by default, or to the Prefetcher in case prefetcher is set

        # Filter away unwanted fields
        if "scope" in message:
            # First replace an ' with " since loads() cannot handle ' signs properly
            # Then convert to a list and get the 0th element (there should be only one)
            try:
                #_msg = loads(message.replace("'",'"'))[0]
                _msg = loads(message.replace("'",'"').replace('u"','"'))[0]
            except Exception, e:
                tolog("!!WARNING!!2233!! Caught exception: %s" % (e))
            else:
                # _msg = {u'eventRangeID': u'79-2161071668-11456-1011-1', u'LFN': u'EVNT.01461041._000001.pool.root.1', u'lastEvent': 1020, u'startEvent': 1011, u'scope': u'mc12_8TeV', u'GUID': u'BABC9918-743B-C742-9049-FC3DCC8DD774'}
                # Now remove the "scope" key/value
                scope = _msg.pop("scope")
                # Convert back to a string
                message = str([_msg])

        if prefetcher:
            self.__message_server_prefetcher.send(message)
            label = "Prefetcher"
        else:
            self.__message_server_payload.send(message)
            label = "the payload"
        tolog("Sent %s to %s" % (message, label))

    def stopPrefetcher(self, prefetcherProcess, prefetcher_stdout, prefetcher_stderr):
        """ Stop Prefetcher thread and close output steams """

        if prefetcherProcess:
            os.killpg(os.getpgid(prefetcherProcess.pid), signal.SIGTERM)
            # prefetcherProcess.kill()

        # Close stdout/err streams
        if prefetcher_stdout:
            prefetcher_stderr.close()
        if prefetcher_stdout:
            prefetcher_stderr.close()

# main process starts here
if __name__ == "__main__":        
        
        tolog("Starting RunJobPrefetcher")
        
        
        
        # Create and start the Prefetcher

        # Extract the proper setup string from the run command in case the Prefetcher should be used
        if runJob.usePrefetcher():
            setupString = thisEventService.extractSetup(runCommandList[0], job.trf)
            if "export ATLAS_LOCAL_ROOT_BASE" not in setupString:
                setupString = "export ATLAS_LOCAL_ROOT_BASE=%s/atlas.cern.ch/repo/ATLASLocalRootBase;" % thisExperiment.getCVMFSPath() + setupString
            tolog("The Prefetcher will be setup using: %s" % (setupString))

            #job.transferType = 'direct'
            #RunJobUtilities.updateCopysetups('', transferType=job.transferType)
            #si = getSiteInformation(runJob.getExperiment())
            #si.updateDirectAccess(job.transferType)

            # Create the file objects
            prefetcher_stdout, prefetcher_stderr = runJob.getStdoutStderrFileObjects(stdoutName="prefetcher_stdout.txt", stderrName="prefetcher_stderr.txt")

            # Get the full path to the input file from the fileState file
            if job.transferType == "direct":
                state = "direct_access"
            else:
                state = "prefetch"
            input_files = getFilesOfState(runJob.getParentWorkDir(), job.jobId, ftype="input", state=state)
            if input_files == "":
                pilotErrorDiag = "Did not find any turls in fileState file"
                tolog("!!WARNING!!4545!! %s" % (pilotErrorDiag))

                # Set error code
                job.result[0] = "failed"
                job.result[2] = error.ERR_ESRECOVERABLE
                runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

            input_file = ""
            infiles = input_files.split(",")
            for infile in infiles:
                for inFileLfn in job.inFiles:
                    if inFileLfn in infile:
                        input_file += infile + " "
                        break
                else:
                    pilotErrorDiag = "Did not find turl for lfn=%s in fileState file" % (inFileLfn)
                    tolog("!!WARNING!!4545!! %s" % (pilotErrorDiag))

                    # Set error code
                    job.result[0] = "failed"
                    job.result[2] = error.ERR_ESRECOVERABLE
                    runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

            if input_file.endswith(" "):
                input_file = input_file[:-1]

            if input_file == "":
                pilotErrorDiag = "Did not find turl for any lfn in fileState file"
                tolog("!!WARNING!!4545!! %s" % (pilotErrorDiag))

                # Set error code
                job.result[0] = "failed"
                job.result[2] = error.ERR_ESRECOVERABLE
                runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

            # Get the Prefetcher process
            prefetcherProcess = runJob.getPrefetcherProcess(thisExperiment, setupString, input_file=input_file, \
                                                                    stdout=prefetcher_stdout, stderr=prefetcher_stderr)
            if not prefetcherProcess:
                tolog("!!WARNING!!1234!! Prefetcher could not be started - will run without it")
                runJob.setUsePrefetcher(False)
            else:
                tolog("Prefetcher is running")
        else:
            prefetcher_stdout = None
            prefetcher_stderr = None
            prefetcherProcess = None

        # service tools ends here ..........................................................................
        # Create the file objects
        athenamp_stdout, athenamp_stderr = runJob.getStdoutStderrFileObjects(stdoutName="athena_stdout.txt", stderrName="athena_stderr.txt")

        # download event ranges before athenaMP
        # Pilot will download some event ranges from the Event Server
        catchalls = runJob.resolveConfigItem('catchall')
        first_event_ranges = None
        try:
            job.coreCount = int(job.coreCount)
        except:
            pass
        if runJob.getAllowPrefetchEvents():
            numRanges = max(job.coreCount, runJob.getMinEvents())
            message = downloadEventRanges(job.jobId, job.jobsetID, job.taskID, job.pandaProxySecretKey, numRanges=numRanges, url=runJob.getPanDAServer())
            # Create a list of event ranges from the downloaded message
            first_event_ranges = runJob.extractEventRanges(message)
            if first_event_ranges is None or first_event_ranges == []:
                tolog("No more events. will finish this job directly")
                runJob.failJob(0, error.ERR_NOEVENTS, job, pilotErrorDiag="No events before start AthenaMP")
            if len(first_event_ranges) < runJob.getMinEvents():
                tolog("Got less events(%s events) than minimal requirement(%s events). will finish this job directly" % (len(first_event_ranges), runJob.getMinEvents()))
                runJob.failJob(0, error.ERR_TOOFEWEVENTS, job, pilotErrorDiag="Got less events(%s events) than minimal requirement(%s events)" % (len(first_event_ranges), runJob.getMinEvents()))
           
            # Get the current list of eventRangeIDs
            currentEventRangeIDs = runJob.extractEventRangeIDs(first_event_ranges)

            # Store the current event range id's in the total event range id dictionary
            runJob.addEventRangeIDsToDictionary(currentEventRangeIDs)
            
        # Create and start the AthenaMP process
        t0 = os.times()
        tolog("t0 = %s" % str(t0))
        path = os.path.join(job.workdir, 't0_times.txt')
        if writeFile(path, str(t0)):
            tolog("Wrote %s to file %s" % (str(t0), path))
        else:
            tolog("!!WARNING!!3344!! Failed to write t0 to file, will not be able to calculate CPU consumption time on the fly")

        athenaMPProcess = runJob.getSubprocess(thisExperiment, runCommandList[0], stdout=athenamp_stdout, stderr=athenamp_stderr)

        if athenaMPProcess:
             path = os.path.join(job.workdir, 'cpid.txt')
             if writeFile(path, str(athenaMPProcess.pid)):
                 tolog("Wrote cpid=%s to file %s" % (athenaMPProcess.pid, path))
