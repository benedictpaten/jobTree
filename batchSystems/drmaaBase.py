#!/usr/bin/env python

#Copyright (C) 2013 by Glenn Hickey and Benedict Paten (benedictpaten@gmail.com)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.

import os 
import re
import subprocess
import time
import datetime
import sys
from Queue import Queue, Empty
from threading import Thread

# (torque) http://sourceforge.net/projects/pbspro-drmaa/
# Python Wrapper http://code.google.com/p/drmaa-python/
import drmaa

from sonLib.bioio import logger
from sonLib.bioio import system
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.src.master import getParasolResultsFileName

# iterate over the issued jobs in our set continuously.
# if a finished job is found, but its id and exit status
# in the finished jobs queue (and remove it from the
# issued jobs set)
class DrmaaJobPoller(Thread):
    def __init__(self, bs):
        Thread.__init__(self)
        self.bs = bs

    def run(self):
        while True:
            for jobName in list(self.bs.issuedJobNames):
                jStatus = self.bs.session.jobStatus(jobName)
                if (jStatus == drmaa.JobState.DONE or
                    jStatus == drmaa.JobState.FAILED):
                    retval = self.bs.session.wait(jobName,
                                                  drmaa.Session.TIMEOUT_NO_WAIT)
                    self.bs.finishedQueue.put((jobName, retval.exitStatus))
                    self.bs.issuedJobNames.remove(jobName)

            time.sleep(10)

class DrmaaBatchSystem(AbstractBatchSystem):
    """The interface for DRMAA.  This is a general API that should work
    on a variety of batch systems including Grid Engine and Torque. It
    only does very basic functions like submitting and polling.
    Resource allocation other than time, ie mem and cpu, are not handled.
    We should therefore derive classes for particular systems
    (ex: DrmaaTorque).  

    Issued jobs are kept in a set in memory. This set is used for most
    functions since the the DRMAA API doesn't allow to iterate over jobs.
    This means that if the batch system crashes, issued job names are
    lost-- so we cannot kill them or poll them.
    """
    
    def __init__(self, config):
        AbstractBatchSystem.__init__(self, config) #Call the parent constructor

        # create and initialize a new DRMAA Session object
        self.session = drmaa.Session()
        self.session.initialize()

        # construct single job template for efficiency
        self.job = self.session.createJobTemplate()

        # don't do anything with error or output for now
        #self.job.outputPath = '/dev/null'
        #self.job.errorPath = '/dev/null'
        self.job.blockEmail = True
        self.job.email = '/dev/null'
        self.job.joinFiles = True
        
        # todo : can we restore an old session here?  may have to update some
        # kind of file as a hack.  will avoid thinking about this while
        # implementing first prototype.

        # keep track of all jobs
        self.issuedJobNames = set()

        # map back and forth between drmaa names and jobtree ids
        # (todo fold one of these into issuedJobNames to save space)
        self.nameToID = dict()
        self.IDToName = dict()
        self.nextID = 0

        # queue of jobs that have exited
        self.finishedQueue = Queue()

        # constantly poll queues in a separate process: popping off
        # submitted and moving to finished
        self.worker = DrmaaJobPoller(self)
        self.worker.daemon = True
        self.worker.start()

        # not sure if need anymore
        self.drmaaResultsFile = getParasolResultsFileName(config.attrib["job_tree"])        
        #Reset the job queue and results (initially, we do this again once we've killed the jobs)
        self.drmaaResultsFileHandle = open(self.drmaaResultsFile, 'w')
        #We lose any previous state in this file, and ensure the files existence
        self.drmaaResultsFileHandle.close() 
    
    def __del__(self):
        self.session.deleteJobTemplate(self.job)
        self.session.exit()  

    # issue the job to the batch systme using drmaa
    # THIS IS THE FUNCTION TO REIMPLEMENT IN DERIVED CLASSES
    def submitJob(self, command, memory, cpu):
        self.job.remoteCommand = command
        self.job.jobName = command
        jobName = self.session.runJob(self.job)
        logger.debug("DRMAA Issued the job command: %s with job id: %s " % (command, str(jobName)))
        return jobName
                
    # submit a job to the batch system 
    def issueJob(self, command, memory, cpu):
        jobName = self.submitJob(command, memory, cpu)
        self.issuedJobNames.add(jobName)
        jobID = self.nextID
        self.nameToID[jobName] = jobID
        self.IDToName[self.nextID] = jobName
        self.nextID += 1
        return jobID

    # jobs are killed only if they are in the issuedJobs set
    # and are detected as running on the batch system
    def killJobs(self, jobIDs):
        killedIDs = []
        # kill jobs in input
        for jobID in jobIDs:
            if jobID in self.IDToName:
                jobName = self.IDToName[jobID]
                # todo fold into one check
                if jobName in self.issuedJobNames:
                    jStatus = self.session.jobStatus(jobName)
                    if jStatus == drmaa.JobState.RUNNING:
                        self.session.control(jobName, drmaa.JobControlAction.TERMINATE)
                        killedIDs += jobName
                    self.issuedJobNames.remove(jobName)
                    del self.IDToName[jobID]
                    del self.nameToID[jobName]
                else:
                    # this should probably be a fatal error
                    logger.critical("DRMMA couldnt has no record of job id %s so cant kill" % jobName)

        # wait on killed jobs to be done
        self.session.synchronize(killedIDs, DRMAA_TIMEOUT_NO_WAIT)
    
    def getIssuedJobIDs(self):
        jobIDs = []
        for jobName in self.issuedJobNames:
            jobID = self.nameToID[jobName]
            jobIDs.append(jobID)
        return jobIDs
    
    def getRunningJobIDs(self):
        jobTimes = dict()
        
        for jobName in self.issuedJobNames:
            jStatus = self.session.jobStatus(jobName)
            if jStatus == drmaa.JobState.RUNNING:
                jobID = self.nameToID[jobName]
                # DRMAA is unable to query running time of a job
                # we set to 1 for now.  Is this critical?
                jobTimes[jobID] = 1

        return jobTimes

    # pull job from finishedQueue if present
    def getUpdatedJob(self, maxWait):
        try:
            jobName, exitCode = self.finishedQueue.get(timeout=maxWait)
            self.finishedQueue.task_done()
            jobID = self.nameToID[jobName]
            # note that it was removed from self.issuedJobNames when
            # it was added to the finished queue
            del self.IDToName[jobID]
            del self.nameToID[jobName]
            return (jobID, exitCode)
        except Empty:
            pass
        return None
    
    def getWaitDuration(self):
        """We give parasol a second to catch its breath (in seconds)
        """
        return 0.0
    
    def getRescueJobFrequency(self):
        """Parasol leaks jobs, but rescuing jobs involves calls to parasol list jobs and pstat2,
        making it expensive. We allow this every 10 minutes..
        """
        return 1800 #Half an hour

    def obtainSystemConstants(self):
        pass
                
        
def main():
    pass

def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
