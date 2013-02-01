#!/usr/bin/env python

#Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
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
import sys
from Queue import Queue, Empty
from threading import Thread

# (torque) http://sourceforge.net/projects/pbspro-drmaa/
# Python Wrapper http://code.google.com/p/drmaa-python/
import drmaa

from sonLib.bioio import logger
from sonLib.bioio import system
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem

# iterate over the issued jobs in our set continuously.
# if a finished job is found, but its id and exit status
# in the finished jobs queue (and remove it from the
# issued jobs set)
class DrmaaJobPoller(Thread):
    def __init__(self, bs):
        Thread.__init__(self)
        self.bs = bs

    def run(self):
        doneJobs = set()
        while True:
            for jobName in self.bs.issuedJobNames - doneJobs:
                jStatus = self.bs.session.jobStatus(jobName)
                print "polling %s return status %s" % (jobName, str(jStatus))
                if (jStatus == drmaa.JobState.DONE or
                    jStatus == drmaa.JobState.FAILED):
                    retval = self.bs.session.wait(jobName,
                                                  drmaa.Session.TIMEOUT_NO_WAIT)
                    self.bs.finishedQueue.put((jobName, retval.exitStatus))
                    doneJobs.add(jobName)
                    print "JOB COMPLETED WITH NAME %s" % jobName
                    break
            # we keep this local structure to avoid modifying issuedJobNames
            # beacause that seems less thread safe
            doneJobs = doneJobs.intersection(self.bs.issuedJobNames)
            time.sleep(.5)

class DrmaApiBatchSystem(AbstractBatchSystem):
    """The interface for drmaa
    Issued jobs are kept in a set in memory. This set is used for most
    functions since the the DRMAA API doesn't allow to iterate over jobs.
    This means that any type of recovery from a crash is presently
    impossible.  Only way around this seems to be serializing to file
    (TODO)
    """
    
    def __init__(self, config):
        AbstractBatchSystem.__init__(self, config) #Call the parent constructor

        # create and initialize a new DRMAA Session object
        self.session = drmaa.Session()
        self.session.initialize()

        # construct single job template for efficiency
        self.job = self.session.createJobTemplate()
        
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

        #worker = Process(target=self.__pollFinishedJobs, args=[])
        #worker.daemon = True
        #worker.start()
        #assert worker.is_alive()
        
        self.usedCpus = 0
        self.maxCpus = int(config.attrib["max_jobs"])
        self.jobNamesToCpu = {}

    def __pollFinishedJobs(self):
        while True:
            print self.issuedJobNames
            for jobName in self.issuedJobNames:
                jStatus = self.session.jobStatus(jobName)
                print "polling %s return status %s" % (jobName, str(jStatus))
                if (jStatus == drmaa.JobState.DONE or
                    jStatus == drmaa.JobState.FAILED):
                    retval = self.session.wait(jobName,
                                               drmaa.Session.TIMEOUT_NO_WAIT)
                    self.finishedQueue.put((jobName, retval.exitStatus))
                    print "JOB COMPLETED WITH NAME %s" % jobName
            sleep(1)
    
    def __des__(self):
        self.session.deleteJobTemplate(self.job)
        self.session.exit()  

    # submit a job to the batch system 
    def issueJob(self, command, memory, cpu):
        self.job.remoteCommand = command
        jobName = self.session.runJob(self.job)
        self.issuedJobNames.add(jobName)
        jobID = self.nextID
        self.nameToID[jobName] = jobID
        self.IDToName[self.nextID] = jobName
        self.nextID += 1
        logger.debug("DRMAA Issued the job command: %s with job id: %s " % (command, str(jobName)))
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
            self.issuedJobNames.remove(jobName)
            del self.IDToName[jobID]
            del self.nameToID[jobName]
            print "Return updated id %d" % jobID
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
