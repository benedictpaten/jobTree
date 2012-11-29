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

from sonLib.bioio import logger
from sonLib.bioio import system
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from jobTree.src.master import getParasolResultsFileName

from jobTree.batchSystems.multijob import MultiTarget
from jobTree.batchSystems.gridengine import MemoryString

def prepareQsub(cpu, mem):
    qsubline = ["qsub","-terse","-j", "oe", "-d", ".", "-o", "/dev/null", "-e", "/dev/null", "-v",
                     "LD_LIBRARY_PATH=\'%s\'" % os.environ["LD_LIBRARY_PATH"]]
    reqline = list()
    if cpu is not None:
        reqline.append("p="+str(cpu))
    if mem is not None:
        reqline.append("vf="+str(mem/ 1024)+"K")
    if len(reqline) > 0:
        qsubline.extend(["-hard","-l", ",".join(reqline)])
    return qsubline

def qsub(qsubline):
    logger.debug("**"+" ".join(qsubline))
    process = subprocess.Popen(qsubline, stdout=subprocess.PIPE)
    result = int(process.stdout.readline().strip().split('.')[0])
    logger.debug("Got the job id: %s" % (str(result)))
    return result

def getjobexitcode(sgeJobID):
        job, task = sgeJobID
        args = ["qstat", "-f", str(job)]
#        if task is not None:
#             args.extend(["-t", str(task)])

        process = subprocess.Popen(args, stdout = subprocess.PIPE,stderr = subprocess.STDOUT)
        for line in process.stdout:
            if line.startswith("failed") and int(line.split()[1]) == 1:
                return 1
            elif line.startswith("exit_status"):
                return int(line.split()[2])
        return None

class Worker(Thread):
    def __init__(self, newJobsQueue, updatedJobsQueue, boss):
        Thread.__init__(self)
        self.newJobsQueue = newJobsQueue
        self.updatedJobsQueue = updatedJobsQueue
        self.currentjobs = list()
        self.runningjobs = set()
        self.boss = boss
        
    def run(self):
        while True:
            # Load new job ids:
            while not self.newJobsQueue.empty():
                self.currentjobs.append(self.newJobsQueue.get())

            # Launch jobs as necessary:
            while len(self.currentjobs) > 0 and len(self.runningjobs) < int(self.boss.config.attrib["max_jobs"]):
                jobID, qsubline = self.currentjobs.pop()
                sgeJobID = qsub(qsubline)
                self.boss.jobIDs[(sgeJobID, None)] = jobID
                self.boss.sgeJobIDs[jobID] = (sgeJobID, None)
                self.runningjobs.add((sgeJobID, None))

            # Test known job list
            for sgeJobID in list(self.runningjobs):
                exit = getjobexitcode(sgeJobID)
                if exit is not None:
                    self.updatedJobsQueue.put((sgeJobID, exit))
                    self.runningjobs.remove(sgeJobID)

            time.sleep(10)

class TorqueBatchSystem(AbstractBatchSystem):
    """The interface for torque.
    """
    
    def __init__(self, config):
        AbstractBatchSystem.__init__(self, config) #Call the parent constructor
        self.torqueResultsFile = getParasolResultsFileName(config.attrib["job_tree"])
        #Reset the job queue and results (initially, we do this again once we've killed the jobs)
        self.torqueResultsFileHandle = open(self.torqueResultsFile, 'w')
        self.torqueResultsFileHandle.close() #We lose any previous state in this file, and ensure the files existence
        self.currentjobs = set()
        self.obtainSystemConstants()
        self.jobIDs = dict()
        self.sgeJobIDs = dict()
        self.nextJobID = 0

        self.newJobsQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.worker = Worker(self.newJobsQueue, self.updatedJobsQueue, self)
        self.worker.setDaemon(True)
        self.worker.start()
        
    def __des__(self):
        #Closes the file handle associated with the results file.
        self.torqueResultsFileHandle.close() #Close the results file, cos were done.

    def issueJob(self, command, memory, cpu):
        jobID = self.nextJobID
        self.nextJobID += 1

        self.currentjobs.add(jobID)
        qsubline = prepareQsub(cpu, memory) + [command]
        self.newJobsQueue.put((jobID, qsubline))
        logger.debug("Issued the job command: %s with job id: %s " % (command, str(jobID)))
        return jobID

    def getSgeID(self, jobID):
        if not jobID in self.sgeJobIDs:
             RuntimeError("Unknown jobID, could not be converted")

        (job,task) = self.sgeJobIDs[jobID]
        if task is None:
             return str(job) 
        else:
             return str(job) + "." + str(task)
    
    def killJobs(self, jobIDs):
        """Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        for jobID in jobIDs:
            self.currentjobs.remove(jobID)
            process = subprocess.Popen(["qdel", self.getSgeID(jobID)])
            del self.jobIDs[self.sgeJobIDs[jobID]]
            del self.sgeJobIDs[jobID]

        toKill = set(jobIDs)
        while len(toKill) > 0:
            for jobID in list(toKill):
                if getjobexitcode(self.sgeJobIDs[jobID]) is not None:
                    toKill.remove(jobID)

            if len(toKill) > 0:
                logger.critical("Tried to kill some jobs, but something happened and they are still going, so I'll try again")
                time.sleep(5)
    
    def getIssuedJobIDs(self):
        """Gets the list of jobs issued to parasol.
        """
        #Example issued job, first field is jobID, last is the results file
        #31816891 localhost  benedictpaten 2009/07/23 10:54:09 python ~/Desktop/out.txt           
        return self.currentjobs
    
    def getRunningJobIDs(self):
        times = {}
        currentjobs = set(self.sgeJobIDs[x] for x in self.getIssuedJobIDs())
        process = subprocess.Popen(["qstat"], stdout = subprocess.PIPE)
        
        for currline in process.stdout:
            items = curline.strip().split()
            if ((len(items) > 9 and (items[0],items[9]) in currentjobs) or (items[0], None) in currentjobs) and items[4] == 'r':
                jobstart = " ".join(items[5:7])
                jobstart = time.mktime(time.strptime(jobstart,"%m/%d/%Y %H:%M:%S"))
                times[self.jobIDs[(items[0], items[9])]] = time.time() - jobstart 

        return times
    
    def getUpdatedJob(self, maxWait):
        i = None
        try:
            sgeJobID, retcode = self.updatedJobsQueue.get(timeout=maxWait)
            self.updatedJobsQueue.task_done()
            i = (self.jobIDs[sgeJobID], retcode)
            self.currentjobs -= set([self.jobIDs[sgeJobID]])
        except Empty:
            pass

        return i
    
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
