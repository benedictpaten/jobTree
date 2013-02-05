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

# (torque) http://sourceforge.net/projects/pbspro-drmaa/
# Python Wrapper http://code.google.com/p/drmaa-python/
import drmaa

from sonLib.bioio import logger
from sonLib.bioio import system
from jobTree.batchSystems.drmaaBase import DrmaaBatchSystem

class DrmaaTorqueBatchSystem(DrmaaBatchSystem):
    """DRMAA isn't so great at qsub options.  So we need
    to reimplement this part for particular batch systems.  This
    one is designed for PBS/Torque as run on gordon.sdsc.edu
    """

    def __init__(self, config):
        DrmaaBatchSystem.__init__(self, config)

    def __del__(self):
        DrmaaBatchSystem.__del__(self, config)

    # Submit job using qsub
    def submitJob(self, command, memory, cpu):
        # we don't presently have a way of estimating running time.
        # if we did, we could pass to to pbs/torque as follows:
        #timeSeconds = 10
        #walltime = str(datetime.timedelta(seconds=timeSeconds))
        #lString = "walltime=%s,mem=%d,nodes=1:ppn=%d:native" % (walltime, memory, cpu)
        # but for now, we forget about time and just give memory and processors
        lString = "mem=%db,nodes=1:ppn=%d:native" % (memory, cpu)
        
        process = subprocess.Popen(['qsub', '-l', lString, '-N', 'jobTree',
                                    '-j', 'oe', '-o', '/dev/null',
                                    '-e', '/dev/null'],
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE)
        jobName, nothing = process.communicate(command)
        logger.debug("DrmaaTorque Issued the job command: %s with job id: %s " % (command, str(jobName)))
        return jobName.strip()
