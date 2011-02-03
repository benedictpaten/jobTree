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

import sys
import os

try:
    import cPickle 
except ImportError:
    import pickle as cPickle

from workflow.jobTree.lib.bioio import getTempFile
from workflow.jobTree.lib.bioio import system

class MultiTarget():
    def __init__(self, commands):
        self.commands = commands

    def execute(self):
        task_id = int(os.environ['SGE_TASK_ID'])
        if task_id is None:
             RuntimeError("Multi-target launched without task id") 
        if task_id < 1 or task_id > len(self.commands):
             RuntimeError("Task ID not within the array range 1 <= %i <= %i", task_id, len(self.commands))
        (job, outfile) = self.commands[task_id - 1]
        if outfile is None:
                system(job)
        else:
                system("%s >& %s" % (job, outfile))

    def makeRunnable(self, tempDir):
        pickleFile = tempDir.getTempFile(".pickle")
        fileHandle = open(pickleFile, 'w')
        cPickle.dump(self, fileHandle, cPickle.HIGHEST_PROTOCOL)
        fileHandle.close() 
        return "multijob %s" % (pickleFile)

if __name__ == "__main__":
        fileHandle = open(sys.argv[1], 'r')
        multitarget = cPickle.load(fileHandle)
        fileHandle.close()
        multitarget.execute() 
