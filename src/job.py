try:
    import cPickle
except ImportError:
    import pickle as cPickle

def readJob(jobFile):
    fileHandle = open(jobFile, 'r')
    job = cPickle.load(fileHandle)
    fileHandle.close()
    return job

def getJobFileName(globalTempDir):
    return os.path.join(globalTempDir, "job.xml")

def getStatsFileName(globalTempDir):
    return os.path.join(globalTempDir, "stats.xml")

class Job:
    #Colours for job
    blue = 0
    black = 1
    grey = 2
    red = 3
    dead = 4
    
    def __init__(self, command, memory, cpu, parentPath, config):
        self.globalTempDir = config.attrib["job_file_tree"].getTempDirectory()
        self.remainingRetryCount = int(config.attrib["retry_count"])
        self.colour = "grey"
        self.parentPath = parentPath
        self.childCount = 0
        self.blackChildCount = 0
        self.childCommands = []
        self.followOns = []
        self.messages = []
        self.addFollowOnCommand(command, memory, cpu)
    
    def write(self, jobFile):
        fileHandle = open(jobFile, 'w')
        cPickle.dump(self, fileHandle, cPickle.HIGHEST_PROTOCOL)
        fileHandle.close() 
    
    def getJobFileName(self):
        return getJobFileName(self.globalTempDir)
    
    def getSlaveLogFileName(self):
        return os.path.join(self.globalTempDir, "slave_log.txt")
        
    def getLogFileName(self):
        return os.path.join(self.globalTempDir, "log.txt")
        
    def getGlobalTempDirName(self):
        return self.globalTempDir
        
    def getJobStatsFileName(self):
        return getStatsFileName(self.globalTempDir)
    
    def getRemainingRetryCount(self):
        return self.remainingRetryCount
    
    def reduceRemainingRetryCount(self):
        self.remainingRetryCount -= 1
    
    def setRemainingRetryCount(self, remainingRetryCount):
        self.remainingRetryCount = remainingRetryCount
    
    def getParentPath(self):
        return self.parentPath
    
    def getChildCount(self):
        return self.childCount
    
    def increaseChildCount(self, increment=1):
        self.childCount += increment
    
    def getBlackChildCount(self):
        return self.blackChildCount
    
    def increaseBlackChildCount(self, increment=1):
        self.blackChildCount += increment
        
    def getColour(self):
        return self.colour
    
    def setColour(self, colour):
        self.colour = colour 
        
    def getNumberOfFollowOnCommands(self):
        return len(self.followOns)
        
    def getFollowOnCommand(self):
        return self.followOns[-1]
    
    def popFollowOnCommand(self):
        return self.followOns.pop()
    
    def addFollowOnCommand(self, command, memory, cpu):
        self.followOns.append((command, memory, cpu))
        
    def getNumberOfChildCommands(self):
        return len(self.childCommands)
        
    def removeChildren(self):
        children = self.childCommands
        self.childCommands = []
        return children
        
    def addChildCommand(self, command, memory, cpu):
        self.childCommands.append((command, memory, cpu))
        
    def getNumberOfMessages(self):
        return len(self.messages)
    
    def removeMessages(self):
        messages = self.messages
        self.messages = []
        return messages