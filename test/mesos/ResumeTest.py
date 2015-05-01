import sys
from time import sleep
from jobTree.src.target import Target
from jobTree.src.stack import Stack
from optparse import OptionParser

class LongTest(Target):
    def __init__(self, seconds):
        Target.__init__(self, time=1, memory=1000000, cpu=1)
        self.seconds =seconds

    def run(self):
        sleep(self.seconds)
        for i in range(1,3):
            self.addChildTarget(HelloWorld(i))
        self.setFollowOnTarget(HelloWorldFollow(self.seconds))


class HelloWorld(Target):

    def __init__(self,i):
        Target.__init__(self, time=1, memory=100000, cpu=0.5)
        self.i=i

    def run(self):
        with open ('hello_world_child{}.txt'.format(self.i), 'w') as file:
            file.write('This is a triumph')


class HelloWorldFollow(Target):

    def __init__(self, seconds):
        #sleep(seconds)
        Target.__init__(self, time=1, memory=1000000, cpu=1)

    def run(self):
        with open ('hello_world_follow.txt', 'w') as file:
            file.write('This is a triumph')


def run(seconds):
    # Boilerplate -- startJobTree requires options
    sys.argv.append("--batchSystem=mesos")
    parser = OptionParser()
    Stack.addJobTreeOptions(parser)
    options, args = parser.parse_args()

    # Setup the job stack and launch jobTree job
    i = Stack(LongTest(seconds)).startJobTree(options)


