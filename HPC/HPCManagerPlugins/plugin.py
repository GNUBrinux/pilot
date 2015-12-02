
from HPC.Logger import Logger

class Plugin:
    def __init__(self, logFileName):
        self.__log= Logger(logFileName)

    def getHPCResources(self, partition, max_nodes=None, min_nodes=2, min_walltime_m=30):
        pass

    def submitJob(self):
        pass