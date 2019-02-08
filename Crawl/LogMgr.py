class LogType:
    LOG_INFO = 1;
    LOG_ERROR = 2;
def LogInfo(logType, logContent):
    print ("log: %s %s ") % ( logType, logContent)