import os
import sys
import commands

# exec
def run(inFile,v_onlyTA):
    import cPickle as pickle
    print 'ln8'
    try:
        # read Jobs from file
        f = open(inFile)
        jobs = pickle.load(f)
        f.close()
        print 'ln14'
        print 'ln15 jobs=' + str([x.valuesMap() for x in jobs])
    except:
        print 'ln16'
        type, value, traceBack = sys.exc_info()
        print("aaaa run() : %s %s" % (type, value))
        return
    print 'ln21'
    # password
    from config import panda_config
    passwd = panda_config.dbpasswd
    # initialize cx_Oracle using dummy connection
    from taskbuffer.Initializer import initializer
    initializer.init()
    # instantiate TB
    from taskbuffer.TaskBuffer import taskBuffer
    taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
    # run Setupper
    from dataservice.Setupper import Setupper
    print 'ln33'
    thr = Setupper(taskBuffer,jobs,onlyTA=v_onlyTA,useNativeDQ2=True)
    print 'ln35'
    thr.start()
    print 'ln37'
    thr.join()
    print 'ln39'
    return


# exit action
def _onExit(fname):
    commands.getoutput('rm -rf %s' % fname)
        

####################################################################
# main
def main():
    import getopt
    import atexit
    # option class
    class _options:
        def __init__(self):
            pass
    options = _options()
    del _options
    # set default values
    options.inFile  = ""
    options.onlyTA  = False
    # get command-line parameters
    try:
        opts, args = getopt.getopt(sys.argv[1:],"i:t")
    except:
        print("ERROR : Invalid options")
        sys.exit(1)    
    # set options
    for o, a in opts:
        if o in ("-i",):
            options.inFile = a
        if o in ("-t",):
            options.onlyTA = True
    # exit action
    atexit.register(_onExit,options.inFile)
    # run
    run(options.inFile,options.onlyTA)
    # return
    sys.exit(0)


if __name__ == "__main__":
    main()
