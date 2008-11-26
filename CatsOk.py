#! /usr/bin/python
#


import sys, os, select, pg, time, traceback, datetime, socket, EpicsCA
#import Bang



class CatsOkError( Exception):
    value = None

    def __init__( self, value):
        self.value = value
        print >> sys.stderr, sys.exc_info()[0]
        print >> sys.stderr, '-'*60
        traceback.print_exc(file=sys.stderr)
        print >> sys.stderr, '-'*60

    def __str__( self):
        return repr( self.value)


class _Q:
    
    db = None   # our database connection

    def open( self):
        self.db = pg.connect( dbname="ls", host="contrabass.ls-cat.org", user="lsuser" )

    def close( self):
        self.db.close()

    def __init__( self):
        self.open()

    def reset( self):
        self.db.reset()

    def query( self, qs):
        if qs == '':
            return rtn
        if self.db.status == 0:
            self.reset()
        try:
            # ping the server
            qr = self.db.query(qs)
        except:
            print "Failed query: %s" % (qs)
            if self.db.status == 1:
                print >> sys.stderr, sys.exc_info()[0]
                print >> sys.stderr, '-'*60
                traceback.print_exc(file=sys.stderr)
                print >> sys.stderr, '-'*60
                return None
            # reset the connection, should
            # put in logic here to deal with transactions
            # as transactions are rolled back
            #
            self.db.reset()
            if self.db.status != 1:
                # Bad status even after a reset, bail
                raise CatsOkError( 'Database Connection Lost')

            qr = self.db.query( qs)

        return qr

    def dictresult( self, qr):
        return qr.dictresult()

    def e( self, s):
        return pg.escape_string( s)

    def fileno( self):
        return self.db.fileno()

    def getnotify( self):
        return self.db.getnotify()

class CatsOk:
    """
    Monitors status of the cats robot, updates database, and controls CatsOk lock
    """
    workingPath = ""    # path we are currently running
    afterCmd    = []
    pathsNeedingAirRights = [
        "put", "put_bcrd", "get", "getput", "getput_bcrd"
        ]

    sampleMounted = { "lid" : None, "sample" : None, "timestamp" : None}
    sampleTooled  = { "lid" : None, "sample" : None, "timestamp" : None}
    checkMountedSample = False

    dbFlag       = True        # indicates a command might still be in the queue
    lastPathName  = ""
    needAirRights = False
    haveAirRights = False

    robotOn = None
    robotInRemote = None
    robotError = None
    diES       = None

    CATSOkRelayPVName = '21:F1:pmac10:acc65e:1:bo0'
    CATSOkRelayPV     = None            # the epics pv of our relay
    termstr = "\r"      # character expected to terminate cats response
    db = None           # database connection

    t1 = None           # socket object used to make connection to cats control socket
    t1Input = ""        # used to build response from t1

    t2 = None           # socket object used to make connection to cats status socket
    t2Input = ""        # used to build response from t2

    p = None            # poll object to manage active file descriptors

    fan = {}            # Dictionary used to find the appropriate service routine for each socket

    sFan = {}           # Dictionary used to parse status socket messages

    MD2 = None          # Sphere Defining MD2 location exclusion zone
    RCap = None         # Capsule defining robot tool and arm
    RR   = 0.7          # Distance from tool tip to elbow
    Pr2 = False         # Process Output 2: CATS requires Air Rights

    statusStateLast    = None
    statusIoLast       = None
    statusPositionLast = None
    statusMessageLast  = None
    waiting = False     # true when the last status reponse did not contain an entire message (more to come)
    cmdQueue = []       # queue of commands received from postgresql
    statusQueue = []    # queue of status requests to send to cats server
    statusFailedPushCount = 0


    def maybePushStatus( self, rqst):
        if self.t2 == None:
            # send requests to the bit bucket if the server is not connected
            return False
        try:
            i = self.statusQueue.index( rqst)
        except ValueError:
            self.statusQueue.append( rqst)
            self.p.register( self.t2, select.POLLIN | select.POLLPRI | select.POLLOUT)
            return True

        self.statusFailedPushCount += 1
        if self.statusFailedPushCount >=10:
            print "Exceeded failure count"
            return False
        self.statusFailedPushCount = 0
        return True

    def popStatus( self):
        rtn = None
        if len( self.statusQueue):
            rtn = self.statusQueue.pop()
        else:
            self.p.register( self.t2, select.POLLIN | select.POLLPRI)
        return rtn

    def pushCmd( self, cmd):
        print "pushing command '%s'" % (cmd)
        if cmd == 'abort' or cmd == 'panic':
            self.cmdQueue = []
            self.afterCmd = []
        self.cmdQueue.append( cmd)
        self.p.register( self.t1, select.POLLIN | select.POLLPRI | select.POLLOUT)


    def nextCmd( self):
        rtn = None
        if len( self.cmdQueue) > 0:
            rtn = self.cmdQueue.pop(0)
        else:
            self.p.register( self.t1, select.POLLIN | select.POLLPRI)

        return rtn

    def dbService( self, event):

        if event & select.POLLERR:
            self.db.reset()

        if event & (select.POLLIN | select.POLLPRI):
            #
            # Eat up any accumulated notifies
            #
            ntfy = self.db.getnotify()
            while  ntfy != None:
                print "Received Notify: ", ntfy
                ntfy = self.db.getnotify()

            #
            # grab a waiting command
            #
            self.dbFlag = True
            while( self.dbFlag):
                qr = self.db.query( "select cats._popqueue() as cmd")
                r = qr.dictresult()[0]
                if len( r["cmd"]) > 0:
                    cmd = r["cmd"]
                else:
                    if self.workingPath == "" and len(self.afterCmd) > 0:
                        print "Queuing Command: ", self.afterCmd[0]
                        self.pushCmd( self.afterCmd[0])
                        self.workingPath = self.afterCmd[0]
                        self.afterCmd.pop(0)
                    self.dbFlag = False
                    return True

                #
                # does this look like a normal path command that we might want to delay?
                #
                if cmd.find( "(") > 0:
                    try:
                        # Pick off the path name and test it against those needing air rights: We'll need a second list of commands to test if we ever want to call one that does not need air rights
                        trialPath = cmd[0:cmd.find("(")]
                        ndx = self.pathsNeedingAirRights.index( trialPath)
                    except ValueError:
                        #
                        # No path found: just push the command and hope for the best
                        self.pushCmd( cmd)
                        self.workingPath = cmd
                    else:
                        #
                        # We found one: save it for later if we are busy now
                        #
                        if self.workingPath != "":
                            self.afterCmd.append(cmd)
                            print "Saving Command: ", cmd
                        else:
                            self.pushCmd( cmd)
                            self.workingPath = cmd
                else:
                    self.pushCmd( cmd)
                    self.workingPath = cmd
        return True

    #
    # Service reply from Command socket
    #
    def t1Service( self, event):
        if event & select.POLLOUT:
            cmd = self.nextCmd()
            if cmd != None:
                print "sending command '%s'" % (cmd)
                self.t1.send( cmd + self.termstr)

        if event & (select.POLLIN | select.POLLPRI):
            newStr = self.t1.recv( 4096)
            if len(newStr) == 0:
                self.p.unregister( self.t1.fileno())
                return False

            print "Received:", newStr
            str = self.t1Input + newStr
            pList = str.replace('\n','\r').split( self.termstr)
            if len( str) > 0 and str[-2:-1] != self.termstr:
                self.t1Input = pList.pop( -1)
        
            for s in pList:
                print s
        return True

    #
    # Service reply from status socket
    def t2Service( self, event):
        if event & (select.POLLIN | select.POLLPRI):
            try:
                newStr = self.t2.recv( 4096)
            except socket.error:
                self.p.unregister( self.t2.fileno())
                self.t2 = None
                return False

            #
            # zero length return means an error, most likely the socket has shut down
            if len(newStr) == 0:
                self.p.unregister( self.t2.fileno())
                self.t2 = None
                return False

            # Assume we have the full response
            self.waiting = False

            #print "Status Received:", newStr.strip()

            #
            # add what we have from what was left over from last time
            str = self.t2Input + newStr
        
            #
            # split the string into an array of responses: each distinct reply ends with termstr
            pList = str.replace('\n','\r').split( self.termstr)
            if len( str) > 0 and str[-2:-1] != self.termstr:
                # if the last entry does not end with a termstr then there is more to come, save it for later
                #
                self.t2Input = pList.pop( -1)
        
            #
            # go through the list of completed status responses
            for s in pList:
                sFound = False
                for ss in self.sFan:
                    if s.startswith( ss):
                        # call the status response function
                        self.sFan[ss](s)
                        sFound = True
                        break
                if not sFound:
                    # if we do not recognize the response then it must be a message
                    self.statusMessageParse( s)

        if event & select.POLLOUT:
            s = self.popStatus()
            if s != None:
                #print "status request: ", s
                self.t2.send( s + self.termstr)

        return True

    def __init__( self):
        # See if we are on a path that requires air rights

        #
        # establish connecitons to database server
        self.db = _Q()

        #
        # establish connections to CATS sockets
        qr = self.db.query("select px.getcatsaddr() as a")
        catsaddr = qr.dictresult()[0]["a"]

        self.t1 = socket.socket( socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.t1.connect( ( catsaddr, 1000))
        except socket.error:
            raise CatsOkError( "Could not connect to command port")
        self.t1.setblocking( 1)


        self.t2 = socket.socket( socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.t2.connect( ( catsaddr, 10000))
        except socket.error:
            raise CatsOkError( "Could not connect to status port")
        self.t2.setblocking( 1)

        #
        # Listen to db requests
        self.db.query( "select cats.init()")


        #
        # Get the epics relay pv
        self.CATSOkRelayPV = EpicsCA.PV( self.CATSOkRelayPVName)
        # initially set this high
        self.CATSOkRelayPV.put( 1)


        #
        # Set up poll object
        self.p = select.poll()
        self.p.register( self.t1.fileno(), select.POLLIN | select.POLLPRI)
        self.p.register( self.t2.fileno(), select.POLLIN | select.POLLPRI)
        self.p.register( self.db.fileno(), select.POLLIN | select.POLLPRI)

        #
        # Set up fan to unmultiplex the poll response
        self.fan = {
            self.t1.fileno()     : self.t1Service,
            self.t2.fileno()     : self.t2Service,
            self.db.fileno()     : self.dbService,
            }

        #
        # Set up sFan to handle status socket messages
        self.sFan = {
            "state("    : self.statusStateParse,
            "io("       : self.statusIoParse,
            "di("       : self.statusDiParse,
            "do("       : self.statusDoParse,
            "position(" : self.statusPositionParse,
            "config"   : self.statusConfigParse
            }

        self.srqst = {
            "state"     : { "period" : 0.55, "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            # "io"        : { "period" : 0.5, "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "do"        : { "period" : 0.5, "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "di"        : { "period" : 0.6, "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            # "position"  : { "period" : 0.5, "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "message"   : { "period" : 0.65, "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0}
            }

        # self.MD2 = Bang.Sphere( Bang.Point( 0.5, 0.5, 0.5), 0.5)

    def close( self):
        if self.t1 != None:
            self.t1.close()
            self.t1 = None
        if self.t2 != None:
            self.t2.close()
            self.t2 = None
        if self.db != None:
            self.db.close()
            self.db = None

    def run( self):
        runFlag = True
        self.pushCmd( "vdi90off")
        lastDbTime = datetime.datetime.now()
        while( runFlag):
            for ( fd, event) in self.p.poll( 100):
                runFlag = runFlag and self.fan[fd](event)
                if not runFlag:
                    break
            n = datetime.datetime.now()
            if runFlag and not self.waiting:
                #
                # queue up new requests if it is time to
                for k in self.srqst.keys():
                    r = self.srqst[k]
                    if r["last"] == None or (n - r["last"] > datetime.timedelta(0,r["period"])):
                        runFlag &= self.maybePushStatus( k)
                        r["last"] = datetime.datetime.now()
                        r["rqstCnt"] += 1

            if runFlag and (self.dbFlag or (n - lastDbTime > datetime.timedelta(0, 1))):
                    lastDbTime = n
                    runFlag &= self.dbService( select.POLLIN)

            if runFlag and self.needAirRights and not self.haveAirRights:
                qs = "select px.requestRobotAirRights() as rslt"
                qr = self.db.query( qs)
                rslt = qr.dictresult()[0]["rslt"]
                if rslt == "t":
                    self.haveAirRights = True
                    self.pushCmd( "vdi90on")
                    print "received haveAirRights and setting vdi90on"

            if runFlag and not self.needAirRights and self.haveAirRights:
                self.db.query( "select px.dropRobotAirRights()")    # drop rights and send notify that sample is ready (if it is)
                self.pushCmd( "vdi90off")
                self.haveAirRights = False
                print "dropped Air Rights and setting vdi90off"

        self.close()

    def statusStateParse( self, s):
        self.srqst["state"]["rcvdCnt"] = self.srqst["state"]["rcvdCnt"] + 1

        # One line command to an argument list
        a = s[s.find("(")+1 : s.find(")")].split(',')

        if len(a) != 15:
            #print s
            raise CatsOkError( 'Wrong number of arguments received in status state response: got %d, exptected 15' % (len(a)))
        #                            0            1            2             3           4          5   6   7   8   9  10   11         12           13           14
        if self.statusStateLast == None or self.statusStateLast != s:
            b = []
            i = 0
            #             0           1           2         3       4         5      6       7       8        9     10        11        12          13         14
            aType = ["::boolean","::boolean","::boolean","::text","::text","::int","::int","::int","::int","::int","::int","::text","::boolean","::boolean","::boolean"]
            qs = "select cats.setstate( "

            needComma = False
            for zz in a:
                if zz == None or len(zz) == 0:
                    b.append("NULL")
                else:
                    b.append( "'%s'%s" % (zz, aType[i]))

                if needComma:
                    qs = qs+","
                qs = qs + b[i]
                i = i+1
                needComma = True

            qs = qs + ")"
            self.db.query( qs)
            self.statusStateLast = s


        if self.sampleMounted["lid"] != a[7] or self.sampleMounted["sample"] != a[8]:
            self.sampleMounted["lid"] = a[7]
            self.sampleMounted["sample"] = a[8]
            self.sampleMounted["timestamp"] = datetime.datetime.now()
            self.checkMountedSample = True


        if self.sampleTooled["lid"] != a[7] or self.sampleTooled["sample"] != a[8]:
            self.sampleTooled["lid"] = a[7]
            self.sampleTooled["sample"] = a[8]
            self.sampleTooled["timestamp"] = datetime.datetime.now()



        self.robotOn = a[0]      == "1"
        self.robotInRemote= a[1] == "1"
        self.robotError = a[2]   == "1"

        # things to do when in remote mode
        if self.robotInRemote:
            if self.robotError or not self.diES:
                self.pushCmd( "reset")
            elif not self.robotOn:
                self.pushCmd( "on")

        # Nab air rights when we embark on a path requiring them
        pathName = a[4]
        self.workingPath = pathName
        if self.lastPathName != pathName and not self.haveAirRights:
            try:
                ndx = self.pathsNeedingAirRights.index(pathName)
            except ValueError:
                self.needAirRights = False
                if pathName != "":
                    print "Current path '%s' does not need air rights" % (pathName)
                else:
                    print "No path running"
            else:
                print "Need Air Rights for path '%s'" % (pathName)
                self.needAirRights = True
        self.lastPathName = pathName

        if self.checkMountedSample and pathName != 'get' and (datetime.datetime.now() - self.sampleMounted["timestamp"] > datetime.timedelta(0,3)):
            self.checkMountedSample = False
            qr = self.db.query( "select px.getmagnetstate() as ms")
            ms = qr.dictresult()[0]["ms"]
            print "Sample Mounted: ", ms
            print self.sampleMounted
            if ms == "t" and (self.sampleMounted["lid"] == "" or self.sampleMounted["sample"] == ""):
                print "Sample on diffractometer but robot thinks there isn't: aborting"
                self.pushCmd( "abort")
                self.needAirRights = False

    def statusDoParse( self, s):
        self.srqst["do"]["rcvdCnt"] = self.srqst["do"]["rcvdCnt"] + 1
        #print "do:", s
        do = s[s.find("(")+1:s.find(")")]
        if do[0] != "1" and do[0] != "0":
            print "Bad 'do' returned: %s" % (do)
            return
        else:
            qs = "select cats.setdo( b'%s')" % (do)
            self.db.query( qs)

        # give up air rights on falling edge of Pr2
        lastPr2 = self.Pr2
        self.Pr2 = do[5] == "1"
        if lastPr2 and not self.Pr2:
            self.needAirRights = False


    def statusDiParse( self, s):
        self.srqst["di"]["rcvdCnt"] = self.srqst["di"]["rcvdCnt"] + 1
        #print "di: ", s
        di = s[s.find("(")+1:s.find(")")]

        qs = "select cats.setdi( b'%s')" % (di)
        self.db.query( qs)

        self.diES  = di[1] == "1"
        #print "diES: ", self.diES

    def statusIoParse( self, s):
        self.srqst["io"]["rcvdCnt"] = self.srqst["io"]["rcvdCnt"] + 1
        if self.statusIoLast != None and self.statusIoLast == s:
            #self.db.query( "select cats.setio()")
            #self.db.query( "execute io_noArgs")
            return

        # One line command to pull out the arguments as one string
        # hope this is in the right format to send to postresql server

        a = s[s.find("(")+1:s.find(")")]
        b = a.replace( "1", "'t'")
        c = b.replace( "0", "'f'")

        qs = "select cats.setio( %s)" % (c)
        #print s,qs
        self.db.query( qs)
        self.statusIoLast = s
        

    def statusPositionParse( self, s):
        self.srqst["position"]["rcvdCnt"] = self.srqst["position"]["rcvdCnt"] + 1
        if self.statusPositionLast != None and self.statusPositionLast == s:
            #self.db.query( "select cats.setposition()")
            #self.db.query( "execute position_noArgs")
            return

        # One line command to an argument list
        a = s[s.find("(")+1 : s.find(")")].split(',')

        if len(a) != 6:
            raise CatsOkError( 'Wrong number of arguments received in status state response: got %d, exptected 14' % (len(a)))
        #                               0   1   2   3   4  5
        qs = "select cats.setposition( %s, %s, %s, %s, %s, %s)" %  ( a[0], a[1], a[2], a[3], a[4], a[5])
        self.db.query( qs)
        self.statusPositionLast = s

        # self.RCap( Bang.Point( float(a[0]), float(a[1]), float(a[2]), Bang.Point( float

    def statusConfigParse( self, s):
        pass
    
    def statusMessageParse( self, s):
        self.srqst["message"]["rcvdCnt"] = self.srqst["message"]["rcvdCnt"] + 1
        if self.statusMessageLast != None and self.statusMessageLast == s:
            #self.db.query( "select cats.setmessage()")
            #self.db.query( "execute message_noArgs")
            return

        qs = "select cats.setmessage( '%s')" % (s)
        self.db.query( qs)
        self.statusMessageLast = s

        

#
# Default usage
#
if __name__ == '__main__':
    iFlag = True
    rFlag = True

    while rFlag:
        while iFlag:
            try:
                z = CatsOk()
            except CatsOkError, e:
                time.sleep( 10)
            else:
                iFlag = False
        try:
            z.run()
        except CatsOkError:
            z.close()
        time.sleep( 10)
        iFlag = True

