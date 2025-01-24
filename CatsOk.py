#! /usr/bin/python
#
#
# Support for data collection at LS-CAT
#
# Copyright 2008-2015 by Keith Brister, Northwestern University
#
#   This file is part of the LS-CAT Beamline Control Package which is
#   free software: you can redistribute it and/or modify it under the
#   terms of the GNU General Public License as published by the Free
#   Software Foundation, either version 3 of the License, or (at your
#   option) any later version.
#
#   This software is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#   General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
#
#
# Support for CATS robot control
#
#

#
# Delay before checking to see if the sample dismouted successfully in
# seconds.  Should be either

#   1) Set so that the check occurs after the old sample would have
#      been replaced but before the next sample has been picked up AND
#      when the gripper is not in the LN2 (Important for when using
#      the smart magnet to detect the old cap.)
#
#   2) Set so that the check occurs any time when the gripper is not
#      in the LN2 AND before it picks up the next sample.  Use this
#      when the video cap detector is used.  This timing is not as
#      strict as the first case.
#
#   3) Set to "None".  Use this when VDI92 is configured in the CATS
#      code to do its own cap check at a place in the path where it
#      makes sense to do so.

dismountSampleCheckDelaySeconds = 10

import sys              # stderr
import select           # poll
import pg               # our db world
import time             # sleep between invocations
import traceback        # error tracking
import datetime         # our time format
import socket           # communications with the robot itself
import redis            # new database model


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


class _R:
    r       = None      # our connection
    rdy     = False     # true when it looks like our connection is ok and redis is configured
    head    = None      # the start of all our keys in the redis database
    robopub = None      # our pen name
    ourKVs  = {}        # a list of our KV pairs

    def getconfig( self):
        hn = socket.gethostname()
        try:
            self.head = self.r.hget( 'config.%s' % (hn.lower()), 'HEAD')
            if self.head == None or self.head == '':
                print >> sys.stderr, 'Redis is not configured for this host "%s"' % (hn)
                self.rdy = False
                self.head = None
                return

            self.robopub = self.r.hget( 'config.%s' % (hn), 'ROBOPUB')

        except redis.exceptions.ConnectionError:
            print >> sys.stderr, 'Redis connection error.  Is it running?'
            self.rdy = False
            self.head = None
            return


    def __init__( self):
        self.r = redis.Redis()          # should all be defaults
        self.getconfig()

    def set( self, k, v):
        if self.ourKVs.has_key( k) and self.ourKVs[k] == v:
            return

        try:
            if self.r.ping():
                if self.head == None:
                    self.getconfig()
                if self.head == None:
                    return
                
                bigk = "%s.%s" % (self.head, k)

                self.r.hmset( bigk, {'KEY': bigk, 'VALUE':v})

                if self.robopub:
                    self.r.publish( self.robopub, bigk)

                self.ourKVs[k] = v
        except:
            print >> sys.stderr, "Redis error setting key '%s' to value '%s'" % (k, v)


    def get( self, k):
        try:
            if self.r.ping():
                if self.head == None:
                    self.getconfig()
                if self.head == None:
                    return None

            bigk = "%s.%s" % (self.head, k)
            v = self.r.hget( bigk, "VALUE")

            self.ourKVs[k] = v
            return v
        except:
            print >> sys.stderr, "Redis error getting key '%s'" % (k)


class _Q:
    
    db = None   # our database connection

    def open( self):
        self.db = pg.connect(dbname="ls", host="postgres.ls-cat.net", user="lsuser")

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
    oldStr = None       # used to keep from spamming console
    oldSendCmd = None
    oldPushCmd = None
    inRecoveryMode = False
    workingPath = ""    # path we are currently running
    pathsNeedingAirRights = [
        "put", "put_bcrd", "get", "getput", "getput_bcrd", "test"
        ]

    sampleMounted = { "lid" : None, "sample" : None, "timestamp" : None}
    sampleTooled  = { "lid" : None, "sample" : None, "timestamp" : None}
    checkMountedSample = False
    capDetected   = None

    dbFlag       = True        # indicates a command might still be in the queue
    lastPathName  = ""
    cryoLocked = None
    needAirRights = False
    haveAirRights = False
    catsExclusionZone = None
    dpX = None
    dpY = None
    dpZ = None
    inExclusionZone = False

    robotOn = None
    robotInRemote = None
    robotError = None
    diES       = None

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
                        #
                        # Command Queues
                        #  There are two queues: cmdQueue and afterCmd
                        #      cmdQueue runs the given command at the specified time
                        #      afterCmd saves path commands (with the start time) until the current path is no longer running
                        #
                        #   If a path is running then new path commands are added to afterCmd
                        #   If a path command is not running or if the command to be added is not a path command then
                        #   the command is added directly to cmdQueue
                        #   This gets around the problem that the CATS will ignore commands that it doesn't like while running a path.
                        #   But allows us to immediately run a command while a path is running if it is likely to be executed.
                        #   This is a long winded explaination.  This allows us to send vdiXX commands while a path is running while queuing up the next getput/get/put
                        #   vdi90 = airrights, so we really need to be able to do this.
                        #
                        #   vdi91 = in diffractometer air space
                        #   vdi92 = sample mounted
                        #
    cmdQueue = []       # queue of commands received from postgresql: tuple (cmd,startTime,path,tool)
    afterCmd    = []    # queue of commands to run after the current one is done
    statusQueue = []    # queue of status requests to send to cats server
    statusFailedPushCount = 0

    collision     = None # state from DI (99 digit binary numbers written in ascii is a weird design decision)
    cryoHighAlarm = None # state from DI (99 digit binary numbers written in ascii is a weird design decision)
    cryoHigh      = None # state from DI (99 digit binary numbers written in ascii is a weird design decision)
    cryoLow       = None # state from DI (99 digit binary numbers written in ascii is a weird design decision)
    cryoLowAlarm  = None # state from DI (99 digit binary numbers written in ascii is a weird design decision)
    lid1Open      = None # state from DI (99 digit binary numbers written in ascii is a weird design decision)
    lid2Open      = None # state from DI (99 digit binary numbers written in ascii is a weird design decision)
    lid3Open      = None # state from DI (99 digit binary numbers written in ascii is a weird design decision)
    toolOpen      = None # state from DI (99 digit binary numbers written in ascii is a weird design decision)
    toolClosed    = None # state from DI (99 digit binary numbers written in ascii is a weird design decision)
    pucksDetected = None # from bits DI (rank 13 through 21, aka bits 12 to 20)

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

    def compareCmdQueue( self, x, y):
        if x[1] < y[1]:
            return -1
        if x[1] > y[1]:
            return 1
        return 0

    def pushCmd( self, cmd, startTime=datetime.datetime.now(), path=None, tool=None):
        if self.oldPushCmd == None or self.oldPushCmd != cmd:
            print "pushing command '%s'" % (cmd)
            self.oldPushCmd = cmd
        if cmd == 'abort' or cmd == 'panic':
            self.cmdQueue = []
            self.afterCmd = []
            startTime = datetime.datetime.now()
        self.cmdQueue.append( (cmd, startTime, path, tool))
        self.cmdQueue.sort( self.compareCmdQueue)
        self.p.register( self.t1, select.POLLIN | select.POLLPRI | select.POLLOUT)


    def nextCmd( self):
        rtn = None
        if len( self.cmdQueue) > 0:
            if self.cmdQueue[0][1] <= datetime.datetime.now():
                if self.cmdQueue[0][2] != None:
                    tool = int(self.cmdQueue[0][3])
                    qs = "select cats.cmdTimingStart( '%s', %d)" % (self.cmdQueue[0][2], tool)
                    self.db.query( qs)
                rtn = self.cmdQueue.pop(0)[0]
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
                qr = self.db.query( "select cmd, startEpoch as se, pqpath, pqtool from cats._popqueue()")
                r = qr.dictresult()[0]
                if len( r["cmd"]) > 0:
                    cmd = r["cmd"]
                    path = r["pqpath"]
                    tool = r["pqtool"]
                    startTime = datetime.datetime.fromtimestamp(r["se"])
                    print r["se"], startTime
                    d = startTime - datetime.datetime.now()
                    print "Got command %s to be started in %d seconds" % (cmd, d.days*86400+d.seconds+d.microseconds/1000000)
                else:
                    if self.workingPath == "" and len(self.afterCmd) > 0:
                        print "Queuing Command: ", self.afterCmd[0][0]
                        self.pushCmd( self.afterCmd[0][0], self.afterCmd[0][1], self.afterCmd[0][2], self.afterCmd[0][3])
                        self.workingPath = self.afterCmd[0][0]
                        self.afterCmd.pop(0)
                    self.dbFlag = False
                    return True

                #
                # does this look like a normal path command that we might want to delay?
                #
                if cmd.find( "(") > 0:
                    try:
                        # Pick off the path name and test it against those needing air rights:
                        #  We'll need a second list of commands to test if we ever want to call one that does not need air rights
                        ndx = self.pathsNeedingAirRights.index( path)
                    except ValueError:
                        #
                        # No path found: just push the command and hope for the best
                        self.pushCmd( cmd, startTime, path, tool)
                        self.workingPath = cmd
                    else:
                        #
                        # We found one: save it for later if we are busy now
                        #
                        if self.workingPath != "":
                            self.afterCmd.append((cmd, startTime, path, tool))
                            print "Saving Command: ", cmd
                        else:
                            self.pushCmd( cmd, startTime, path, tool)
                            self.workingPath = cmd
                else:
                    self.pushCmd( cmd, startTime, path, tool)
                    self.workingPath = cmd
        return True

    #
    # Service reply from Command socket
    #
    def t1Service( self, event):
        if event & select.POLLOUT:
            cmd = self.nextCmd()
            if cmd != None:
                if self.oldSendCmd == None or self.oldSendCmd != cmd:
                    print "sending command '%s'" % (cmd)
                    self.oldSendCmd = cmd
                self.t1.send( cmd + self.termstr)

        if event & (select.POLLIN | select.POLLPRI):
            newStr = self.t1.recv( 4096)
            if len(newStr) == 0:
                self.p.unregister( self.t1.fileno())
                return False

            if self.oldStr == None or newStr != self.oldStr:
                print "Received:", newStr
                str = self.t1Input + newStr
                pList = str.replace('\n','\r').split( self.termstr)
                if len( str) > 0 and str[-2:-1] != self.termstr:
                    self.t1Input = pList.pop( -1)
        
                for s in pList:
                    print s
                self.oldStr = newStr

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
                self.t2.send( s + self.termstr)

        return True

    def __init__( self, stn=None):
        # See if we are on a path that requires air rights

        #
        # establish connecitons to database server
        self.db = _Q()

        #
        # set up our reids connection
        self.redis = _R()

        #
        # establish connections to CATS sockets
        if stn != None:
            qs = "select coalesce(px.getcatsaddr( stn='%s')::text,'Not Found') as a" % stn
        else:
            qs = "select coalesce(px.getcatsaddr()::text,'Not Found') as a"
        qr = self.db.query(qs)
        
        catsaddr = qr.dictresult()[0]["a"]
        if catsaddr == "Not Found":
            raise CatsOkError( "Robot's address not found.  Sorry.")

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
        # Find diffractometer position: assume it does not change while we are running
        #
        qs = "select dpx, dpy, dpz from cats.diffpos where dpstn=px.getstation()"
        qr = self.db.query( qs)
        r = qr.dictresult()[0]
        self.dpX = float(r["dpx"])
        self.dpY = float(r["dpy"])
        self.dpZ = float(r["dpz"])

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
            #"io("       : self.statusIoParse,
            "di("       : self.statusDiParse,
            "do("       : self.statusDoParse,
            "position(" : self.statusPositionParse,
            "config"   : self.statusConfigParse
            }

        self.srqst = {
            "state"     : { "period" : 0.55,   "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "do"        : { "period" : 0.5,    "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "di"        : { "period" : 0.6,    "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "position"  : { "period" : 0.51,   "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "config"    : { "period" : 86400,  "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "message"   : { "period" : 0.65,   "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0}
            }

        self.db.query( "select px.lockCryo()")
        self.cryoLocked = True
        self.redis.set( "robot.cryoLocked", True)
        

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
        print "starting run"
        self.pushCmd( "vdi90off")
        self.redis.set( "robot.airRights", False)
        lastDbTime = datetime.datetime.now()

        # initialize the cap detector
        if self.redis.get( "capDetected") == "1":
            self.capDetected = True
            self.pushCmd( "vdi92on")
        else:
            self.capDetected = False
            self.pushCmd( "vdi92off")
        
        while( runFlag):
            for ( fd, event) in self.p.poll( 100):
                runFlag = runFlag and self.fan[fd](event)
                if not runFlag:
                    break
            n = datetime.datetime.now()


            if runFlag and not self.waiting:
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
                self.redis.set( "robot.airRights", "Requested")
                qr = self.db.query( qs)
                rslt = qr.dictresult()[0]["rslt"]
                if rslt:
                    self.haveAirRights = True
                    self.pushCmd( "vdi90on", datetime.datetime.now())
                    self.redis.set( "robot.airRights", True)
                    print "received haveAirRights and setting vdi90on"

            if runFlag and not self.needAirRights and self.haveAirRights:
                # Drop air rights and notify thru postgres that the sample is
                # ready.
                self.redis.set( "robot.airRights", "Returning")
                self.db.query( "select px.dropRobotAirRights()")
                self.pushCmd( "vdi90off")
                self.haveAirRights = False
                self.redis.set( "robot.airRights", False)
                print "dropped Air Rights and setting vdi90off"

        self.close()

    def statusStateParse( self, s):
        self.srqst["state"]["rcvdCnt"] = self.srqst["state"]["rcvdCnt"] + 1

        # One line command to an argument list
        a = s[s.find("(")+1 : s.find(")")].split(',')

        #
        # state ask for the sample changer status -
        #                                          0 = power (1 or 0)
        #                                          1 = auto mode status (1 or 0)
        #                                          2 = default status (1 or 0)
        #                                          3 = tool number or name
        #                                          4 = path name
        #                                          5 = lid number of sample mounted on tool
        #                                          6 = number of the sample on tool
        #                                          7 = lid number of sample mounted on diffractometer
        #                                          8 = number of sample mounted on diffractometer
        #                                          9 = number of plate in tool
        #                                          10 = well number
        #                                          11 = barcode number
        #                                          12 = path running (1 or 0)
        #                                          13 = LN2 regulation running Dewar#1(1 or 0)
        #                                          14 = LN2 regulation running Dewar #2(1 or 0)
        #                                          15 = robot speed ratio (%)
        #                                          16 = puck detection result on Dewar#1
        #                                          17 = puck detection result on Dewar#2
        #                                          18 = position number in Dewar#1
        #                                          19 = position number in Dewar#2


        if len(a) != 20:
            print s
            raise CatsOkError( 'Wrong number of arguments received in status state response: got %d, exptected 20' % (len(a)))
        #                            0            1            2             3           4          5   6   7   8   9  10   11         12           13           14

        if self.statusStateLast == None or self.statusStateLast != s:
            b = []
            i = 0
            #          power       auto stat    default    tool    path     lid#    sam#   mlid#    msam#    plt#   well#   barcode   running     reg1 (1)    reg2 (2)     speed    detc1     detc2  pos#1     pos#2
            #             0           1           2         3       4         5      6       7       8        9     10        11        12          13         14           15        16       17      18       19
            aType = ["::boolean","::boolean","::boolean","::text","::text","::int","::int","::int","::int","::int","::int","::text","::boolean","::boolean","::boolean","::float", "::int", "::int", "::int", "::int"]
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

            self.redis.set( "robot.power",          a[ 0])
            self.redis.set( "robot.autoModeStatus", a[ 1])
            self.redis.set( "robot.defaultStatus",  a[ 2])
            self.redis.set( "robot.tool",           a[ 3])
            self.redis.set( "robot.path",           a[ 4])
            self.redis.set( "robot.lidTooled",      a[ 5])
            self.redis.set( "robot.sampleTooled",   a[ 6])
            self.redis.set( "robot.lidMounted",     a[ 7])
            self.redis.set( "robot.sampleMounted",  a[ 8])
            self.redis.set( "robot.platedTooled",   a[ 9])
            self.redis.set( "robot.well",           a[10])
            self.redis.set( "robot.barcode",        a[11])
            self.redis.set( "robot.running",        a[12])
            self.redis.set( "robot.ln2reg1",        a[13])
            self.redis.set( "robot.ln2reg2",        a[14])
            self.redis.set( "robot.speed",          a[15])
            self.redis.set( "robot.dewar1pucks",    a[16])
            self.redis.set( "robot.dewar2pucks",    a[17])
            self.redis.set( "robot.dewar1pos",      a[18])
            self.redis.set( "robot.dewar2pos",      a[19])
            

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


        pathName = a[4]
        self.workingPath = pathName

        # mark the end of a path
        if len(self.lastPathName)>0 and self.lastPathName != pathName:
            self.db.query( "select cats.cmdTimingDone()")

        # Nab air rights when we embark on a path requiring them
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

        #
        # Give up air rights when not moving and not in the exclusion zone
        #
        if self.workingPath == "" and not self.inExclusionZone and self.haveAirRights:
            self.needAirRights = False


        #
        # set the cap detector flag
        #
        if self.redis.get( "capDetected") == "1":
            if not self.capDetected:
                self.capDetected = True
                self.pushCmd( "vdi92on")
        else:
            if self.capDetected:
                self.capDetected = False
                self.pushCmd( "vdi92off")

        #
        # Check if the magnet state makes sense. The smart magnet functionality
        # was removed long ago, make sure the magnet is disabled. 
        #
        if dismountSampleCheckDelaySeconds != None and self.checkMountedSample and pathName != 'get' and (datetime.datetime.now() - self.sampleMounted["timestamp"] > datetime.timedelta(0,dismountSampleCheckDelaySeconds)):
            self.checkMountedSample = False
            if self.redis.get( "capDetected") == "1":
                ms = True
            else:
                ms = False
            
            # Check if the robot and the MD2 (or the camera cap detector) agree
            if ms and (self.sampleMounted["lid"] == "" or self.sampleMounted["sample"] == ""):
                print "It looks to me like there is a sample on diffractometer but robot thinks there isn't one"

                self.pushCmd( "panic")
                if not self.inRecoveryMode:
                    self.inRecoveryMode = True
                else:
                    self.needAirRights = False
                    self.inRecoveryMode = False

    def statusDoParse( self, s):
        self.srqst["do"]["rcvdCnt"] = self.srqst["do"]["rcvdCnt"] + 1
        do = s[s.find("(")+1:s.find(")")]
        if do[0] != "1" and do[0] != "0":
            print "Bad 'do' returned: %s" % (do)
            return

        # Calculate Pr2 (robot air rights request)
        lastPr2 = self.Pr2
        self.Pr2 = do[5] == "1"

        # robot needs airrights on rising edge of Pr2
        if not lastPr2 and self.Pr2:
            print "Robot needs air rights now"
            if self.cryoLocked:
                print "Unlocking Cryo..."
                self.db.query( "select px.unlockCryo()")
                self.cryoLocked = False
                self.redis.set( "robot.cryoLocked", False)

            self.db.query( "select cats.cmdTimingNeedAir()")

        # robot no longer needs airrights on falling edge of Pr2
        if lastPr2 and not self.Pr2:
            print "Robot no longer needs air rights"
            self.needAirRights = False
            self.db.query( "select cats.cmdTimingNoAir()")
            if not self.cryoLocked:
                print "Locking Cryo..."
                self.db.query( "select px.lockCryo()")
                self.cryoLocked = True
                self.redis.set( "robot.cryoLocked", True)


    def statusDiParse( self, s):
        self.srqst["di"]["rcvdCnt"] = self.srqst["di"]["rcvdCnt"] + 1
        di = s[s.find("(")+1:s.find(")")]

        self.diES  = di[1] == "1"
        self.redis.set( "robot.emergencyStop", self.diES)

        self.collision = di[2] == "1"
        self.redis.set( "robot.collision", self.collision)

        self.cryoHighAlarm = di[3] == "1"
        self.redis.set( "robot.cryoHighAlarm", self.cryoHighAlarm)

        self.cryoHigh = di[4] == "1"
        self.redis.set( "robot.cryoHigh", self.cryoHigh)

        self.cryoLow = di[5] == "1"
        self.redis.set( "robot.cryoLow", self.cryoLow)

        self.cryoLowAlarm = di[6] == "1"
        self.redis.set( "robot.cryoLowAlarm", self.cryoLowAlarm)

        self.lid1Open = di[21] == "1"
        self.redis.set( "robot.lid1Open", self.lid1Open)

        self.lid2Open = di[22] == "1"
        self.redis.set( "robot.lid2Open", self.lid2Open)

        self.lid3Open = di[23] == "1"
        self.redis.set( "robot.lid3Open", self.lid3Open)

        self.toolOpen = di[24] == "1"
        self.redis.set( "robot.toolOpen", self.toolOpen)

        self.toolClosed = di[25] == "1"
        self.redis.set( "robot.toolClosed", self.toolOpen)

        tmp  = 0
        tmp2 = 1
        for i in range( 12, 21):
            if di[i] == "1":
                tmp += tmp2
            tmp2 *= 2

        self.pucksDetected = tmp
        self.redis.set( "robot.pucksDetected", self.pucksDetected);

    def statusPositionParse( self, s):
        # Note: we do not call cats.setposition() in postgres, presumably
        # because we read the actual position from the robot itself in real
        # time.
        self.srqst["position"]["rcvdCnt"] = self.srqst["position"]["rcvdCnt"] + 1
        if self.statusPositionLast != None and self.statusPositionLast == s:
            return

        # One line command to an argument list
        a = s[s.find("(")+1 : s.find(")")].split(',')

        if len(a) != 6:
            raise CatsOkError( 'Wrong number of arguments received in status state response: got %d, exptected 14' % (len(a)))
        self.statusPositionLast = s

        #
        # See if we are in the 300mm exclusion zone
        #
        x = float(a[0])
        y = float(a[1])
        z = float(a[2])
        if ((x-self.dpX)*(x-self.dpX)+(y-self.dpY)*(y-self.dpY)+(z-self.dpZ)*(z-self.dpZ)) < 9E4:
            #
            # In exclusion zone
            #
            if not self.inExclusionZone:
                print "In Exclusion zone"
                self.redis.set( "robot.inExclusionZone", True)
                # we are in the exclusion zone but thought that we were not
                #
                if not self.haveAirRights:
                    # Opps, we don't have air rights.  This is bad.
                    #
                    print "Aborting: need air rights but don't have them"
                    self.pushCmd("panic")
                    self.db.query( "select px.pusherror( 39201,'')")   # post error message
                    self.needAirRights = True
                else:
                    # We have the right to be here.  Just make a note
                    self.db.query( "select px.pusherror( 39200,'')")
                # set the flag
                self.inExclusionZone = True

            #
            # Tell the robot we are in the exclusion zone
            #
            if self.catsExclusionZone == None or self.catsExclusionZone != self.inExclusionZone:
                self.pushCmd( "vdi91on")
                self.catsExclusionZone = True
        else:
            #
            # We are NOT in the exclusion zone
            if self.inExclusionZone:
                print "Not in exclusion zone"
                self.redis.set( "robot.inExclusionZone", False)
                # but we thought we were
                self.db.query( "select px.pusherror( 30200,'')")
                self.inExclusionZone = False

            if self.haveAirRights:
                # We have air rights, but do we need them anymore?
                #
                if self.workingPath == "":
                    # we are not on a path, perhaps we've aborted?
                    #
                    self.needAirRights = False
                    
            #
            # Tell the robot we are not in the exclusion zone
            #
            if self.catsExclusionZone == None or self.catsExclusionZone != self.inExclusionZone:
                self.pushCmd( "vdi91off")
                self.catsExclusionZone = False

    def statusConfigParse( self, s):
        pass
    
    def statusMessageParse( self, s):
        self.srqst["message"]["rcvdCnt"] = self.srqst["message"]["rcvdCnt"] + 1
        if self.statusMessageLast != None and self.statusMessageLast == s:
            return

        self.statusMessageLast = s

        

#
# Default usage
#
if __name__ == '__main__':
    iFlag = True
    rFlag = True

    while rFlag:
        while iFlag:
            print "Starting Up..."
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

