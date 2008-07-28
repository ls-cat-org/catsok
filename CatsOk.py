#! /usr/bin/python
#


import sys, os, select, pg, time, traceback, datetime, socket, Bang

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


class CatsOk:
    """
    Monitors status of the cats robot, updates database, and controls CatsOk lock
    """

    termstr = "\r"      # character expected to terminate cats response
    db = None           # database connection
    dblock = None       # database connection for the CatsOk lock

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

    statusStateLast    = None
    statusIoLast       = None
    statusPositionLast = None
    statusMessageLast  = None
    waiting = False

    def dbService( self, event):
        while self.db.getnotify() != None:
            pass
        return True

    def dblockService( self, event):
        return True

    def t1Service( self, event):
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

    def t2Service( self, event):
        try:
            newStr = self.t2.recv( 4096)
        except socket.error:
            self.p.unregister( self.t2.fileno())
            return False

        if len(newStr) == 0:
            self.p.unregister( self.t2.fileno())
            return False
        self.waiting = False
        print "Status Received:", newStr

        str = self.t2Input + newStr
        pList = str.replace('\n','\r').split( self.termstr)
        if len( str) > 0 and str[-2:-1] != self.termstr:
            self.t2Input = pList.pop( -1)
        
        for s in pList:
            sFound = False
            for ss in self.sFan:
                if s.startswith( ss):
                    self.sFan[ss](s)
                    sFound = True
                    break
            if not sFound:
                # if we do not recognize the response then it must be a message
                self.statusMessageParse( s)

        return True

    def __init__( self):
        #
        # establish connections to CATS sockets
        self.t1 = socket.socket( socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.t1.connect( ("164.54.252.155", 1000))
        except socket.error:
            raise CatsOkError( "Could not connect to command port")
        self.t2 = socket.socket( socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.t2.connect( ("164.54.252.155", 10000))
        except socket.error:
            raise CatsOkError( "Could not connect to status port")
    
        #
        # establish connecitons to database server
        self.db       = pg.connect(dbname='ls',user='lsuser', host='contrabass.ls-cat.org')
        self.dblock   = pg.connect(dbname='lslocks',user='lsuser', host='contrabass.ls-cat.org')

        #
        # Set up poll object
        self.p = select.poll()
        self.p.register( self.t1.fileno(), select.POLLIN)
        self.p.register( self.t2.fileno(), select.POLLIN)
        self.p.register( self.db.fileno(), select.POLLIN)
        self.p.register( self.dblock.fileno(), select.POLLIN)

        #
        # Set up fan to unmultiplex the poll response
        self.fan = {
            self.t1.fileno()     : self.t1Service,
            self.t2.fileno()     : self.t2Service,
            self.db.fileno()     : self.dbService,
            self.dblock.fileno() : self.dblockService
            }

        #
        # Set up sFan to handle status socket messages
        self.sFan = {
            "state"    : self.statusStateParse,
            "io"       : self.statusIoParse,
            "position" : self.statusPositionParse,
            "config"   : self.statusConfigParse
            }

        self.srqst = {
            "state"     : { "period" : 30.0, "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "io"        : { "period" : 30.0, "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "position"  : { "period" : 30.0, "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0},
            "message"   : { "period" : 30.0, "last" : None, "rqstCnt" : 0, "rcvdCnt" : 0}
            }

        self.MD2 = Bang.Sphere( Bang.Point( 0.5, 0.5, 0.5), 0.5)

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
        if self.dblock != None:
            self.dblock.close()
            self.dblock = None

    def run( self):
        runFlag = True
        while( runFlag):
            for ( fd, event) in self.p.poll( 100):
                runFlag = runFlag and self.fan[fd](event)
                if not runFlag:
                    break
            if runFlag and not self.waiting:
                #
                # create status requests
                for k in self.srqst.keys():
                    n = datetime.datetime.now()
                    r = self.srqst[k]
                    if r["last"] == None or (n - r["last"] > datetime.timedelta(0,r["period"])):
                        print k, self.t2.send( k + "\r")
                        self.waiting = True
                        r["last"] = datetime.datetime.now()
                        r["rqstCnt"] = r["rqstCnt"] + 1
                        break
        self.close()

    def statusStateParse( self, s):
        self.srqst["state"]["rcvdCnt"] = self.srqst["state"]["rcvdCnt"] + 1
        if self.statusStateLast != None and self.statusStateLast == s:
            self.db.query( "select cats.setstate()")
            return

        # One line command to an argument list
        a = s.partition( '(')[2].partition( ')')[0].split(',')
        if len(a) != 15:
            print s
            raise CatsOkError( 'Wrong number of arguments received in status state response: got %d, exptected 15' % (len(a)))
        #                            0            1            2             3           4          5   6   7   8   9  10   11         12           13           14
        b = []
        i = 0
        #             0           1           2         3       4         5      6       7       8        9     10        11        12          13         14
        aType = ["::boolean","::boolean","::boolean","::text","::text","::int","::int","::int","::int","::int","::int","::text","::boolean","::boolean","::boolean"]
        qs = "select cats.setstate( "
        print a
        needComma = False
        for zz in a:
            if len(zz) == 0:
                b.append("NULL")
            else:
                b.append( "'%s'%s" % (zz, aType[i]))

            if needComma:
                qs = qs+","
            qs = qs + b[i]
            i = i+1
            needComma = True

        qs = qs + ")"
        #qs = "select cats.setstate( '%s'::boolean, '%s'::boolean, '%s'::boolean, '%s'::text, '%s'::text, '%s', '%s', '%s', '%s', '%s', '%s', '%s'::text, '%s'::boolean, '%s'::boolean, '%s'::boolean)" % \
        #( a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10], a[11], a[12], a[13], a[14])
        self.db.query( qs)
        self.statusStateLast = s
        

    def statusIoParse( self, s):
        self.srqst["io"]["rcvdCnt"] = self.srqst["io"]["rcvdCnt"] + 1
        if self.statusIoLast != None and self.statusIoLast == s:
            self.db.query( "select cats.setio()")
            return

        # One line command to pull out the arguments as one string
        # hope this is in the right format to send to postresql server
        a = s.partition( '(')[2].partition( ')')[0]
        b = a.replace( "1", "'t'")
        c = b.replace( "0", "'f'")

        qs = "select cats.setio( %s)" % (c)
        print s,qs
        self.db.query( qs)
        self.statusIoLast = s
        

    def statusPositionParse( self, s):
        self.srqst["position"]["rcvdCnt"] = self.srqst["position"]["rcvdCnt"] + 1
        if self.statusPositionLast != None and self.statusPositionLast == s:
            self.db.query( "select cats.setposition()")
            return

        # One line command to an argument list
        a = s.partition( '(')[2].partition( ')')[0].split(',')
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
            self.db.query( "select cats.setmessage()")
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
        except CatsOkError, e:
            z.close()
        time.sleep( 10)
        iFlag = True

