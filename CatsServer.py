#! /usr/bin/python

import sys, os, select, pg, time, traceback, datetime, socket, math

class CatsServer:
    """
    Basic server to test cats client functionality
    """

    skt1 = None
    skt2 = None
    cmdSkt = None
    statusSkt = None
    p    = None
    statusList = {}

    pUpdateTime = None
    x = 0.0
    y = 0.0
    z = 0.0
    rx = 0.0
    ry = 0.0
    rz = 0.0

    ios = []
    states = []

    def statusStateService( self):
        rtn = "state("
        needComma = False
        for s in self.states:
            if needComma:
                rtn = rtn+","
            rtn = rtn + "%s" % (s)
            needComma = True
        rtn = rtn + ")"
        return rtn

    def statusIoService( self):
        rtn = "io("
        needComma = False
        for s in self.ios:
            if needComma:
                rtn = rtn+","
            rtn = rtn + "%d" %(s)
            needComma = True
        rtn = rtn + ")"
        return rtn


    def statusPositionService( self):
        n = datetime.datetime.now()
        if self.pUpdateTime != None:
            d = n - self.pUpdateTime
            dt = d.days*24.*3600. + d.seconds + d.microseconds/1000000.

            self.x = 1000 * math.sin( dt)
            self.y = 1000 * math.sin( dt)
            self.z = 1000 * math.sin( dt)
            
        self.pUpdateTime = n



        return( "position(%.6f,%.6f,%.6f,%.6f,%.6f,%.6f)") % (self.x, self.y, self.z, self.rx, self.ry, self.rz)


    def statusMessageService( self):
        return "Here I am"

    def statusConfigService( self):
        return "config"

    def __init__(self):
        self.skt1 = socket.socket( socket.AF_INET, socket.SOCK_STREAM)
        self.skt1.bind( ("localhost", 1000))
        self.skt1.listen( 1)

        self.skt2 = socket.socket( socket.AF_INET, socket.SOCK_STREAM)
        self.skt2.bind( ("localhost", 10000))
        self.skt2.listen( 2)


        self.p = select.poll()
        self.p.register( self.skt1.fileno(), select.POLLIN)
        self.p.register( self.skt2.fileno(), select.POLLIN)

        self.statusList = {
            "state" : self.statusStateService,
            "io"    : self.statusIoService,
            "position" : self.statusPositionService,
            "message"  : self.statusMessageService,
            "config"   : self.statusConfigService
            }

        for i in range( 48):
            self.ios.append( 0)

        for i in range( 15):
            self.states.append( 0)

    def run( self):
        while( True):
            for (fd, event) in self.p.poll():
                if self.skt1 != None and fd == self.skt1.fileno():
                    self.getCommandSocket()
                if self.skt2 != None and fd == self.skt2.fileno():
                    self.getStatusSocket()

                if self.cmdSkt != None and fd == self.cmdSkt.fileno():
                    self.serviceCommand()

                if self.statusSkt != None and fd == self.statusSkt.fileno():
                    self.serviceStatus()


    def getCommandSocket( self):
        print "accepting command connection"
        self.cmdSkt, addr = self.skt1.accept()
        self.p.register( self.cmdSkt.fileno(), select.POLLIN)

    def getStatusSocket( self):
        print "accepting status connection"
        self.statusSkt, addr = self.skt2.accept()
        self.p.register( self.statusSkt.fileno(), select.POLLIN)


    def serviceCommand( self):
        pass

    def serviceStatus( self):
        s = self.statusSkt.recv( 4096)
        if len(s) == 0:
            self.p.unregister( self.statusSkt)
            return

        print "serviceStatus: ", s
        
        for t in s.split():
            if len(t) > 0:
                print "Status Request: '%s'\n" % (t)
                r = self.statusList[t.strip()]()
                self.statusSkt.send( r + "\n")

            

if __name__ == "__main__":
    zz = CatsServer()
    zz.run()

