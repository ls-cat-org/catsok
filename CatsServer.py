#! /usr/bin/python

import sys, os, select, pg, time, traceback, datetime, socket, math, copy, EpicsCA

class CatsServer:
    """
    Basic server to test cats client functionality
    """

    MagnetRelayPVName = '21:F1:pmac10:acc65e:1:bo1'

    vdi = [0,0,0,0,0,0,0,0,0,0]
    stateList = [
        "power",
        "autoMode",
        "defaultStatus",
        "toolNumber",
        "pathName",
        "lidNumberOnTool",
        "sampleNumberOnTool",
        "lidNumberMounted",
        "sampleNumberMounted",
        "plateNumber",
        "wellNumber",
        "barcode",
        "pathRunning",
        "LN2Reg",
        "LN2Warming"
        ]

    ioList = [
        "ioCryoOK"      ,        # 00 Cryogen Sensors OK:  1 = OK
        "ioESAP"        ,        # 01 Emergency Stop and Air Pressure OK: 1 = OK
        "ioCollisionOK" ,        # 02 Collision Sensor OK: 1 = No Collision
        "ioCryoHighAlarm" ,      # 03 Cryogen High Level Alarm: 1 = No Alarm
        "ioCryoHigh"    ,        # 04 Cryogen High Level: 1 = high level reached
        "ioCryoLow"     ,        # 05 Cryogen Low Level: 1 = low level reached
        "ioCryoLowAlarm" ,       # 06 Cryogen Low Level Alarm: 1 = no alarm
        "ioCryoLiquid"  ,        # 07 Cryogen Liquid Detection: 0=Gas, 1=Liquid
        "ioInput1"      ,        # 08
        "ioInput2"      ,        # 09
        "ioInput3"      ,        # 10
        "ioInput4"      ,        # 11
        "ioCassette1"   ,        # 12 Cassette 1 presence:  1 = cassette in place
        "ioCassette2"   ,        # 13 Cassette 2 presence:  1 = cassette in place
        "ioCassette3"   ,        # 14 Cassette 3 presence:  1 = cassette in place
        "ioCassette4"   ,        # 15 Cassette 4 presence:  1 = cassette in place
        "ioCassette5"   ,        # 16 Cassette 5 presence:  1 = cassette in place
        "ioCassette6"   ,        # 17 Cassette 6 presence:  1 = cassette in place
        "ioCassette7"   ,        # 18 Cassette 7 presence:  1 = cassette in place
        "ioCassette8"   ,        # 19 Cassette 8 presence:  1 = cassette in place
        "ioCassette9"   ,        # 20 Cassette 9 presence:  1 = cassette in place
        "ioLid1Open"    ,        # 21 Lid 1 Opened: 1 = lid completely opened
        "ioLid2Open"    ,        # 22 Lid 2 Opened: 1 = lid completely opened
        "ioLid3Open"    ,        # 23 Lid 3 Opened: 1 = lid completely opened
        "ioToolOpened"  ,        # 24 Tool Opened: 1 = tool completely opened
        "ioToolClosed"  ,        # 25 Tool Closed: 1 = tool completely closed
        "ioLimit1"      ,        # 26 Limit Switch 1:  0 = gripper in diffractometer position
        "ioLimit2"      ,        # 27 Limit Switch 2:  0 = gripper in dewar position
        "ioTool"        ,        # 28 Tool Changer:	0 = tool gripped, 1 = tool released
        "ioToolOC"      ,        # 29 Tool Open/Close:  0 = tool closed, 1 = tool opened
        "ioFast"        ,        # 30 Fast Output: 0 = contact open, 1 = contact closed
        "ioUnlabed1"    ,        # 31 Usage not documented
        "ioMagnet"      ,        # 32 Output Process Information 2 [sic], Magnet ON: 0 = contact open, 1 = contact closed
        "ioOutput2"     ,        # 33 Output Process Information 2: 0 = contact open, 1 = contact closed
        "ioOutput3"     ,        # 34 Output Process Information 3: 0 = contact open, 1 = contact closed
        "ioOutput4"     ,        # 35 Output Process Information 4: 0 = contact open, 1 = contact closed
        "ioGreen"       ,        # 36 Green Light (Power, air, and Modbus network OK): 1 = OK
        "ioPILZ"        ,        # 37 PILZ Relay Reset: 1 = reset of the relay
        "ioServoOn"     ,        # 38 Servo Card ON: 1 = card on
        "ioServoRot"    ,        # 39 Servo Card Rotation +/-: 0 = toard the diffractometer, 1 = toward the dewar
        "ioLN2C"        ,        # 40 Cryogen Valve LN2_C: 0 = closed, 1 = open
        "ioLN2E"        ,        # 41 Cryogen Valve LN2_E: 0 = closed, 1 = open
        "ioLG2E"        ,        # 42 Cryogen Valve GN2_E: 0 = closed, 1 = open
        "ioHeater"      ,        # 43 Heater ON/ODD: 0 = off, 1 = on
        "ioUnlabed2"    ,        # 44 Usage not documented
        "ioUnlabed3"    ,        # 45 Usage not documented
        "ioUnlabed4"    ,        # 46 Usage not documented
        "ioUnlabed5"             # 47 Usage not documented
        ]
    cmds = {}

    program = []        # robot program we are running
    pgmPtr  = -1        # pointer to current line of program


    fan = {}
    skt1 = None
    skt2 = None
    p    = None
    statusList = {}
    cmdBuff = {}
    acceptNewProgramCmds = True
    cmdQueue = []

    x = 0.0
    y = 0.0
    z = 0.0
    rx = 0.0
    ry = 0.0
    rz = 0.0

    ios = {}
    states = {}
    tools = ["None","SPINE","Plate","Rigaku", "ALS", "UNI"]

    argNames = [ "Cap", "Lid", "Sample", "NewLid", "NewSample", "XtalPlate", "Well", "Arg7", "Arg8", "Arg9", "XShift", "YShift", "ZShift", "Angle", "Oscs", "Exp", "Step", "Final", "Arg18", "Arg19"]

    args = { "Cap"       : 0,
             "Lid"       : 0,
             "Sample"    : 0,
             "NewLid"    : 0,
             "NewSample" : 0,
             "XtalPlate" : 0,
             "Well"      : 0,
             "Arg7"      : 0,
             "Arg8"      : 0,
             "Arg9"      : 0,
             "XShift"    : 0,
             "YShift"    : 0, 
             "ZShift"    : 0,
             "Angle"     : 0,
             "Oscs"      : 0,
             "Exp"       : 0,
             "Step"      : 0,
             "Final"     : 0,
             "Arg18"     : 0,
             "Arg19"     : 0
             }


    def statusStateService( self):
        rtn = "state("
        needComma = False
        for n in self.stateList:
            s = self.states[n]
            if needComma:
                rtn += ","
            else:
                needComma = True
            if s != None:
                rtn += "%s" % (s)
        rtn = rtn + ")"
        return rtn

    def statusIoService( self):
        rtn = "io("
        needComma = False
        for n in self.ioList:
            s = self.ios[ n]
            if needComma:
                rtn += ","
            else:
                needComma = True
            rtn += "%d" % (s)
        rtn = rtn + ")"
        return rtn


    def statusPositionService( self):
        return( "position(%.6f,%.6f,%.6f,%.6f,%.6f,%.6f)") % (self.x, self.y, self.z, self.rx, self.ry, self.rz)


    def statusMessageService( self):
        return "Here I am"

    def statusConfigService( self):
        return "config"

    def pause( self):
        print "In Pause"
        pass

    def openLid( self):
        if int(self.args["Lid"]) == None:
            l = 0
        else:
            l = int(self.args["Lid"])
        if l == 1:
            self.ios["ioLid1Open"] = 1
        elif l == 2:
            self.ios["ioLid2Open"] = 1
        elif l == 3:
            self.ios["ioLid3Open"] = 1

    def openMountedLid( self):
        if self.states["lidNumberOnTool"] == None:
            l = 0
        else:
            l = int(self.states["lidNumberOnTool"])
        if l == 1:
            self.ios["ioLid1Open"] = 1
        elif l == 2:
            self.ios["ioLid2Open"] = 1
        elif l == 3:
            self.ios["ioLid3Open"] = 1

    def closeLids( self):
        self.ios["ioLid1Open"] = 0
        self.ios["ioLid2Open"] = 0
        self.ios["ioLid3Open"] = 0

    def getSampleFromDewar( self):
        self.states["toolNumber"] = self.tools[int(self.args["Cap"])]
        self.states["lidNumberOnTool"] = self.args["Lid"]
        self.states["sampleNumberOnTool"] = self.args["Sample"]
        self.states["pathName"] = "ToDewar"
        self.states["pathRunning"] = 1

    def putSampleInDewar( self):
        self.states["lidNumberOnTool"] = None
        self.states["sampleNumberOnTool"] = None
        self.states["pathName"] = "ToDewar"
        self.states["pathRunning"] = 1

    def getSampleFromMD2( self):
        self.states["toolNumber"] = self.tools[int(self.args["Cap"])]
        self.states["lidNumberOnTool"] = self.states["lidNumberMounted"]
        self.states["sampleNumberOnTool"] = self.states["sampleNumberMounted"]
        self.states["lidNumberMounted"] = None
        self.states["sampleNumberMounted"] = None
        self.states["pathName"] = "FromMD2"
        self.states["pathRunning"] = 1


    def putSampleOnMD2( self):
        self.states["lidNumberMounted"] = self.states["lidNumberOnTool"]
        self.states["sampleNumberMounted"] = self.states["sampleNumberOnTool"]
        self.states["lidNumberOnTool"] = None
        self.states["sampleNumberOnTool"] = None
        self.states["pathName"] = "ToMD2"
        self.states["pathRunning"] = 1

    def goHome( self):
        self.states["lidNumberOnTool"] = None
        self.states["sampleNumberOnTool"] = None
        self.states["pathName"] = None
        self.states["pathRunning"] = 0

    def vdion( self, cmd):
        i = int(cmd[4])
        self.vdi[i] = 1

    def vdioff( self, cmd):
        i = cmd[4]
        self.vdi[i] = 0

    def dewarToCheckPoint( self):
        if self.vdi[0] != 1:
            self.pgmPtr -= 1
        self.states["pathName"] = 'MD2CheckPointWaitingForAirRights'
        self.states["pathRunning"] = 1


    def homeToCheckPoint( self):
        if self.vdi[0] != 1:
            self.pgmPtr -= 1
        self.states["pathName"] = 'MD2CheckPointWaitingForAirRights'
        self.states["pathRunning"] = 1

    def MD2ToCheckPoint( self):
        self.states["pathName"] = 'checkPointDropAirRights'
        self.states["pathRunning"] = 1

    def strengthenMagnet( self):
        self.ios["ioMagnet"] = 1
        self.MagnetRelayPV.put( 1)

    def weakenMagnet( self):
        self.ios["ioMagnet"] = 0
        self.MagnetRelayPV.put( 0)

    def __init__(self):
        self.nowCmds = {
            "vdi90on" : self.vdion, "vdi91on" : self.vdion, "vdi92on" : self.vdion, "vdi93on" : self.vdion, "vdi94on" : self.vdion,
            "vdi95on" : self.vdion, "vdi96on" : self.vdion, "vdi97on" : self.vdion, "vdi98on" : self.vdion, "vdi99on" : self.vdion,
            "vdi90off" : self.vdioff, "vdi91off" : self.vdioff, "vdi92off" : self.vdioff, "vdi93off" : self.vdioff, "vdi94off" : self.vdioff,
            "vdi95off" : self.vdioff, "vdi96off" : self.vdioff, "vdi97off" : self.vdioff, "vdi98off" : self.vdioff, "vdi99off" : self.vdioff,
            }
        self.cmds = {
            'backup'         : [(10.0, self.pause)],
            'restore'        : [(10.0, self.pause)],
            'home'           : [(10.0, self.pause)],
            'safe'           : [(10.0, self.pause)],
            'reference'      : [(10.0, self.pause)],
            'put'            : [( 2.0, self.openLid),( 10.0, self.getSampleFromDewar),(0.1, self.dewarToCheckPoint),( 10.0, self.putSampleOnMD2),(2.0, self.weakenMagnet),(2.0,self.strengthenMagnet),(2.0, self.MD2ToCheckPoint),(0.1, self.closeLids), ( 5.0, self.goHome)],
            'put_bcrd'       : [(10.0, self.pause)],
            'get'            : [(0.1, self.homeToCheckPoint),( 10.0, self.getSampleFromMD2),(2.0, self.weakenMagnet),(2.0,self.strengthenMagnet),(0.1,self.openMountedLid),(2, self.MD2ToCheckPoint),( 10.0, self.putSampleInDewar),(5.0, self.goHome),(0.1, self.closeLids)],
            'get_bcrd'       : [(10.0, self.pause)],
            'getput'         : [(0.1, self.homeToCheckPoint),( 10.0, self.getSampleFromMD2),  (2.0, self.weakenMagnet),     (2.0,self.strengthenMagnet),(0.1,self.openMountedLid),
                                (2, self.MD2ToCheckPoint),   ( 10.0, self.putSampleInDewar),  (5.0, self.goHome),           (0.1, self.closeLids),
                                ( 2.0, self.openLid),        ( 10.0, self.getSampleFromDewar),(0.1, self.dewarToCheckPoint),( 10.0, self.putSampleOnMD2),
                                (2.0, self.weakenMagnet),    (2.0,self.strengthenMagnet),     (2.0, self.MD2ToCheckPoint),  (0.1, self.closeLids), ( 5.0, self.goHome)],
            'getput_bcrd'    : [(10.0, self.pause)],
            'barcode'        : [(10.0, self.pause)],
            'transfer'       : [(10.0, self.pause)],
            'soak'           : [(10.0, self.pause)],
            'dry'            : [(10.0, self.pause)],
            'putplate'       : [(10.0, self.pause)],
            'getplate'       : [(10.0, self.pause)],
            'getputplate'    : [(10.0, self.pause)],
            'goto_well'      : [(10.0, self.pause)],
            'adjust'         : [(10.0, self.pause)],
            'focus'          : [(10.0, self.pause)],
            'expose'         : [(10.0, self.pause)],
            'collect'        : [(10.0, self.pause)],
            'setplateangle'  : [(10.0, self.pause)]
            }
        skt1 = socket.socket( socket.AF_INET, socket.SOCK_STREAM)
        tryFlag = True
        i = 0
        while tryFlag:
            try:
                skt1.bind( ("", 10001))
                tryFlag = False
            except socket.error, (errno, strerr):
                if errno == 98:
                    i += 1
                    print "Waiting for command socket...%06d\r" % (i)
                    time.sleep( 5)
                else:
                    print "Can't get command socket"
                    sys.exit( 1)
            
        skt1.listen( 1)

        tryFlag = True
        i = 0
        while tryFlag:
            skt2 = socket.socket( socket.AF_INET, socket.SOCK_STREAM)
            try:
                skt2.bind( ("", 10000))
                tryFlag = False
            except socket.error, (errno, strerr):
                if errno == 98:
                    i += 1
                    print "Waiting for status socket...%06d\r" % (i)
                    time.sleep( 5)
        skt2.listen( 1)

        self.p = select.poll()
        self.p.register( skt1.fileno(), select.POLLIN | select.POLLPRI)
        self.p.register( skt2.fileno(), select.POLLIN | select.POLLPRI)

        self.fan = {
            skt1.fileno(): { "sock" : skt1, "sub" : self.acceptCmd },
            skt2.fileno(): { "sock" : skt2, "sub" : self.acceptStatus }
            }
            

        self.statusList = {
            "state" : self.statusStateService,
            "io"    : self.statusIoService,
            "position" : self.statusPositionService,
            "message"  : self.statusMessageService,
            "config"   : self.statusConfigService
            }

        # initialize IO List
        for n in self.ioList:
            self.ios[n] = 0

        self.ios["ioCryoOK"]       = 1
        self.ios["ioESAP"]         = 1
        self.ios["ioCollisionOK"]  = 1
        self.ios["ioLowAlarm"]     = 1
        self.ios["ioHighAlarm"]    = 1
        self.ios["ioLowAlarm"]     = 1
        self.ios["ioCassette1"]    = 1
        self.ios["ioCassette2"]    = 1
        self.ios["ioCassette3"]    = 1
        self.ios["ioCassette4"]    = 1
        self.ios["ioCassette5"]    = 1
        self.ios["ioCassette6"]    = 1
        self.ios["ioCassette7"]    = 1
        self.ios["ioCassette8"]    = 1
        self.ios["ioCassette9"]    = 1
        self.ios["ioLimit1"]       = 1
        self.ios["ioLimit2"]       = 1
        self.ios["ioToolOC"]       = 1
        self.ios["ioMagnet"]       = 1
        self.ios["ioGreen"]        = 1


        for n in self.stateList:
            self.states[n] = None

        self.states["power"]    = 1
        self.states["autoMode"] = 1
        self.states["toolNumber"] = "SPINE"

        self.MagnetRelayPV = EpicsCA.PV( self.MagnetRelayPVName)
        self.MagnetRelayPV.put( 1)
        

        print "\nReady for connections"

    def run( self):
        whileFlag = True
        timeLeft = -1   # negative time left means we wait for commands and status requests
        #
        while( whileFlag):
            theThen = datetime.datetime.now()

            # poll treats <0.001 as zero
            if timeLeft >0 and timeLeft < 0.001:
                timeLeft = 0.001

            for (fd, event) in self.p.poll( 1000 * timeLeft):
                # service status and parse commands
                print "fd: ", fd,"  event: ", event
                whileFlag = self.fan[fd]["sub"]( fd, event)

            theNow = datetime.datetime.now()

            # when there is time left we must be running the program
            print "timeLeft: ", timeLeft
            if timeLeft >= 0 and self.pgmPtr > -1:
                theDiff = theNow - theThen
                if theDiff < datetime.timedelta(0, timeLeft):
                    # not time yet...
                    timeLeft -= theDiff.days*24*3600 + theDiff.seconds + theDiff.microseconds/1000000.
                else:
                    # It's now time to run the next step
                    timeLeft = -1
                    if self.pgmPtr > -1:
                        if self.pgmPtr >= len( self.program):
                            print "Resetting Program Counter"
                            self.pgmPtr = -1
                        else:
                            print "Running next line: "
                            pgmLine = self.program[self.pgmPtr]
                            print "which is: ", pgmLine
                            timeLeft = pgmLine[0]
                            pgmLine[1]()
                            self.pgmPtr += 1

            if self.pgmPtr < 0:
                # We get here if we were not running a program
                # see if there is something in the command queue
                if len( self.cmdQueue) > 0:
                    cmdline = self.cmdQueue.pop(0)
                    print "cmdline: ", cmdline
                    cmd = None
                    if self.hasArgs( cmdline):
                        cmd = self.parseCmdArgs( cmdline)
                        print "args: ", self.args
                    else:
                        if self.cmds.has_key( cmdline):
                            cmd = cmdline
                    print "Running: ", cmd
                    if cmd != None:
                        self.program = copy.copy( self.cmds[cmd])
                        self.pgmPtr = 0
                        timeLeft = 0.001
                else:
                    # Nothing in the queue, see if we can get some more work
                    if not self.acceptNewProgramCmds:
                        self.acceptNewProgramCmds = True
            

    def acceptCmd( self, fd, event):
        skt, addr = self.fan[fd]["sock"].accept()
        print "accepting command connection ", skt.fileno()
        self.cmdBuff[skt.fileno()] = ""
        skt.setblocking( 1)
        self.p.register( skt.fileno(), select.POLLIN | select.POLLPRI)
        self.fan[skt.fileno()] = { "sock" : skt, "sub": self.cmdFan }
        return True

    def cmdFan( self, fd, event):
        if event & (select.POLLIN | select.POLLPRI):
            return self.cmdInput( fd, event)

        # ignore errors for now
        return True

    def hasArgs( self, cmdline):
        print "In hasArgs: ", cmdline
        rtn = False
        try:
            commaCount = cmdline.split("(")[1].split(")")[0].count(",")
            rtn = commaCount == 19
        except:
            pass
        return rtn

    def parseCmdArgs( self, cmdline):
        print "In parseCmdArgs: ", cmdline
        a = cmdline.split( "(")
        cmd = a[0]
        argList = a[1].replace(")","").split(",")
        i = 0
        self.prevArgs = copy.copy( self.args)
        for aname in self.argNames:
            self.args[aname] = argList[i]
            i += 1
        return cmd
        
    def acceptStatus( self, fd, event):
        skt, addr = self.fan[fd]["sock"].accept()
        print "accepting status connection ", skt.fileno()
        skt.setblocking( 1)
        self.p.register( skt.fileno(), select.POLLIN | select.POLLPRI)
        self.fan[skt.fileno()] = { "sock" : skt, "sub" : self.statusFan }
        return True


    def statusFan( self, fd, event):
        if event & (select.POLLIN | select.POLLPRI):
            return self.statusInput( fd, event)

        return True

    def closeSocket( self, fd, event):
        print "closing connection ", fd
        try:
            self.fan[fd]["sock"].close()
        except:
            pass

        if self.cmdBuff.has_key( fd):
            del self.cmdBuff[fd]

        self.p.unregister( fd)
        del self.fan[fd]
        return True

    def cmdInput( self, fd, event):
        s = self.fan[fd]["sock"].recv( 4096)
        if len(s) == 0:
            self.closeSocket( fd, select.POLLERR)
            return True

        # collect current input into the buff and break it up on the \r chars
        # if the last char was not \r then save that bit for later
        self.cmdBuff[fd] += s
        ba = self.cmdBuff[fd].split("\r")
        if self.cmdBuff[fd][-1] != "\r":
            # haven't got the entire last command yet, save it for later
            self.cmdBuff[fd] = ba.pop()
        else:
            # got everything, clear the buffer
            self.cmdBuff[fd] = ""

        
        # do the "now" commands, now
        for bael in ba:
            if self.nowCmds.has_key( bael):
                self.nowCmds[bael]( bael)

        # add what we got to the queue
        if self.acceptNewProgramCmds:
            for bael in ba:
                if len( bael) > 0:
                    self.cmdQueue.append( bael)
                    # Ignore all future commands untill we are done with this command
                    self.acceptNewProgramCmds = False
                    break

        print "cmdInput: ", s
        
        return True

    def statusInput( self, fd, event):
        try:
            s = self.fan[fd]["sock"].recv( 4096)
        except socket.error:
            self.closeSocket( fd, select.POLLERR)
            return True

        if len(s) == 0:
            self.closeSocket( fd, select.POLLERR)
            return True

        print "statusInput: ", s
        
        for u in s.split():
            t = u.strip()
            if len(t) > 0 and self.statusList.has_key(t):
                print "Status Request: '%s'\n" % (t)
                r = self.statusList[t]()
                self.fan[fd]["sock"].send( r + "\n")
        return True

            

if __name__ == "__main__":
    zz = CatsServer()
    zz.run()

