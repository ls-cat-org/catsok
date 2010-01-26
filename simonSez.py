#! /usr/bin/python
#
# States enumerated here:
# https://spreadsheets.google.com/a/ls-cat.org/ccc?key=0AlcMs9J61ndgdHoyQnJfRmFidm1qWWtka0lzR1d5emc&hl=en
#

import sys, os, select, pg, time, traceback, datetime, socket



class SimonError( Exception):
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



class smState:
    sId     = None      # unique identifier for this state
    sValue  = None      # value of this state (getState & mask)
    sMask   = None      # mask used to find value
    sLast   = None      # id's of allowed previous states
    sDesc   = None      # brief description of this state
    _q      = None      # shared datebase connection

    def __init__( self, _q, sId, sValue, sMask, sLast, sDesc):
        self._q     = _q
        self.sId    = sId
        self.sValue = sValue
        self.sMask  = sMask
        self.sLast  = sLast
        self.sDesc  = sDesc

    def __str__( self):
        return self.sDesc

    def here( self, lastId, currentState):
        #
        if (self.sMask & currentState) != self.sValue:
            # regardless of the last state, the current state does not match
            return 0
        #
        #  Here on down we only need to check the last state
        #
        if self.sLast == None or lastId == 0:
            # Oh, we don't care about the last state
            return self.sId
        #
        # look for a match we the last state
        rtn = 0
        try:
            if self.sLast.index(lastId):
                # got it
                rtn = self.sId
        except ValueError:
            # I guess we didn't find it, return false
            pass

        # Should never get here
        return rtn
    #raise SimonError( "here: id=%d, got to where we shouldn't have.  lastId=%d, currentState=%d " % (self.sId, lastId, currentState))
    
                

class smSeq:
    qReady = None       # state of readiness
    qList = []          # list of states in this sequence
    timeZero = None     # time that we first noticed we're ready
    _q       = None     # shared database connection

    #
    # returns the id of the ready state or 0 if not ready
    def isReady( self, currentState):
        rtn = self.qReady.here( None, currentState)
        if rtn != 0:
            if self.timeZero == None:
                self.timeZero = datetime.datetime.now()
        else:
            self.timeZero = None
                
        return rtn


    def action( self, delay, stn, smpl):
        if self.timeZero != None:
            if datetime.datetime.now() >= self.timeZero + delay:
                self.timeZero = None
                qs = "select px.requestTransfer( %d, %d)" % (stn, smpl)
                self._q.query( qs)
                return True
            else:
                return False
        return False

    #
    # returns either the currently running id or 0 if not running
    def isRunning( self, lastId, currentState):
        for l in self.qList:
            v = l.here( lastId, currentState)
            if v != 0:
                return v
        return 0


    def __init__( self, _q, stateDict):
        self._q = _q
        r = stateDict["ready"]
        self.qReady = smState( self._q, r["id"], r["value"], r["mask"], r["last"], r["desc"])

        for r in stateDict["running"]:
            self.qList.append( smState( self._q, r["id"], r["value"], r["mask"], r["last"], r["desc"]))





class Simon:
    #
    # 1 = Diffractometer On
    # 2 = Air Rights Taken
    # 3 = Detector Ready
    # 4 = Exposing
    # 5 = Detector On
    # 6 = Cryo Locked
    #

    actions = {
        1 : [],
        2 : [],
        3 : [
        0x03030101,0x03030102,0x03030103,0x03030104,0x03030105,0x03030106,0x03030107,0x03030108,0x03030109,0x0303010a,
        0x03030201,0x03030202,0x03030203,0x03030204,0x03030205,0x03030206,0x03030207,0x03030208,0x03030209,0x0303020a,
        0x03030301,0x03030302,0x03030303,0x03030304,0x03030305,0x03030306,0x03030307,0x03030308,0x03030309,0x0303030a,

        0x03040101,0x03040102,0x03040103,0x03040104,0x03040105,0x03040106,0x03040107,0x03040108,0x03040109,0x0304010a,
        0x03040201,0x03040202,0x03040203,0x03040204,0x03040205,0x03040206,0x03040207,0x03040208,0x03040209,0x0304020a,
        0x03040301,0x03040302,0x03040303,0x03040304,0x03040305,0x03040306,0x03040307,0x03040308,0x03040309,0x0304030a,

        0x03050401,0x03050402,0x03050403,0x03050404,0x03050405,0x03050406,0x03050407,0x03050408,0x03050409,0x0305040a,0x0305040b,0x0305040c,
        # 0x03050501,0x03050502,0x03050503,0x03050504,0x03050505,0x03050506,0x03050507,0x03050508,0x03050509,0x0305050a,0x0305050b,0x0305050c,
        0x03050601,0x03050602,0x03050603,0x03050604,0x03050605,0x03050606,0x03050607,0x03050608,0x03050609,0x0305060a,0x0305060b,0x0305060c,
        0
        ],
        4 : []
        }

    dataCollectionMask =  95    # & with sate
    simpleTransferMask = 169
    _q       = None             # query object
    states   = {}               # state dictionary
    stations = [1,2,3,4]        # stations
    dataCollection = {}
    dataCollectionInit = {
        "ready"   : { "id" : 500, "value" : 87, "mask" : 95,  "last" : None, "desc" : "Idle"},
        "running" : [
            { "id" : 510, "value" : 95, "mask" : 95, "last" : [500,510],         "desc" : "Start Integration"},
            { "id" : 520, "value" : 91, "mask" : 95, "last" : [500,510,520],     "desc" : "Exposing"},
            { "id" : 530, "value" : 83, "mask" : 95, "last" : [500,510,520,530], "desc" : "Reading"}
            ]
        }

    transfer = {}
    transferInit = {
        "ready"   : { "id" : 200, "value" : 99, "mask" : 235, "last" : None,   "desc" : "Idle"},
        "running" : [
            { "id" : 210, "value" : 227, "mask" : 235, "last" : [200,210],         "desc" : "Path Running"},
            { "id" : 220, "value" : 195, "mask" : 235, "last" : [210,220],         "desc" : "Pulling Cryo Back (brief)"},
            { "id" : 230, "value" : 193, "mask" : 235, "last" : [210,220,230],     "desc" : "Air Rights Ready for Robot (brief)"},
            { "id" : 240, "value" : 131, "mask" : 235, "last" : [210,220,230,240], "desc" : "Transfering"},
            { "id" : 250, "value" : 193, "mask" : 235, "last" : [240,250],         "desc" : "Air Rights Ready for Diffractometer (brief)"},
            { "id" : 260, "value" : 227, "mask" : 235, "last" : [240,250,260],     "desc" : "Path Running"}
            ]
        }



    def getStates( self):
        qs = "select \"Station\", \"State\" from cats.machineState() as ms"
        qr = self._q.query( qs)
        for r in qr.dictresult():
            self.states[r["Station"]] = r["State"]
        
    def __init__( self):
        self._q = _Q()

        for s in self.stations:
            self.transfer[s] = smSeq( self._q, self.transferInit)
            self.dataCollection[s] = smSeq( self._q, self.dataCollectionInit)
            
        self.getStates()
        


    def run( self):
        lastId = {}
        for s in self.stations:
            lastId[s] = None

        while 1:
            self.getStates()
            for s in self.stations:
                st = self.states[s]
                txt = ""
                newId = 0
                dcr = 0
                tr = 0
                lid = lastId[s]

                if lid == None or lid == 0:
                    dcr = self.dataCollection[s].isReady( st)
                    if dcr != 0:
                        txt += " data collection ready"
                    tr  = self.transfer[s].isReady( st)
                    if tr != 0:
                        txt += " transfer ready"
                        try:
                            smpl = self.actions[s].pop(0)
                            if self.transfer[s].action( datetime.timedelta(0,10,0), s, smpl):
                                print "*****************   Action Started"
                            else:
                                self.actions[s].insert( 0, smpl)
                        except IndexError:
                            # Nothing to do, so don't
                            pass

                if dcr == 0 and tr == 0 and lid != None:

                    newId = self.dataCollection[s].isRunning( lid, st)
                    if newId != 0:
                        txt += " collecting data"
                    else:
                        lid = lastId[s]
                        if lid == 0:
                            lid = tr
                        newId  = self.transfer[s].isRunning( lid, st)
                        if newId != 0:
                            txt += " transfering"

                print "station %d: state=%d dcState=%d  tState=%d  newId=%d  %s" % (s, st, st&95, st&235, newId, txt)
                lastId[s] = newId

            time.sleep( 0.5)
            print " "
        


if __name__ == "__main__":
    ss = Simon()
    ss.run()
    
