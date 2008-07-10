DROP SCHEMA cats cascade;
CREATE SCHEMA cats;
GRANT USAGE ON SCHEMA cats TO PUBLIC;

--
-- Status
--
create table cats.states (
       csKey serial primary key,
       csTSStart timestamp with time zone default now(),
       csTSLast  timestamp with time zone default now(),

       csPower boolean,
       csAutoMode boolean,
       csDefaultStatus boolean,
       csToolNumber text,
       csPathName text,
       csLidNumberOnTool integer,
       csSampleNumberOnTool integer,
       csLidNumberMounted integer,
       csSampleNumberMounted integer,
       csPlateNumber integer,
       csWellNumber integer,
       csBarcode text,
       csPathRunning boolean,
       csLN2Reg boolean,
       csLN2Warming boolean
);
ALTER TABLE cats.states OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION cats.statesInsertTF() returns trigger as $$
  DECLARE
    t RECORD;
  BEGIN
    SELECT * INTO t FROM cats.state ORDER BY csTSLast DESC LIMIT 1;
    IF FOUND THEN
      IF
        t.csPower = NEW.csPower AND
        t.csAutoMode = NEW.csAutoMode AND
        t.csDefaultStatus = NEW.csDefaultStatus AND
        t.csToolNumber = NEW.csToolNumber AND
        t.csPathName = NEW.csPathName AND
        t.csLidNumberOnTool = NEW.csLidNumberOnTool AND
        t.csSampleNumberOnTool = NEW.csSampleNumberOnTool AND
        t.csLidNumberMounted = NEW.csLidNumberMounted AND
        t.csSampleNumberMounted = NEW.csSampleNumberMounted AND
        t.csPlateNumber = NEW.csPlateNumber AND
        t.csWellNumber = NEW.csWellNumber AND
        t.csBarcode = NEW.csBarcode AND
        t.csPathRunning = NEW.csPathRunning AND
        t.csLN2Reg = NEW.csLN2Reg AND
        t.csLN2Warming = NEW.csLN2Warming
      THEN
        UPDATE cats.states SET csTSLast = now() WHERE csKey = t.csKey;
        RETURN NULL;
      END IF;
    END IF;
    RETURN NEW;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION cats.statesInsertTF() OWNER TO lsadmin;

CREATE TRIGGER statesInsertTrigger BEFORE INSERT ON cats.states FOR EACH ROW EXECUTE PROCEDURE cats.statesInsertTF();


CREATE OR REPLACE FUNCTION cats.setstate (
       power boolean,			--  0
       autoMode boolean,		--  1
       defaultStatus boolean,		--  2
       toolNumber text,			--  3
       pathName text,			--  4
       lidNumberOnTool integer,		--  5
       sampleNumberOnTool integer,	--  6
       lidNumberMounted integer,	--  7
       sampleNumberMounted integer,	--  8
       plateNumber integer,		--  9
       wellNumber integer,		-- 10
       barcode text,			-- 11
       pathRunning boolean,		-- 12
       LN2Reg boolean,			-- 13
       LN2Warming boolean		-- 14
) returns int as $$
BEGIN
  INSERT INTO cats.states ( csPower, csAutoMode, csDefaultStatus, csToolNumber, csPathName,
              csLidNumberOnTool, csSampleNumberOnTool, csLidNumberMounted, csSampleNumberMounted, csPlateNumber,
              csWellNumber, csBarcode, csPathRunning, csLN2Reg, csLN2Warming)
       VALUES (  power, autoMode, defaultStatus, toolNumber,  pathName,
                 lidNumberOnTool, sampleNumberOnTool, lidNumberMounted, sampleNumberMounted, plateNumber,
                 wellNumber, barcode, pathRunning, LN2Reg, LN2Warming);
  IF FOUND then
    return 1;
  else
    return 0;
  END IF;
  return 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION cats.setstate (
       power boolean,			--  0
       autoMode boolean,		--  1
       defaultStatus boolean,		--  2
       toolNumber text,			--  3
       pathName text,			--  4
       lidNumberOnTool integer,		--  5
       sampleNumberOnTool integer,	--  6
       lidNumberMounted integer,	--  7
       sampleNumberMounted integer,	--  8
       plateNumber integer,		--  9
       wellNumber integer,		-- 10
       barcode text,			-- 11
       pathRunning boolean,		-- 12
       LN2Reg boolean,			-- 13
       LN2Warming boolean		-- 14
) OWNER TO lsadmin;



CREATE OR REPLACE FUNCTION cats.setStateLast() returns void as $$
  DECLARE
    k bigint;
  BEGIN
    SELECT csKey INTO k FROM cats.states ORDER BY csTSLast DESC LIMIT 1;
    IF FOUND THEN
      UPDATE cats.states SET csTSLast = now() WHERE csKey = k;
    END IF;
    return;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION cats.setStateLast() OWNER TO lsadmin;


create table cats.io (
       ioKey serial primary key,
       ioTSStart timestamp with time zone default now(),
       ioTSLast  timestamp with time zone default now(),

       ioCryoOK boolean,	-- 00 Cryogen Sensors OK:  1 = OK
       ioESAP   boolean,	-- 01 Emergency Stop and Air Pressure OK: 1 = OK
       ioCollisionOK boolean,	-- 02 Collision Sensor OK: 1 = No Collision
       ioCryoHighAlarm boolean, -- 03 Cryogen High Level Alarm: 1 = No Alarm
       ioCryoHigh boolean,	-- 04 Cryogen High Level: 1 = high level reached
       ioCryoLow  boolean,	-- 05 Cryogen Low Level: 1 = low level reached
       ioCryoLowAlarm boolean,	-- 06 Cryogen Low Level Alarm: 1 = no alarm
       ioCryoLiquid boolean,	-- 07 Cryogen Liquid Detection: 0=Gas, 1=Liquid
       ioInput1 boolean,	-- 08
       ioInput2 boolean,	-- 09
       ioInput3 boolean,	-- 10
       ioInput4 boolean,	-- 11
       ioCassette1 boolean,	-- 12 Cassette 1 presence:  1 = cassette in place
       ioCassette2 boolean,	-- 13 Cassette 2 presence:  1 = cassette in place
       ioCassette3 boolean,	-- 14 Cassette 3 presence:  1 = cassette in place
       ioCassette4 boolean,	-- 15 Cassette 4 presence:  1 = cassette in place
       ioCassette5 boolean,	-- 16 Cassette 5 presence:  1 = cassette in place
       ioCassette6 boolean,	-- 17 Cassette 6 presence:  1 = cassette in place
       ioCassette7 boolean,	-- 18 Cassette 7 presence:  1 = cassette in place
       ioCassette8 boolean,	-- 19 Cassette 8 presence:  1 = cassette in place
       ioCassette9 boolean,	-- 20 Cassette 9 presence:  1 = cassette in place
       ioLid1Open boolean,	-- 21 Lid 1 Opened: 1 = lid completely opened
       ioLid2Open boolean,	-- 22 Lid 2 Opened: 1 = lid completely opened
       ioLid3Open boolean,	-- 23 Lid 3 Opened: 1 = lid completely opened
       ioToolOpened boolean,	-- 24 Tool Opened: 1 = tool completely opened
       ioToolClosed boolean,	-- 25 Tool Closed: 1 = tool completely closed
       ioLimit1 boolean,	-- 26 Limit Switch 1:  0 = gripper in diffractometer position
       ioLimit2 boolean,	-- 27 Limit Switch 2:  0 = gripper in dewar position
       ioTool boolean,		-- 28 Tool Changer:	0 = tool gripped, 1 = tool released
       ioToolOC boolean,	-- 29 Tool Open/Close:  0 = tool closed, 1 = tool opened
       ioFast boolean,		-- 30 Fast Output: 0 = contact open, 1 = contact closed
       ioUnlabed1 boolean,	-- 31 Usage not documented
       ioMagnet boolean,	-- 32 Output Process Information 2 [sic], Magnet ON: 0 = contact open, 1 = contact closed
       ioOutput2 boolean,	-- 33 Output Process Information 2: 0 = contact open, 1 = contact closed
       ioOutput3 boolean,	-- 34 Output Process Information 3: 0 = contact open, 1 = contact closed
       ioOutput4 boolean,	-- 35 Output Process Information 4: 0 = contact open, 1 = contact closed
       ioGreen boolean,		-- 36 Green Light (Power, air, and Modbus network OK): 1 = OK
       ioPILZ boolean,		-- 37 PILZ Relay Reset: 1 = reset of the relay
       ioServoOn boolean,	-- 38 Servo Card ON: 1 = card on
       ioServoRot boolean,	-- 39 Servo Card Rotation +/-: 0 = toard the diffractometer, 1 = toward the dewar
       ioLN2C boolean,		-- 40 Cryogen Valve LN2_C: 0 = closed, 1 = open
       ioLN2E boolean,		-- 41 Cryogen Valve LN2_E: 0 = closed, 1 = open
       ioLG2E boolean,		-- 42 Cryogen Valve GN2_E: 0 = closed, 1 = open
       ioHeater boolean,	-- 43 Heater ON/ODD: 0 = off, 1 = on
       ioUnlabed2 boolean,	-- 44 Usage not documented
       ioUnlabed3 boolean,	-- 45 Usage not documented
       ioUnlabed4 boolean,	-- 46 Usage not documented
       ioUnlabed5 boolean	-- 47 Usage not documented
);
ALTER TABLE cats.io OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION cats.ioInsertTF() returns trigger as $$
  DECLARE
    t record;
  BEGIN
    SELECT * INTO t FROM cats.io ORDER BY ioTSLast DESC LIMIT 1;
    IF FOUND THEN
      IF
        t.ioCryoOK = NEW.ioCryoOK AND
        t.ioESAP = NEW.ioESAP AND
        t.ioCollisionOK = NEW.ioCollisionOK AND
        t.ioCryoHighAlarm = NEW.ioCryoHighAlarm AND
        t.ioCryoHigh = NEW.ioCryoHigh AND
        t.ioCryoLow  = NEW.ioCryoLow  AND
        t.ioCryoLowAlarm = NEW.ioCryoLowAlarm AND
        t.ioCryoLiquid = NEW.ioCryoLiquid AND
        t.ioInput1 = NEW.ioInput1 AND
        t.ioInput2 = NEW.ioInput2 AND
        t.ioInput3 = NEW.ioInput3 AND
        t.ioInput4 = NEW.ioInput4 AND
        t.ioCassette1 = NEW.ioCassette1 AND
        t.ioCassette2 = NEW.ioCassette2 AND
        t.ioCassette3 = NEW.ioCassette3 AND
        t.ioCassette4 = NEW.ioCassette4 AND
        t.ioCassette5 = NEW.ioCassette5 AND
        t.ioCassette6 = NEW.ioCassette6 AND
        t.ioCassette7 = NEW.ioCassette7 AND
        t.ioCassette8 = NEW.ioCassette8 AND
        t.ioCassette9 = NEW.ioCassette9 AND
        t.ioLid1Open = NEW.ioLid1Open AND
        t.ioLid2Open = NEW.ioLid2Open AND
        t.ioLid3Open = NEW.ioLid3Open AND
        t.ioToolOpened = NEW.ioToolOpened AND
        t.ioToolClosed = NEW.ioToolClosed AND
        t.ioLimit1 = NEW.ioLimit1 AND
        t.ioLimit2 = NEW.ioLimit2 AND
        t.ioTool = NEW.ioTool AND
        t.ioToolOC = NEW.ioToolOC AND
        t.ioFast = NEW.ioFast AND
        t.ioUnlabed1 = NEW.ioUnlabed1 AND
        t.ioMagnet = NEW.ioMagnet AND
        t.ioOutput2 = NEW.ioOutput2 AND
        t.ioOutput3 = NEW.ioOutput3 AND
        t.ioOutput4 = NEW.ioOutput4 AND
        t.ioGreen = NEW.ioGreen AND
        t.ioPILZ = NEW.ioPILZ AND
        t.ioServoOn = NEW.ioServoOn AND
        t.ioServoRot = NEW.ioServoRot AND
        t.ioLN2C = NEW.ioLN2C AND
        t.ioLN2E = NEW.ioLN2E AND
        t.ioLG2E = NEW.ioLG2E AND
        t.ioHeater = NEW.ioHeater AND
        t.ioUnlabed2 = NEW.ioUnlabed2 AND
        t.ioUnlabed3 = NEW.ioUnlabed3 AND
        t.ioUnlabed4 = NEW.ioUnlabed4 AND
        t.ioUnlabed5 = NEW.ioUnlabed5
      THEN
        UPDATE cats.io SET ioTSLast = now() WHERE ioKey = t.ioKey;
        RETURN NULL;
      END IF;
    END IF;
    RETURN NEW;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION cats.ioInsertTF() OWNER TO lsadmin;

CREATE TRIGGER ioInsertTrigger BEFORE INSERT ON cats.io FOR EACH ROW EXECUTE PROCEDURE cats.ioInsertTF();


CREATE OR REPLACE FUNCTION  cats.setio(
       CryoOK boolean,	-- 00 Cryogen Sensors OK:  1 = OK
       ESAP   boolean,	-- 01 Emergency Stop and Air Pressure OK: 1 = OK
       CollisionOK boolean,	-- 02 Collision Sensor OK: 1 = No Collision
       CryoHighAlarm boolean, -- 03 Cryogen High Level Alarm: 1 = No Alarm
       CryoHigh boolean,	-- 04 Cryogen High Level: 1 = high level reached
       CryoLow  boolean,	-- 05 Cryogen Low Level: 1 = low level reached
       CryoLowAlarm boolean,	-- 06 Cryogen Low Level Alarm: 1 = no alarm
       CryoLiquid boolean,	-- 07 Cryogen Liquid Detection: 0=Gas, 1=Liquid
       Input1 boolean,	-- 08
       Input2 boolean,	-- 09
       Input3 boolean,	-- 10
       Input4 boolean,	-- 11
       Cassette1 boolean,	-- 12 Cassette 1 presence:  1 = cassette in place
       Cassette2 boolean,	-- 13 Cassette 2 presence:  1 = cassette in place
       Cassette3 boolean,	-- 14 Cassette 3 presence:  1 = cassette in place
       Cassette4 boolean,	-- 15 Cassette 4 presence:  1 = cassette in place
       Cassette5 boolean,	-- 16 Cassette 5 presence:  1 = cassette in place
       Cassette6 boolean,	-- 17 Cassette 6 presence:  1 = cassette in place
       Cassette7 boolean,	-- 18 Cassette 7 presence:  1 = cassette in place
       Cassette8 boolean,	-- 19 Cassette 8 presence:  1 = cassette in place
       Cassette9 boolean,	-- 20 Cassette 9 presence:  1 = cassette in place
       Lid1Open boolean,	-- 21 Lid 1 Opened: 1 = lid completely opened
       Lid2Open boolean,	-- 22 Lid 2 Opened: 1 = lid completely opened
       Lid3Open boolean,	-- 23 Lid 3 Opened: 1 = lid completely opened
       ToolOpened boolean,	-- 24 Tool Opened: 1 = tool completely opened
       ToolClosed boolean,	-- 25 Tool Closed: 1 = tool completely closed
       Limit1 boolean,	-- 26 Limit Switch 1:  0 = gripper in diffractometer position
       Limit2 boolean,	-- 27 Limit Switch 2:  0 = gripper in dewar position
       Tool boolean,		-- 28 Tool Changer:	0 = tool gripped, 1 = tool released
       ToolOC boolean,	-- 29 Tool Open/Close:  0 = tool closed, 1 = tool opened
       Fast boolean,		-- 30 Fast Output: 0 = contact open, 1 = contact closed
       Unlabed1 boolean,	-- 31 Usage not documented
       Magnet boolean,	-- 32 Output Process Information 2 [sic], Magnet ON: 0 = contact open, 1 = contact closed
       Output2 boolean,	-- 33 Output Process Information 2: 0 = contact open, 1 = contact closed
       Output3 boolean,	-- 34 Output Process Information 3: 0 = contact open, 1 = contact closed
       Output4 boolean,	-- 35 Output Process Information 4: 0 = contact open, 1 = contact closed
       Green boolean,		-- 36 Green Light (Power, air, and Modbus network OK): 1 = OK
       PILZ boolean,		-- 37 PILZ Relay Reset: 1 = reset of the relay
       ServoOn boolean,	-- 38 Servo Card ON: 1 = card on
       ServoRot boolean,	-- 39 Servo Card Rotation +/-: 0 = toard the diffractometer, 1 = toward the dewar
       LN2C boolean,		-- 40 Cryogen Valve LN2_C: 0 = closed, 1 = open
       LN2E boolean,		-- 41 Cryogen Valve LN2_E: 0 = closed, 1 = open
       LG2E boolean,		-- 42 Cryogen Valve GN2_E: 0 = closed, 1 = open
       Heater boolean,	-- 43 Heater ON/ODD: 0 = off, 1 = on
       Unlabed2 boolean,	-- 44 Usage not documented
       Unlabed3 boolean,	-- 45 Usage not documented
       Unlabed4 boolean,	-- 46 Usage not documented
       Unlabed5 boolean	-- 47 Usage not documented
) RETURNS INT AS $$
  BEGIN
    INSERT INTO cats.io (
       ioCryoOK,	-- 00 Cryogen Sensors OK:  1 = OK
       ioESAP  ,	-- 01 Emergency Stop and Air Pressure OK: 1 = OK
       ioCollisionOK,	-- 02 Collision Sensor OK: 1 = No Collision
       ioCryoHighAlarm, -- 03 Cryogen High Level Alarm: 1 = No Alarm
       ioCryoHigh,	-- 04 Cryogen High Level: 1 = high level reached
       ioCryoLow ,	-- 05 Cryogen Low Level: 1 = low level reached
       ioCryoLowAlarm,	-- 06 Cryogen Low Level Alarm: 1 = no alarm
       ioCryoLiquid,	-- 07 Cryogen Liquid Detection: 0=Gas, 1=Liquid
       ioInput1,	-- 08
       ioInput2,	-- 09
       ioInput3,	-- 10
       ioInput4,	-- 11
       ioCassette1,	-- 12 Cassette 1 presence:  1 = cassette in place
       ioCassette2,	-- 13 Cassette 2 presence:  1 = cassette in place
       ioCassette3,	-- 14 Cassette 3 presence:  1 = cassette in place
       ioCassette4,	-- 15 Cassette 4 presence:  1 = cassette in place
       ioCassette5,	-- 16 Cassette 5 presence:  1 = cassette in place
       ioCassette6,	-- 17 Cassette 6 presence:  1 = cassette in place
       ioCassette7,	-- 18 Cassette 7 presence:  1 = cassette in place
       ioCassette8,	-- 19 Cassette 8 presence:  1 = cassette in place
       ioCassette9,	-- 20 Cassette 9 presence:  1 = cassette in place
       ioLid1Open,	-- 21 Lid 1 Opened: 1 = lid completely opened
       ioLid2Open,	-- 22 Lid 2 Opened: 1 = lid completely opened
       ioLid3Open,	-- 23 Lid 3 Opened: 1 = lid completely opened
       ioToolOpened,	-- 24 Tool Opened: 1 = tool completely opened
       ioToolClosed,	-- 25 Tool Closed: 1 = tool completely closed
       ioLimit1,	-- 26 Limit Switch 1:  0 = gripper in diffractometer position
       ioLimit2,	-- 27 Limit Switch 2:  0 = gripper in dewar position
       ioTool,		-- 28 Tool Changer:	0 = tool gripped, 1 = tool released
       ioToolOC,	-- 29 Tool Open/Close:  0 = tool closed, 1 = tool opened
       ioFast,		-- 30 Fast Output: 0 = contact open, 1 = contact closed
       ioUnlabed1,	-- 31 Usage not documented
       ioMagnet,	-- 32 Output Process Information 2 [sic], Magnet ON: 0 = contact open, 1 = contact closed
       ioOutput2,	-- 33 Output Process Information 2: 0 = contact open, 1 = contact closed
       ioOutput3,	-- 34 Output Process Information 3: 0 = contact open, 1 = contact closed
       ioOutput4,	-- 35 Output Process Information 4: 0 = contact open, 1 = contact closed
       ioGreen,		-- 36 Green Light (Power, air, and Modbus network OK): 1 = OK
       ioPILZ,		-- 37 PILZ Relay Reset: 1 = reset of the relay
       ioServoOn,	-- 38 Servo Card ON: 1 = card on
       ioServoRot,	-- 39 Servo Card Rotation +/-: 0 = toard the diffractometer, 1 = toward the dewar
       ioLN2C,		-- 40 Cryogen Valve LN2_C: 0 = closed, 1 = open
       ioLN2E,		-- 41 Cryogen Valve LN2_E: 0 = closed, 1 = open
       ioLG2E,		-- 42 Cryogen Valve GN2_E: 0 = closed, 1 = open
       ioHeater,	-- 43 Heater ON/ODD: 0 = off, 1 = on
       ioUnlabed2,	-- 44 Usage not documented
       ioUnlabed3,	-- 45 Usage not documented
       ioUnlabed4,	-- 46 Usage not documented
       ioUnlabed5	-- 47 Usage not documented
) VALUES (
       CryoOK,	-- 00 Cryogen Sensors OK:  1 = OK
       ESAP  ,	-- 01 Emergency Stop and Air Pressure OK: 1 = OK
       CollisionOK,	-- 02 Collision Sensor OK: 1 = No Collision
       CryoHighAlarm, -- 03 Cryogen High Level Alarm: 1 = No Alarm
       CryoHigh,	-- 04 Cryogen High Level: 1 = high level reached
       CryoLow ,	-- 05 Cryogen Low Level: 1 = low level reached
       CryoLowAlarm,	-- 06 Cryogen Low Level Alarm: 1 = no alarm
       CryoLiquid,	-- 07 Cryogen Liquid Detection: 0=Gas, 1=Liquid
       Input1,	-- 08
       Input2,	-- 09
       Input3,	-- 10
       Input4,	-- 11
       Cassette1,	-- 12 Cassette 1 presence:  1 = cassette in place
       Cassette2,	-- 13 Cassette 2 presence:  1 = cassette in place
       Cassette3,	-- 14 Cassette 3 presence:  1 = cassette in place
       Cassette4,	-- 15 Cassette 4 presence:  1 = cassette in place
       Cassette5,	-- 16 Cassette 5 presence:  1 = cassette in place
       Cassette6,	-- 17 Cassette 6 presence:  1 = cassette in place
       Cassette7,	-- 18 Cassette 7 presence:  1 = cassette in place
       Cassette8,	-- 19 Cassette 8 presence:  1 = cassette in place
       Cassette9,	-- 20 Cassette 9 presence:  1 = cassette in place
       Lid1Open,	-- 21 Lid 1 Opened: 1 = lid completely opened
       Lid2Open,	-- 22 Lid 2 Opened: 1 = lid completely opened
       Lid3Open,	-- 23 Lid 3 Opened: 1 = lid completely opened
       ToolOpened,	-- 24 Tool Opened: 1 = tool completely opened
       ToolClosed,	-- 25 Tool Closed: 1 = tool completely closed
       Limit1,	-- 26 Limit Switch 1:  0 = gripper in diffractometer position
       Limit2,	-- 27 Limit Switch 2:  0 = gripper in dewar position
       Tool,		-- 28 Tool Changer:	0 = tool gripped, 1 = tool released
       ToolOC,	-- 29 Tool Open/Close:  0 = tool closed, 1 = tool opened
       Fast,		-- 30 Fast Output: 0 = contact open, 1 = contact closed
       Unlabed1,	-- 31 Usage not documented
       Magnet,	-- 32 Output Process Information 2 [sic], Magnet ON: 0 = contact open, 1 = contact closed
       Output2,	-- 33 Output Process Information 2: 0 = contact open, 1 = contact closed
       Output3,	-- 34 Output Process Information 3: 0 = contact open, 1 = contact closed
       Output4,	-- 35 Output Process Information 4: 0 = contact open, 1 = contact closed
       Green,		-- 36 Green Light (Power, air, and Modbus network OK): 1 = OK
       PILZ,		-- 37 PILZ Relay Reset: 1 = reset of the relay
       ServoOn,	-- 38 Servo Card ON: 1 = card on
       ServoRot,	-- 39 Servo Card Rotation +/-: 0 = toard the diffractometer, 1 = toward the dewar
       LN2C,		-- 40 Cryogen Valve LN2_C: 0 = closed, 1 = open
       LN2E,		-- 41 Cryogen Valve LN2_E: 0 = closed, 1 = open
       LG2E,		-- 42 Cryogen Valve GN2_E: 0 = closed, 1 = open
       Heater,	-- 43 Heater ON/ODD: 0 = off, 1 = on
       Unlabed2,	-- 44 Usage not documented
       Unlabed3,	-- 45 Usage not documented
       Unlabed4,	-- 46 Usage not documented
       Unlabed5	-- 47 Usage not documented
);

  RETURN 1;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION cats.setio( 
       CryoOK boolean,	-- 00 Cryogen Sensors OK:  1 = OK
       ESAP   boolean,	-- 01 Emergency Stop and Air Pressure OK: 1 = OK
       CollisionOK boolean,	-- 02 Collision Sensor OK: 1 = No Collision
       CryoHighAlarm boolean, -- 03 Cryogen High Level Alarm: 1 = No Alarm
       CryoHigh boolean,	-- 04 Cryogen High Level: 1 = high level reached
       CryoLow  boolean,	-- 05 Cryogen Low Level: 1 = low level reached
       CryoLowAlarm boolean,	-- 06 Cryogen Low Level Alarm: 1 = no alarm
       CryoLiquid boolean,	-- 07 Cryogen Liquid Detection: 0=Gas, 1=Liquid
       Input1 boolean,	-- 08
       Input2 boolean,	-- 09
       Input3 boolean,	-- 10
       Input4 boolean,	-- 11
       Cassette1 boolean,	-- 12 Cassette 1 presence:  1 = cassette in place
       Cassette2 boolean,	-- 13 Cassette 2 presence:  1 = cassette in place
       Cassette3 boolean,	-- 14 Cassette 3 presence:  1 = cassette in place
       Cassette4 boolean,	-- 15 Cassette 4 presence:  1 = cassette in place
       Cassette5 boolean,	-- 16 Cassette 5 presence:  1 = cassette in place
       Cassette6 boolean,	-- 17 Cassette 6 presence:  1 = cassette in place
       Cassette7 boolean,	-- 18 Cassette 7 presence:  1 = cassette in place
       Cassette8 boolean,	-- 19 Cassette 8 presence:  1 = cassette in place
       Cassette9 boolean,	-- 20 Cassette 9 presence:  1 = cassette in place
       Lid1Open boolean,	-- 21 Lid 1 Opened: 1 = lid completely opened
       Lid2Open boolean,	-- 22 Lid 2 Opened: 1 = lid completely opened
       Lid3Open boolean,	-- 23 Lid 3 Opened: 1 = lid completely opened
       ToolOpened boolean,	-- 24 Tool Opened: 1 = tool completely opened
       ToolClosed boolean,	-- 25 Tool Closed: 1 = tool completely closed
       Limit1 boolean,	-- 26 Limit Switch 1:  0 = gripper in diffractometer position
       Limit2 boolean,	-- 27 Limit Switch 2:  0 = gripper in dewar position
       Tool boolean,		-- 28 Tool Changer:	0 = tool gripped, 1 = tool released
       ToolOC boolean,	-- 29 Tool Open/Close:  0 = tool closed, 1 = tool opened
       Fast boolean,		-- 30 Fast Output: 0 = contact open, 1 = contact closed
       Unlabed1 boolean,	-- 31 Usage not documented
       Magnet boolean,	-- 32 Output Process Information 2 [sic], Magnet ON: 0 = contact open, 1 = contact closed
       Output2 boolean,	-- 33 Output Process Information 2: 0 = contact open, 1 = contact closed
       Output3 boolean,	-- 34 Output Process Information 3: 0 = contact open, 1 = contact closed
       Output4 boolean,	-- 35 Output Process Information 4: 0 = contact open, 1 = contact closed
       Green boolean,		-- 36 Green Light (Power, air, and Modbus network OK): 1 = OK
       PILZ boolean,		-- 37 PILZ Relay Reset: 1 = reset of the relay
       ServoOn boolean,	-- 38 Servo Card ON: 1 = card on
       ServoRot boolean,	-- 39 Servo Card Rotation +/-: 0 = toard the diffractometer, 1 = toward the dewar
       LN2C boolean,		-- 40 Cryogen Valve LN2_C: 0 = closed, 1 = open
       LN2E boolean,		-- 41 Cryogen Valve LN2_E: 0 = closed, 1 = open
       LG2E boolean,		-- 42 Cryogen Valve GN2_E: 0 = closed, 1 = open
       Heater boolean,	-- 43 Heater ON/ODD: 0 = off, 1 = on
       Unlabed2 boolean,	-- 44 Usage not documented
       Unlabed3 boolean,	-- 45 Usage not documented
       Unlabed4 boolean,	-- 46 Usage not documented
       Unlabed5 boolean	-- 47 Usage not documented
) OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION cats.setIOLast() returns void as $$
  DECLARE
    k bigint;
  BEGIN
    SELECT ioKey INTO k FROM cats.io ORDER BY ioTSLast DESC LIMIT 1;
    IF FOUND THEN
      UPDATE cats.io SET ioTSLast = now() WHERE ioKey=k;
    END IF;
    return;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION cats.setIOLast() OWNER TO lsadmin;


create table cats.positions (
       pKey serial primary key,
       pTSStart timestamp with time zone default now(),
       pTSLast  timestamp with time zone default now(),

       pX float,
       pY float,
       pZ float,
       pRX float,
       pRY float,
       pRZ float
);

CREATE OR REPLACE FUNCTION cats.positionsInsertTF() returns trigger as $$
  DECLARE
    t record;
  BEGIN
    SELECT * INTO t FROM cats.positions ORDER BY pTSLast DESC LIMIT 1;
    IF FOUND THEN
      IF
        t.pX = NEW.pX AND
        t.pY = NEW.pY AND
        t.pZ = NEW.pZ AND
        t.pRX = NEW.pRX AND
        t.pRY = NEW.pRY AND
        t.pRZ = NEW.pRZ
      THEN
        UPDATE cats.positions SET pTSLast = now() WHERE pKey = t.pKey;
        RETURN NULL;
      END IF;
    END IF;
    RETURN NEW;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION cats.positionsInsertTF() OWNER TO lsadmin;

CREATE TRIGGER positionsInsertTrigger BEFORE INSERT ON cats.positions FOR EACH ROW EXECUTE PROCEDURE cats.positionsInsertTF();

CREATE OR REPLACE FUNCTION cats.setposition( x float, y float, z float, rx float, ry float, rz float) returns int as $$
  BEGIN
    INSERT INTO cats.positions ( pX, pY, pZ, pRX, pRY, pRZ) VALUES ( x, y, z, rx, ry, rz);
    RETURN 1;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION cats.setposition( float, float, float, float, float, float) OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION cats.setPositionLast() returns void as $$
  DECLARE
    k bigint;
  BEGIN
    SELECT pKey INTO k FROM cats.positions ORDER BY pTSLast LIMIT 1;
    IF FOUND THEN
      UPDATE cats.positions SET pTSLast = now() WHERE pKey = k;
    END IF;
    RETURN;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION cats.setPositionLast() OWNER TO lsadmin;

create table cats.messages (
       mKey serial primary key,
       mTSStart timestamp with time zone default now(),
       mTSLast timestamp with time zone default now(),
       
       mmsg text
);

CREATE OR REPLACE FUNCTION cats.messagesInsertTF() returns trigger as $$
  DECLARE
    t record;
  BEGIN
    SELECT * INTO t FROM cats.messages ORDER BY mTSLast DESC LIMIT 1;
    IF FOUND THEN
      IF t.mmsg = NEW.mmsg THEN
        UPDATE cats.messages SET mTSLast = now() WHERE mKey = t.mKey;
        RETURN NULL;
      END IF;
    END IF;
    RETURN NEW;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION cats.messagesInsertTF() OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION cats.setmessage( msg text) returns int as $$
  BEGIN
    INSERT INTO cats.messages (mmsg) VALUES (msg);
    RETURN 1;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
ALTER FUNCTION cats.setmessage( text) OWNER TO lsadmin;

CREATE OR REPLACE FUNCTION cats.setMessageLast() returns void as $$
  DECLARE
    k bigint;
  BEGIN
    SELECT mKey INTO k FROM cats.messages ORDER BY mTSLast DESC LIMIT 1;
    IF FOUND THEN
      UPDATE cats.messages SET mTSLast = now() WHERE mKey = k;
    END IF;
  END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

ALTER FUNCTION cats.setMessageLast() OWNER TO lsadmin;
