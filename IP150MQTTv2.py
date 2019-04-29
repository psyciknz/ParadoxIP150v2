import hashlib
import socket
import time
import lib.client as mqtt
import sys
import array
import random
import ConfigParser
import struct
import importlib
import logging
import logging.handlers
import os.path
import json

version = "2.0.13"

# Alarm controls can be given in payload, e.g. Paradox/C/P1, payl = Disarm
################################################################################################
#  Paradox IP Module
################################################################################################
# Change History
################################################################################################
# 2019-04-03 2.0.13
# - [kovacsbalu] - Adding partition name on live events
# - [psyciknz]   - Adding partition names on keepalive messages (currently only partition 1)
# - [Psyciknz]   - added keepalivestatus1 tests
# - [Psyciknz]   - updated live events tests for partition name changes.
# - [Psyciknz]   - Changed UpdateZonesandAlarmsStatus to use the keepalive code as it was more accurate
# - [Psyciknz]   - Fixed bug where the keep alive count easing working, and events would publish every cycle
# - [Psyciknz]   - Extended the live events message location to message - checksum (about 34) for long zone names.
#
# 2019-02-15 2.0.12
# - Adding user customisable Alarm states = ARMED = 'armed', SLEEP = 'armed_night' - to help with home assistant.
# - Added $version topic to Topic_Publish_AppState (eg Paradox/State) + "/$version"
#
# 2019-02-15 2.0.11
# - Trying to add pytest.  Had to rename the name of the module to IP150MQTTv2.py  removing the dash.
# - Changed ParadoxMap ControlOutput registers from 30/31 to 32/33 which are supposed to not overwrtie commands.
#
# 2019-01-25 2.0.10
# - When disarming, triggered is also posted.  Trying to stop this from happening, and so it's only posted
#   when an actual alert.
#
# 2019-01-11 2.0.9
# - Moved subscribe message to on_connect.  May look at splitting various topics to different functions.
#
# 2018-11-08 2.0.8
# - Updated event logging to tell what even it was, as the message was the same.
# - Set the Alarm topic to retained
#
# 2018-06-14 2.0.7
# - Started adding processing of a triggered event (message 36).  
#   Currently setting Paradox/Partition/Status to Triggered
#   Currently setting Paradox/Partition/Alarm to IN Alarm: Zone <zonename>
#
# 2018-06-13 2.0.6
# - Added and arming and disarming state on live events, and on partition status (1) message
# - Added a new config item Publish_Zones_OpenClosed.  Set to 1 to have the service report OPEN for on/triggered
#   and CLOSED for off/non triggered zones and partitions.
#
# 2018-06-08 2.0.5
# - Reduced keep alive logging
#
# 2018-06-07 2.0.4
# - Added a heartbeat factor for limiting the publish amount for keep alive status'
# - Reduce some of the logging for debug messages.
#
# 2018-06-05 2.0.3
# - Added Zone Status messages to Status 0 Heartbeat (testfor events)
# - Added Partition 1 status checked to test for events (status 1 heartbeat) - will also post 
#   Paradox/Parition/Status of ARMED, DISARMED, SLEEP and STAY 
# 2018-06-01 2.0.2
# - Working in the Panel Status messages showed that we were not logging in correctly
#   and have added the pc password into the authentication this needs to refence the config.ini
# - Deciphering the panel status message 0 for panel time, vdc and battery
# - Will start deciphering the other messages as they given partition and zone status.
# - Added new config item Topic_Publish_status
# - Changed heartbeat to just the status 0 message and not every time.
# - Figured out arm (message 2 event 14 is full arm, message 2, event 12 is sleep arm)
#
# 2018-05-12 2.0.1
# - Deciphering the heartbeat request - which is actually the udpatezonestatus call.
#   this includes the arm/disarm state and battery voltage.

# 2018-05-12 2.0.0
# - Added heartbeat messsage and topic config
# - Is added as the last will and testament - meaning on script end it is set to false

# 2017-03-11
# - Longer delay on errors.
#
# 2017-03-02
# - Added logging around connection retries
# - Added time.sleep(5) on each failure
#
# 2017-02-26
# - Changed #print statementst to logging statement to generate a proper log file
# - Added new config key words Topic_Publish_ZoneState and Topic_Publish_Partition
# - Topic_Publish_ZoneState - that is called from TestEventMessages when a zone event is found, 
#   froamt Paradox/Zone/<zone name> - where zone name is extracted from the event packet (chars 15 - 30)
# - Topic_Publish_Partition - that is called from TestEventMessages when a partition 
#   arm/disarm event is found
###############################################################################################


# Do not edit these variables here, use the config.ini file instead.
Zone_Amount = 32
passw = "abcd"
user = "1234"
IP150_IP = "10.0.0.120"
IP150_Port = 10000
Poll_Speed = 30.5  # Seconds (float)
MQTT_IP = "10.0.0.130"
MQTT_Port = 1883
MQTT_KeepAlive = 60  # Seconds
mqtt_username = None
mqtt_password = None

# Options are Arm, Disarm, Stay, Sleep (case sensitive!)
Topic_Publish_Events = "Paradox/Events"
Events_Payload_Numeric = "False"
Topic_Subscribe_Control = "Paradox/C/" # e.g. To arm partition 1: Paradox/C/P1/Arm
Startup_Publish_All_Info = "True"
Startup_Update_All_Labels = "True"
Topic_Publish_Labels = "Paradox/Labels"
Topic_Publish_AppState = "Paradox/State"
Topic_Publish_ZoneState = "Paradox/Zone"
Topic_Publish_ArmState = "Paradox/Partition"
Topic_Publish_Heartbeat = "Paradox/Heatbeat"
Publish_Status_Factor = 1
Topic_Publish_Status = "Paradox/Status"
Publish_Zones_OpenClosed = 0
ZonesOn = "ON"
ZonesOff = "OFF"
Publish_Static_Topic = 0
Alarm_Model = "ParadoxMG5050"
Alarm_Registry_Map = "ParadoxMG5050"
Alarm_Event_Map = "ParadoxMG5050"

# Global variables
Alarm_Control_Action = 0
Alarm_Control_Partition = 0
Alarm_Control_NewState = ""
Output_FControl_Action = 0
Output_FControl_Number = 0
Output_FControl_NewState = ""
Output_PControl_Action = 0
Output_PControl_Number = 0
Output_PControl_NewState = ""
State_Machine = 0
Polling_Enabled = 1
Debug_Mode = 0
Error_Delay = 30

#Logging
LOG_LEVEL = logging.INFO
LOG_FILE = "/var/log/paradoxip.log"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
#logging.basicConfig(filename=LOG_FILE, format=LOG_FORMAT, level=LOG_LEVEL)

logger = logging.getLogger()

def ConfigSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                logging.info("skip: %s" % option)
        except:
            logging.error("exception on %s!" % option)
            dict1[option] = None
    return dict1


def on_connect(client, userdata, flags, rc):
    logging.info("Connected to MQTT broker with result code " + str(rc))
    client.subscribe(Topic_Subscribe_Control + "#")

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("$SYS/#")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global Alarm_Control_Partition
    global Alarm_Control_NewState
    global Alarm_Control_Action
    global Output_FControl_Number
    global Output_FControl_NewState
    global Output_FControl_Action
    global Output_PControl_Number
    global Output_PControl_NewState
    global Output_PControl_Action
    global Polling_Enabled

    logging.info("MQTT Message: " + msg.topic + " " + str(msg.payload))

    topic = msg.topic


    if Topic_Subscribe_Control in msg.topic:
        if "Polling" in msg.topic:
            if "Enable" in msg.topic:
                logging.info("Enable polling message received...")
                client.publish(Topic_Publish_AppState, "Polling: Enabling...", 1, True)
                Polling_Enabled = 1
            if "Disable" in msg.topic:
                logging.info("Disable polling message received...")
                Polling_Enabled = 0

        elif "/FO/" in msg.topic:
            try:
                Output_FControl_Number = int((topic.split(Topic_Subscribe_Control + 'FO/'))[1].split('/')[0])
                logging.info("Output force control number: %s " % Output_FControl_Number)
                try:
                    Output_FControl_NewState = (topic.split('/FO/' + str(Output_FControl_Number) + '/'))[1]
                except Exception:
                    Output_FControl_NewState = msg.payload
                    if len(Output_FControl_NewState) < 1:
                        logging.info('No payload given for control number: e.g. On')
                logging.info("Output force control state: %s " % Output_FControl_NewState)
                client.publish(Topic_Publish_AppState,
                               "Output: Forcing PGM " + str(Output_FControl_Number) + " to state: " + Output_FControl_NewState, 1, True)
                Output_FControl_Action = 1
            except:
                logging.error("MQTT message received with incorrect structure")

        elif "/PO/" in msg.topic:
            try:
                Output_PControl_Number = int((topic.split(Topic_Subscribe_Control + 'PO/'))[1].split('/')[0])
                logging.info("Output pulse control number: %s " % Output_PControl_Number)
                try:
                    Output_PControl_NewState = (topic.split('/PO/' + str(Output_PControl_Number) + '/'))[1]
                except Exception, e:
                    Output_PControl_NewState = msg.payload
                    if len(Output_PControl_NewState) < 1:
                        logging.error('No payload given for control number: e.g. On')
                logging.info("Output pulse control state: %s" % Output_PControl_NewState)
                client.publish(Topic_Publish_AppState,
                               "Output: Pulsing PGM " + str(Output_PControl_Number) + " to state: " + Output_PControl_NewState,
                               1, True)
                Output_PControl_Action = 1
            except:
                logging.error("MQTT message received with incorrect structure")
        elif "/P" in msg.topic:
            try:
                Alarm_Control_Partition = int((topic.split(Topic_Subscribe_Control + 'P'))[1].split('/')[0])
                logging.info("Alarm control partition: %s" % Alarm_Control_Partition)
                try:
                    Alarm_Control_NewState = (topic.split('/P' + str(Alarm_Control_Partition) + '/'))[1]
                except Exception:
                    Alarm_Control_NewState = msg.payload
                    if len(Alarm_Control_NewState) < 1:
                        logging.error('No payload given for alarm control: e.g. Disarm')

                logging.info("Alarm control state: %s" % Alarm_Control_NewState)
                client.publish(Topic_Publish_AppState,
                               "Alarm: Control partition " + str(Alarm_Control_Partition) + " to state: " + Alarm_Control_NewState,
                               1, True)
                Alarm_Control_Action = 1
            except:
                logging.error("MQTT message received with incorrect structure")


def connect_ip150socket(address, port):
    try:
        #print "trying to connect %s" % address
        logging.info("Connecting to %s" % address)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((address, port))
        #print "connected"
    except Exception, e:
        logging.error( "Error connecting to IP module (exiting): " + repr(e))
        #print "error connecting"
        client.publish(Topic_Publish_AppState,
                       "Error connecting to IP module (exiting): " + repr(e),
                       1, True)
        sys.exit()

    return s


class paradox:
    loggedin = 0
    aliveSeq = 0
    alarmName = None
    zoneTotal = 0
    zoneStatus = ['']
    zoneNames = {}
    partitions = {}
    zonePartition = None
    partitionStatus = None
    partitionName = None
    Skip_Update_Labels = 0
    Alarm_Partition_States = {'DISARMED':'DISARMED', 
    'ARMED':'ARMED',
    'SLEEP':'SLEEP',  #ARMED_NIGHT
    'STAY':'STAY',  #ARMED_HOME
    'ARMING':'ARMING', #PENDING
    'TRIGGERED':'TRIGGERED'}
    keepalivecount = 0
    

    def __init__(self, _transport, client,_encrypted=0, _retries=10, _alarmeventmap="ParadoxMG5050",
                 _alarmregmap="ParadoxMG5050"):
        self.comms = _transport  # instance variable unique to each instance
        self.retries = _retries
        self.encrypted = _encrypted
        self.alarmeventmap = _alarmeventmap
        self.alarmregmap = _alarmregmap
        self.client = client

        # MyClass = getattr(importlib.import_module("." + self.alarmmodel + "EventMap", __name__))

        try:
            mod = __import__("ParadoxMap", fromlist=[self.alarmeventmap + "EventMap"])
            self.eventmap = getattr(mod, self.alarmeventmap + "EventMap")
        except Exception, e:
            logging.error("Failed to load Event Map: %s " % repr(e))
            logging.error("Defaulting to MG5050 Event Map...")
            try:
                mod = __import__("ParadoxMap", fromlist=["ParadoxMG5050EventMap"])
                self.eventmap = getattr(mod, "ParadoxMG5050EventMap")
            except Exception, e:
                logging.error("Failed to load Event Map (exiting): %s" % repr(e))
                sys.exit()

        try:
            mod = __import__("ParadoxMap", fromlist=[self.alarmregmap + "Registers"])
            self.registermap = getattr(mod, self.alarmregmap + "Registers")
        except Exception, e:
            logging.error("Failed to load Register Map (defaulting to not update labels from alarm): %s" % repr(e))
            self.Skip_Update_Labels = 1



            # self.eventmap = ParadoxMG5050EventMap  # Need to check panel type here and assign correct dictionary!
            # self.registermap = ParadoxMG5050Registers  # Need to check panel type here and assign correct dictionary!
    
    def skipLabelUpdate(self):
        return self.Skip_Update_Labels

    def saveState(self):
        self.eventmap.save()

    def loadState(self):
        logging.info("Loading previous event states and labels from file")
        self.eventmap.load()

    def login(self, password,pcpassword, Debug_Mode=0):  # Construct the login message, 16 byte header +
        # 16byte [or multiple] payloading being the password
        logging.info("Logging into alarm system...")

        header = "\xaa"  # First construct the 16 byte header, starting with 0xaa

        header += bytes(bytearray([len(password)]))  # Add the length of the password which is appended after the header
        header += "\x00\x03"  # No idea what this is

        if self.encrypted == 0:  # Encryption flag
            header += "\x08"  # Encryption off [default for now]
        else:
            header += "\x09"  # Encryption on

        header += "\xf0\x00\x0a"  # No idea what this is, although the fist byte seems like a sequence number
        # header += "\xf0\x00\x0e\x00\x01"    # iParadox initial request

        header = header.ljust(16, '\xee')  # The remained of the 16B header is filled with 0xee

        message = password  # Add the password as the start of the payload

        # FIXME: Add support for passwords longer than 16 characters
        message = message.ljust(16, '\xee')  # The remainder of the 16B payload is filled with 0xee

        reply = self.readDataRaw(header + message, Debug_Mode)  # Send message to the alarm panel and read the reply

        if reply[4] == '\x38':
            logging.info("Login to alarm panel successful")
            loggedin = 1
        else:
            loggedin = 0
            logging.info("Login request unsuccessful, panel returned: " + " ".join(hex(ord(reply[4]))))
            return

        header = list(header)

        header[1] = '\x00'
        header[5] = '\xf2'
        header2 = "".join(header)
        self.readDataRaw(header2, Debug_Mode)

        header[5] = '\xf3'
        header2 = "".join(header)
        reply = self.readDataRaw(header2, Debug_Mode)

        reply = list(reply)  # Send "waiting" header until reply is at least 48 bytes in length indicating ready state

        header[1] = '\x25'
        header[3] = '\x04'
        header[5] = '\x00'
        header2 = "".join(header)
        message = '\x72\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        message = self.format37ByteMessage(message)
        reply = self.readDataRaw(header2 + message, Debug_Mode)

        # A - no sending after this
        header[1] = '\x26'
        header[3] = '\x03'
        header[5] = '\xf8'
        header2 = "".join(header)
        message = '\x50\x00\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        message = self.format37ByteMessage(message)
        reply = self.readDataRaw(header2 + message, Debug_Mode)

        header[1] = '\x25'
        header[3] = '\x04'
        header[5] = '\x00'
        header2 = "".join(header)
        #Command 0x5F : Start communication
        logging.info( "Command 0x5F : Start communication")
        message = '\x5f\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        message = self.format37ByteMessage(message)
        reply = self.readDataRaw(header2 + message, Debug_Mode)
        # pull from here, the panel pd, firmware version.
        header[1] = '\x25'
        header[3] = '\x04'
        header[5] = '\x00'
        header[7] = '\x14'
        header2 = "".join(header)
        # reply = '\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11\x12\x13\x14\x15\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11\x12\x13\x14\x15\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x11\x12\x13\x14\x15\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09'
        #          0xaa 0x25 0x0 0x2 0x72 0x0 0x0 0x0 0x0 0xee 0xee 0xee 0xee 0xee 0xee 0xee ### 0x0 0x0 0x0 0x0 0x16 0x6 0x10 0x2 0x27 0x29 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x0 0x7e
        #get the first half of the message with the prodct type and panel id into the new 00 request
        message = reply[16:26]

        #print "***********************************Product Type: {}".format(hex(ord(message[4])))
        #print "***********************************Firmware: {}.{}.{}".format(hex(ord(message[5])),hex(ord(message[6])),hex(ord(message[7])))
        #print "********************************Panel ID: {} {}".format(hex(ord(message[8])),hex(ord(message[9])))
        #print "COMMS MESSAGE  : " + " ".join(hex(ord(i)) for i in message)
        #print "********************************P"
        
        #need to work out how to get pcpassword (PIN) form Config which) to hex string.
        #eg PIN of 1234 should be added as b'\x12' b'\x34' not converted.
        hex_data = pcpassword.decode("hex")
        message += hex_data

        message += '\x19\x00\x00'
        message += reply[31:39]
        message += '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00'
        message = self.format37ByteMessage(message)
        logging.info( "Command 0x00 : Initialize communication")
        reply = self.readDataRaw(header2 + message, Debug_Mode)

        #print "Command 0x50 : PC Status 0"
        header[1] = '\x25'
        header[3] = '\x04'
        header[5] = '\x00'
        header[7] = '\x14'
        header2 = "".join(header)
        message = '\x50\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        message = self.format37ByteMessage(message)
        reply = self.readDataRaw(header2 + message, Debug_Mode)

        header[1] = '\x25'
        header[3] = '\x04'
        header[5] = '\x00'
        header[7] = '\x14'
        header2 = "".join(header)
        message = '\x50\x00\x0e\x52\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
        message = self.format37ByteMessage(message)
        reply = self.readDataRaw(header2 + message, Debug_Mode)

        return loggedin

    def format37ByteMessage(self, message):
        checksum = 0

        if len(message) % 37 != 0:

            for val in message:  # Calculate checksum
                checksum += ord(val)

            ##print "CS: " + str(checksum)
            while checksum > 255:
                checksum = checksum - (checksum / 256) * 256

            ##print "CS: " + str(checksum)

            message += bytes(bytearray([checksum]))  # Add check to end of message

            msgLen = len(message)  # Pad with 0xee till end of last 16 byte message

            if (msgLen % 16) != 0:
                message = message.ljust((msgLen / 16 + 1) * 16, '\xee')

        ##print " ".join(hex(ord(i)) for i in message)

        return message

    # Implementation inspired by https://github.com/bioego/Paradox-UWP
    def updateZoneAndAlarmStatus(self, Startup_Publish_All_Info="True", Debug_Mode=0):
        header = "\xaa\x25\x00\x04\x08\x00\x00\x14\xee\xee\xee\xee\xee\xee\xee\xee"
        message = "\x50\x00\x80"
        # reguest ID
        message += "\x00"
        #NU - 4 - 32  4   5   6   7   8   9  10   11  12  13  14  15  16  17  18  19  20  21  22  23  24  25  26  27  28  29  30  31  32  33
        message += "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        #source  
        message += "\x00"
        #user id
        message += "\x00\x00"
        # 2nd byte was d0
        #message += "\xd0\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee"
        reply = self.readDataRaw(header + self.format37ByteMessage(message), Debug_Mode)
        if len(reply) < 39:
          #print "Response without zone status: {}".format(len(reply))
          return
        #print "***************************************************** printing values"
        data = reply[16:]

        # 2019-04-04 - Switch to using the KeepAliveStatus1 reply processor rather than do it again 
        #              helps with all the partition name names for 2.0.11
        self.keepAliveStatus0(data,Debug_Mode,0)
        #print "heart beat status 0 reply: <--" + " ".join(hex(ord(i)) for i in data)
        #print "Value 16 ({}) and 17 ({}) ".format(ord(data[0]), ord(data[1]))
        # if data[1] == '\x00' and (data[0] == '\x50' or data[0] == '\x52'):
        #     #print "Year :  {}{}".format(ord(data[9]),ord(data[10]))
        #     #print "Month : {}".format( ord(data[11]))
        #     #print "Day :   {}".format(ord(data[12]))
        #     #print "Hour:   {}".format( ord(data[13]))
        #     #print "minute: {}".format( ord(data[14]))
        #     #print "ac:  {}".format( ord(data[15]))
        #     #print "DC:  {}".format( ord(data[16]))
        #     #print "BDC: {}".format(ord(data[17]))
        #     # Skip to zone status
        #     reply = reply[25:]
        #     reply = reply[1:]
        # else:
        #     #print "No 00 record found"
        #     # Skip to zone status
        #     reply = reply[25:]
        #     #reply = reply[10:] # skip date, time and voltages
        #     reply = reply[10:]
        
        
        
        
        # for x in range(4):
        #   data = ord(reply[x])
        #   for y in range(8):
        #     bit = data & 1
        #     data = data / 2
        #     itemNo = x * 8 + y + 1
        #     if itemNo in self.zoneNames.keys():
        #       location = self.zoneNames[itemNo]
        #       if len(location) > 0:
        #         zoneState = ZonesOn if bit else ZonesOff
        #         #print "Publishing initial zone state (state:" + zoneState + ", zone:" + location + ")"
        #         #client.publish(Topic_Publish_ZoneState + "/" + location, "ON" if bit else "OFF", qos=1, retain=True)
        #         self.client.publish(Topic_Publish_ZoneState + "/" + location, zoneState, qos=1, retain=True)
        time.sleep(0.3)
        message =  "\x50\x00\x80\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        message += "\x00\x00\x00\x00\x00\x00\x00\x00\x00\xd1\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee"
        #print "heart beat status 1 request: -->" + " ".join(hex(ord(i)) for i in message)
        reply = self.readDataRaw(header + self.format37ByteMessage(message), Debug_Mode)
        #print "heart beat reply status 1 : <--" + " ".join(hex(ord(i)) for i in reply)
        if len(reply) < 34:
          #print "Response without zone status"
          return
        # Skip to alarm status
        data = reply[16:]

        # 2019-04-04 - Switch to using the KeepAliveStatus1 reply processor rather than do it again 
        #              helps with all the partition name names for 2.0.11
        self.keepAliveStatus1(data,Debug_Mode,0)


        # reply = reply[33:]
        # alarmState = ord(reply[0])
        # alarmState = ZonesOn if (alarmState & 1) else ZonesOff
        # #print "Publishing initial alarm state (state:" + alarmState + ")"
        # if Debug_Mode >= 1:
        #     logging.debug("updateZoneAndAlarmStatus: Publishing initial alarm state (state:" + alarmState + ")")

        # #client.publish(Topic_Publish_ArmState, alarmState, qos=1, retain=True)
        # if Startup_Publish_All_Info == "True":
        #     self.client.publish(Topic_Publish_ArmState,  self.Alarm_Partition_States[alarmState], qos=1, retain=True)
        time.sleep(0.3)
        return

    def updateAllLabels(self, Startup_Publish_All_Info="True", Topic_Publish_Labels="True", Debug_Mode=0):

        for func in self.registermap.getsupportedItems():

            if Debug_Mode >= 2:
                logging.debug("updateAllLabels: Reading from alarm: " + func)

            try:

                register_dict = getattr(self.registermap, "get" + func + "Register")()
                mapping_dict = getattr(self.eventmap, "set" + func)

                total = sum(1 for x in register_dict if isinstance(x, int))

                if Debug_Mode >= 2:
                    logging.debug("updateAllLabels: Amount of numeric items in dictionary to read: " + str(total))

                header = register_dict["Header"]
                skip_next = 0

                for x in range(1, total + 1):

                    if skip_next == 1:
                        skip_next = 0
                        continue

                    # #print "Update generic registers step: " + str(x)

                    message = register_dict[x]["Send"]
                    try:
                        next_message = register_dict[x + 1]["Send"]
                    except KeyError:
                        skip_next = 1
                        # #print "no next key"

                    # #print "Current msg " + " ".join(hex(ord(i)) for i in message)
                    # #print "Next msg    " + " ".join(hex(ord(i)) for i in next_message)

                    assert isinstance(message, basestring), "Message to be sent is not a string: %r" % message
                    message = message.ljust(36, '\x00')

                    # #print " ".join(hex(ord(i)) for i in message)

                    reply = self.readDataRaw(header + self.format37ByteMessage(message), Debug_Mode)

                    start = register_dict[x]["Receive"]["Start"]
                    finish = register_dict[x]["Receive"]["Finish"]
                    # self.zoneNames.append(reply[start:finish].rstrip()) FIXME: remove all internal zoneNames references and only use the dict
                    # newzone = reply[start:finish].rstrip().translate(None, '\x00')
                    newzone = reply[start:finish].rstrip().translate(None, '\x00').decode('utf8', 'ignore').encode('utf8')
                    mapping_dict(x, newzone)

                    if (skip_next == 0) and (message[0:len(next_message)] == next_message):
                        # #print "Same"
                        start = register_dict[x + 1]["Receive"]["Start"]
                        finish = register_dict[x + 1]["Receive"]["Finish"]
                        #newzone = reply[start:finish].rstrip().translate(None, '\x00')
                        newzone = reply[start:finish].rstrip().translate(None, '\x00').decode('utf8', 'ignore').encode('utf8')
                        mapping_dict(x + 1, newzone)
                        skip_next = 1

                try:
                    completed_dict = getattr(self.eventmap, "getAll" + func)()
                    if Debug_Mode >= 1:
                        logging.info("Labels detected for " + func + ":")
                        logging.info(completed_dict)
                except Exception, e:
                    logging.error("Failed to load supported function's completed mappings after updating: %s" % repr(e))


                topic = func.split("Label")[0]
                if Startup_Publish_All_Info == "True":
                    topic = func.split("Label")[0]
                    if topic[0].upper() + topic[1:] + "s" == "Zones":
                        self.zoneNames = completed_dict
                    elif topic.upper() == "PARTITION":
                        self.partitions = completed_dict

                    logging.info("updateAllLabels:  Topic being published " + Topic_Publish_Labels + "/" + topic[0].upper() + topic[1:] + "s" + ';'.join('{}{}'.format(key, ":" + val) for key, val in completed_dict.items()))
                    self.client.publish(Topic_Publish_Labels + "/" + topic[0].upper() + topic[1:] + "s",
                                   ';'.join('{}{}'.format(key, ":" + val) for key, val in completed_dict.items()), 1, True)
                else:
                    if topic[0].upper() + topic[1:] + "s" == "Zones":
                        self.zoneNames = completed_dict
                    elif topic.upper() == "PARTITION":
                        self.partitions = completed_dict

                #print self.zoneNames


            except Exception, e:
                logging.error("Failed to load supported function's mapping: %s" % repr(e))

        return

    def testForEvents(self, Events_Payload_Numeric=0, Publish_Static_Topic=0, Debug_Mode=0, data=None):

        if data is None:
            reply_amount, headers, messages = self.splitMessage(self.readDataRaw('', Debug_Mode))
        else:
            messages = data
            reply_amount = 1
        interrupt = 0  # Signal 3rd party connection interrupt

        #if Debug_Mode >= 1:
        #    logging.debug('.')

        reply = '.'

        if Debug_Mode > 1 and reply_amount > 1:
            logging.debug("Multiple data: " + repr(messages))

        if reply_amount > 0:
            if self.retries < 10:
                logging.info("Setting retries back to 3 after a couple of errors")
                self.retries = 10

            for message in messages:

                if Debug_Mode >= 2:
                    logging.debug("Event data: " + " ".join(hex(ord(i)) for i in message))

                if len(message) > 0:
                    # live event
                    if message[0] == '\xe2' or message[0] == '\xe0':

                        try:
                            location = ""
                            if Events_Payload_Numeric == 0:

                                event, subevent = self.eventmap.getEventDescription(ord(message[7]), ord(message[8]))
                                location = message[15:31].translate(None, '\x00').translate(None,'\x01').strip()
                                if location and Debug_Mode >= 1:
                                    logging.debug("Event location: \"%s\"" % location)
                                    #print "Event location: \"%s\"" % location

                                reply = "Event:" + event + ";SubEvent:" + subevent

                                if Debug_Mode >= 2:
                                    #print "Events 7-{} 8-{}- Reply: {}".format(ord(message[7]),ord(message[8]),reply)
                                    logging.debug("Events 7-{} 8-{}- Reply: {}".format(ord(message[7]),ord(message[8]),reply))

                                try:
                                    if (ord(message[7]) == 0 or ord(message[7]) ==1) and (self.zoneNames is not None and len(self.zoneNames) > 0):
                                        if Debug_Mode >= 2:
                                            logging.debug("Message is a 1 or 0, and self.Zonenames not empty")
                                        zonename = self.zoneNames[ord(message[8])]
                                        if zonename != location:
                                            logging.info("Zonename from labels {0} does not match event location {1}, updating".format(zonename,location))
                                            self.zoneNames[ord(message[8])]= location
                                        #else:
                                        #    logging.debug("Updating zone name to match location in live event")
                                        #    #print "zones {0} matches location {1} ".format(zonename,location)
                                except Exception as ezone:
                                        logging.error("Exception checking/updating zone names: {}".format(ezone.message))

                                # zone status messages Paradox/Zone/ZoneName 0 for close, 1 for open
                                if ord(message[7]) == 0:
                                    #Zone state off
                                    logging.info("Publishing ZONE event \"%s\" for \"%s\" =  %s" % (Topic_Publish_ZoneState, location, ZonesOff))
                                    self.client.publish(Topic_Publish_ZoneState + "/" + location,ZonesOff, qos=1, retain=True)
                                elif ord(message[7]) == 1:
                                    #zone state on
                                    logging.info("Publishing ZONE event \"%s\" for \"%s\" =  %s" % (Topic_Publish_ZoneState, location, ZonesOn))
                                    self.client.publish(Topic_Publish_ZoneState + "/" + location,ZonesOn, qos=1, retain=True)
                                elif ord(message[7]) == 2 and (ord(message[8]) == 11 or ord(message[8]) == 3):   #Disarm
                                    #partition disarmed event
                                    logging.info("Publishing DISARMED event \"%s\" =  \"%s\"" % (Topic_Publish_ArmState, self.Alarm_Partition_States['DISARMED']))
                                    self.client.publish(Topic_Publish_ArmState ,ZonesOff, qos=1, retain=True)
                                    self.client.publish(Topic_Publish_ArmState + "/" + location+ "/Status" ,self.Alarm_Partition_States['DISARMED'], qos=1, retain=True)
                                elif ord(message[7]) == 6 and (ord(message[8]) == 4 ):   #SLEEP
                                    #partition sleep armed event
                                    #12 is sleep arm, 14 is full arm- is STAY 13?
                                    logging.info("Publishing SLEEP event \"%s\" =  \"%s\"" % (Topic_Publish_ArmState, self.Alarm_Partition_States['SLEEP']))
                                    self.client.publish(Topic_Publish_ArmState ,ZonesOn, qos=1, retain=True)
                                    self.client.publish(Topic_Publish_ArmState + "/" + location+ "/Status", self.Alarm_Partition_States['SLEEP'], qos=1, retain=True)
                                elif ord(message[7]) == 6 and (ord(message[8]) == 3 ):   #STAY
                                    #partition stayd armed event
                                    #12 is sleep arm, 14 is full arm- is STAY 13?
                                    logging.info("Publishing STAY event \"%s\" =  \"%s\"" % (Topic_Publish_ArmState, self.Alarm_Partition_States['STAY']))
                                    self.client.publish(Topic_Publish_ArmState ,ZonesOn, qos=1, retain=True)
                                    self.client.publish(Topic_Publish_ArmState + "/" + location+ "/Status", self.Alarm_Partition_States["STAY"], qos=1, retain=True)
                                elif ord(message[7]) == 2 and (ord(message[8]) == 12):   #arm
                                    #partition full armed event
                                    #12 is sleep arm, 14 is full arm - is STAY 13?
                                    logging.info("Publishing ARMED event \"%s\" =  \"%s\"" % (Topic_Publish_ArmState, self.Alarm_Partition_States["ARMED"]))
                                    self.client.publish(Topic_Publish_ArmState ,ZonesOn, qos=1, retain=True)
                                    self.client.publish(Topic_Publish_ArmState + "/" + location+ "/Status", self.Alarm_Partition_States["ARMED"], qos=1, retain=True)
                                elif ord(message[7]) == 2 and (ord(message[8]) == 9):   #Arming state on Swawk off
                                    #sqwak off messages - part of the arming sequence.
                                    logging.info("Publishing ARMING event \"%s\" =  \"%s\"" % (Topic_Publish_ArmState, self.Alarm_Partition_States["ARMING"]))
                                    self.client.publish(Topic_Publish_ArmState + "/" + location+ "/Status", self.Alarm_Partition_States["ARMING"], qos=1, retain=True)
                                elif ord(message[7]) == 9: # and ord(message[8] == 1): # remote button pressed
                                    #remote button pressed
                                    #print "button pressed: " + str(ord(message[7])) #+ " " +  str(ord(message[8]))
                                    if message[8]:
                                       #print "Message 8: %s" % str(ord(message[8]))
                                       logging.info("Publishing PGM event \"%s Button%s\" =  \"%s\"" % (Topic_Publish_Events,str(ord(message[8])), "ON"))
                                       self.client.publish(Topic_Publish_Events + "/PGM" + str(ord(message[8])) ,"ON", qos=1, retain=True)
                                elif (ord(message[7]) == 36 or ord(message[7]) == 37) and (ord(message[8]) == 11):
                                    #zone triggered = 36
                                    #Smoke alarm = 37
                                    #2018-06-14 07:55:49,749 DEBUG Events 7-36 8-11- Reply: Event:Zone in alarm;SubEvent:Mid toilet Reed
                                    #2018-06-14 07:55:49,752 DEBUG Message 7: 36 Message 8: 11
                                    logging.info("Publishing Triggered event \"%s\" =  \"%s\"" % (Topic_Publish_ArmState, self.Alarm_Partition_States["TRIGGERED"]))
                                    self.client.publish(Topic_Publish_ArmState + "/" + location + "/Status", self.Alarm_Partition_States["TRIGGERED"], qos=1, retain=True)
                                    self.client.publish(Topic_Publish_ArmState + "/Alarm" ,"IN ALARM, Zone: " + location, qos=1, retain=True)
                                else:
                                    logging.debug("Events 7-{} 8-{}- Reply: {}".format(ord(message[7]),ord(message[8]),reply))
  

                            if Events_Payload_Numeric == 1:

                                reply = "E:" + str(ord(message[7])) + ";SE:" + str(ord(message[8]))
                                logging.info("Publishing event E\"%s\" for :SE \"%s\" " % (str(ord(message[7])), str(ord(message[8])) ) )

                            if Publish_Static_Topic == "1":
                                self.client.publish(Topic_Publish_Events + "/" + str(ord(message[7])) + "/" + str(ord(message[8])), qos=1, retain=False)

                            if Debug_Mode >= 2:
                                logging.debug("Message 7: {0} Message 8: {1}".format(ord(message[7]),ord(message[8])))

                            self.client.publish(Topic_Publish_Events, reply, qos=0, retain=False)

                            if Debug_Mode > 1:
                                logging.debug(reply)

                        except ValueError:
                            reply = "No register entry for Event: " + str(ord(message[7])) + ", Sub-Event: " + str(
                                ord(message[8]))

                    elif message[0] == '\x75' and message[1] == '\x49':
                        interrupt = 1
                    elif (message[0] == '\x52' or message[0] == 'x50') and message[2] =='\x80':
                        if Debug_Mode >= 2:
                            logging.debug("KEEP ALIVE REPLY FOUND********************{}*******".format(ord(message[3])) +  " ".join(hex(ord(i)) for i in message))
                        if ord(message[3]) == 0:
                            self.keepAliveStatus0(message, Debug_Mode)
                        elif ord(message[3]) == 1:
                            self.keepAliveStatus1(message, Debug_Mode)
                        elif Debug_Mode >= 2:
                            logging.debug("Other Keepalive Sequence reply: {}".format(ord(message[3])))
                    else:
                        reply = "Unknown event: " + " ".join(hex(ord(i)) for i in message)

        return interrupt

    def splitMessage(self, request=''):  # FIXME: Make msg a list to handle multiple 37byte replies

        if len(request) > 0:

            requests = request.split('\xaa')

            del requests[0]

            for i, val in enumerate(requests):
                requests[i] = '\xaa' + val
                # #print "Request seq " + str(i) + ": " + " ".join(hex(ord(i)) for i in requests[i])

            # #print "Request(s): ", requests

            replyAmount = len(requests)
            x = replyAmount

            headers = [] * replyAmount
            messages = [] * replyAmount

            # #print "Reply amount: ", x

            x -= 1

            # #print "Going into while with first element: " + requests[0]

            while x >= 0:
                # #print "Working on number " + str(x) + ": " + " ".join(hex(ord(i)) for i in requests[i])
                if len(requests[x]) > 16:
                    headers.append(requests[x][:16])
                    messages.append(requests[x][16:])

                elif len(requests[x]) == 16:
                    headers.append(requests[x][:16])
                    messages.append([])
                    # return headers, ''
                x -= 1

            return replyAmount, headers, messages

        else:
            return 0, [], []

    def sendData(self, request=''):

        if len(request) > 0:
            self.comms.send(request)
            time.sleep(0.25)

    def readDataRaw(self, request='', Debug_Mode=2):

        # self.testForEvents()                # First check for any pending events received

        tries = self.retries

        while tries > 0:
            try:
                if Debug_Mode >= 2:
                    logging.debug(str(len(request)) + "->   " + " ".join(hex(ord(i)) for i in request))
                #if len(request) == 0: # heartbeart
                #    logging.info("Publishing heartbeat event")
                #    client.publish(Topic_Publish_Heartbeat,"ON")

                self.sendData(request)
                inc_data = self.comms.recv(1024)
                if Debug_Mode >= 2:
                    logging.debug( str(len(inc_data)) + "<-   " + " ".join(hex(ord(i)) for i in inc_data))
                tries = 0

            except socket.timeout, e:
                err = e.args[0]
                if err == 'timed out':
                    #logging.error("Timed out error, no retry -<-- could fix this" + repr(e))
                    #this seems to be where it goes normally while waiting for traffic.
                    tries = 0
                    sys.exc_clear()
                    return ''
                    # sleep(1)
                    # #print 'Receive timed out, ret'
                    # continue
                else:
                    logging.error("Error reading data from IP module, retrying again... (" + str(tries) + "): " + repr(e))
                    tries -= 1
                    time.sleep(Error_Delay)
                    sys.exc_clear()
                    pass
            except socket.error, e:
                logging.error("Unknown error on socket connection, retrying (%d) ... %s " % (tries, repr(e)))
                tries -= 1
                time.sleep(Error_Delay)
                if tries == 0:
                    logging.info("Failure, disconnected.")
                    sys.exit(1)
                else:
                    logging.error("After error, continuing %d attempts left" % tries)
                    sys.exc_clear()
                    return ''
            else:
                if len(inc_data) == 0:
                    tries -= 1
                    logging.info('Socket connection closed by remote host: %d' % tries)
                    time.sleep(Error_Delay)
                    if tries == 0:
                        logging.error('Failure, disconnecting')
                        sys.exit(0)
                else:
                    return inc_data

    def readDataStruct37(self, inputData='', Debug_Mode=0):  # Sends data, read input data and return the Header and Message

        rawdata = self.readDataRaw(inputData, Debug_Mode)

        # Extract the header and message
        if len(rawdata) > 16:
            header = rawdata[:16]
            message = rawdata[17:]

        return header, message

    def controlGenericOutput(self, mapping_dict, output, state, Debug_Mode=0):

        registers = mapping_dict

        header = registers["Header"]

        if Debug_Mode >= 1:
            logging.debug( "Sending generic Output Control: Output: " + str(output) + ", State: " + state)

        message = registers[output][state]

        assert isinstance(message, basestring), "Message to be sent is not a string: %r" % message
        message = message.ljust(36, '\x00')

        # #print " ".join(hex(ord(i)) for i in message)

        reply = self.readDataRaw(header + self.format37ByteMessage(message), Debug_Mode)

        return

    def controlPGM(self, pgm, state="OFF", Debug_Mode=0):

        # #print state.upper()

        assert (isinstance(pgm, int) and pgm >= 0 and pgm <= 16), "Problem with PGM number: %r" % str(pgm)
        assert (isinstance(pgm, int) and pgm >= 0 and pgm <= 16), "Problem with PGM number: %r" % str(pgm)
        assert isinstance(state, basestring), "State given is not a string: %r" % str(state)
        assert (state.upper() == "ON" or state.upper() == "OFF"), "State is not given correctly: %r" % str(state)

        self.controlGenericOutput(self.registermap.getcontrolOutputRegister(), pgm, state.upper(), Debug_Mode)

        return

    def controlGenericAlarm(self, mapping_dict, partition, state, Debug_Mode):
        registers = mapping_dict

        header = registers["Header"]

        logging.info("Sending generic Alarm Control: Partition: " + str(partition) + ", State: " + state)

        message = registers[partition][state]

        assert isinstance(message, basestring), "Message to be sent is not a string: %r" % message
        message = message.ljust(36, '\x00')

        # #print " ".join(hex(ord(i)) for i in message)

        reply = self.readDataRaw(header + self.format37ByteMessage(message), Debug_Mode)

        return

    def controlAlarm(self, partition=1, state="Disarm", Debug_Mode=0):

        assert (
            isinstance(partition,
                       int) and partition >= 0 and partition <= 16), "Problem with partition number: %r" % str(
            partition)
        assert isinstance(state, basestring), "State given is not a string: %r" % str(state)
        assert (state.upper() in self.registermap.getcontrolAlarmRegister()[
            partition]), "State is not given correctly: %r" % str(state)

        self.controlGenericAlarm(self.registermap.getcontrolAlarmRegister(), partition, state.upper(), Debug_Mode)

        return

    def disconnect(self, Debug_Mode=2):

        # header = "\xaa\x00\x00\x03\x51\xff\x00\x0e\x00\x01\xee\xee\xee\xee\xee\xee"
        header = "\xaa\x25\x00\x04\x08\x00\x00\x14\xee\xee\xee\xee\xee\xee\xee\xee"
        message = "\x70\x00\x05"

        self.readDataRaw(header + self.format37ByteMessage(message), Debug_Mode)

    def keepAliveStatus0(self, data, Debug_Mode,keepalivecount=keepalivecount):
        #Panel Status 0 - troubles, voltage, zone status
        paneldatetime = "{}-{}-{} {}:{}".format(ord(data[9])*100 + ord(data[10]),
                        "{0:02d}".format(ord(data[11])),
                        "{0:02d}".format(ord(data[12])),
                        "{0:02d}".format(ord(data[13])),
                        "{0:02d}".format(ord(data[14])))
        #print "dateTime: {}".format(paneldatetime)
        vdc = round(ord(data[15])*(20.3-1.4)/255.0+1.4,1)
        #vdc = ord(data[15])
        if Debug_Mode > 1:
            logging.debug("VDC: {}".format(vdc) )
        dc = round(ord(data[16])*22.8/255.0,1)
        if Debug_Mode > 1:
            logging.debug("DC: {}".format(dc))
        battery = round(ord(data[17])*22.8/255.0,1)
        if Debug_Mode > 1:
            logging.debug("battery: {}".format(battery))

        jsondata = json.dumps({"paneldate":paneldatetime,"vdc":vdc,"dc":dc,"battery":battery})
        if self.keepalivecount % Publish_Status_Factor == 0:
            if Debug_Mode >= 1:
                logging.info("Publishing panel status json: '{}'".format(jsondata))
            self.client.publish(Topic_Publish_Status + "/{0}".format(self.aliveSeq),jsondata)

        self.client.publish(Topic_Publish_Heartbeat,"ON")
        

        bit = 0
        zonebits = data[19:23]
        #Tertuish Method
        for x in range(4):
            b = ord(zonebits[x])
            for y in range(8):
                bit = b & 1
                b = b / 2
                itemNo = x * 8 + y + 1
                zoneState = ZonesOn if bit else ZonesOff
                if itemNo in self.zoneNames.keys():
                    location = self.zoneNames[itemNo]
                    if len(location) > 0:
                        if Debug_Mode > 1:
                            logging.debug("Publishing initial zone state (state: {}, zone number: {} Zone {})".format( zoneState,itemNo,location))
                        if self.keepalivecount % Publish_Status_Factor == 0:
                            self.client.publish(Topic_Publish_ZoneState + "/" + location, ZonesOn if bit else ZonesOff, qos=1, retain=True)

    def keepAliveStatus1(self, data, Debug_Mode,keepalivecount=keepalivecount):
        #Panel Status 1 - Partition Status 
        # Get Partition 1 Status
        self.keepAlivePartitionStatus(data[17:21],1,Debug_Mode)

        # Get Partition 2 Status
        self.keepAlivePartitionStatus(data[21:25],2,Debug_Mode)
        

    def keepAlivePartitionStatus(self, data, partition, Debug_Mode=0):
        #Panel Status 1 - Partition Status 
        partition1status1 = ord(data[0])
        
        armstate = "DISARMED"
        for y in range(8):
            bit = partition1status1 & 1
            partition1status1 = partition1status1 / 2
            itemNo = y + 1
            zoneState = ZonesOn if bit else ZonesOff
            if Debug_Mode > 1:
                #print "Publishing paritions status 1 bits state (state: {}, bit: {})".format( zoneState, itemNo)
                logging.debug("Publishing paritions status 1 bits state (state: {}, bit: {})".format( zoneState, itemNo))
            if itemNo == 1:
                #alarm disarmed
                if Debug_Mode > 1:
                    logging.debug("Publishing Partition Arm state (state: {}, bit: {})".format( zoneState, itemNo))
                if zoneState == ZonesOn:
                    armstate = "ARMED"
            elif itemNo == 2: # sleep
                if zoneState == ZonesOn:
                    armstate = "SLEEP"
            elif itemNo == 3: #away
                if zoneState == ZonesOn:
                    armstate = "STAY"
        
            #client.publish(Topic_Publish_ZoneState + "/" + location, "ON" if bit else "OFF", qos=1, retain=True)
            #client.publish(Topic_Publish_ZoneState + "/" + location, "ON" if bit else "OFF", qos=1, retain=True)
        
        partition1status2 = ord(data[1])
        for y in range(8):
            bit = partition1status2 & 1
            partition1status2 = partition1status2 / 2
            itemNo = y + 1
            zoneState = ZonesOn if bit else ZonesOff
            
            if Debug_Mode >= 2:
                #print "Publishing paritions status 2 bits state (state: {}, bit: {})".format( zoneState, itemNo)
                logging.debug("Publishing paritions status 2 bits state (state: {}, bit: {})".format( zoneState, itemNo))
            if itemNo == 1:
                if zoneState == ZonesOn:
                    armstate = "ARMING"

        partitionlocation =  ""
        if self.partitions:
            partitionlocation = "/" + self.partitions[partition].strip()
        if self.keepalivecount % Publish_Status_Factor == 0:
            if Debug_Mode >= 1:
                logging.info("Publishing Partition \"{}\" Arm state (state: {})".format(partitionlocation, armstate))
            self.client.publish(Topic_Publish_ArmState + partitionlocation + "/Status",self.Alarm_Partition_States[armstate],qos=1,retain=True)

        partition1status3 = ord(data[2])
        for y in range(8):
            bit = partition1status3 & 1
            partition1status3 = partition1status3 / 2
            itemNo = y + 1
            zoneState = ZonesOn if bit else ZonesOff
            
            if Debug_Mode >= 2:
                #print "Publishing paritions status 3 bits state (state: {}, bit: {})".format( zoneState, itemNo)
                logging.debug("Publishing paritions status 3 bits state (state: {}, bit: {})".format( zoneState, itemNo))
        partition1status4 = ord(data[3])
        for y in range(8):
            bit = partition1status4 & 1
            partition1status4 = partition1status4 / 2
            itemNo = y + 1
            zoneState = ZonesOn if bit else ZonesOff
            if Debug_Mode >= 2:
                #print "Publishing paritions status 4 bits state (state: {}, bit: {})".format( zoneState, itemNo)
                logging.debug("Publishing paritions status 4 bits state (state: {}, bit: {})".format( zoneState, itemNo))
                #print "partition 1 status: {} {} {} {}".format(partition1status1,partition1status2,partition1status3,partition1status4)

        if Debug_Mode >= 2:
            logging.debug("partition {}:{} status: {} {} {} {}".format(partition,partitionlocation,partition1status1,partition1status2,partition1status3,partition1status4))

    def keepAlive(self, Debug_Mode=0):

        header = "\xaa\x25\x00\x04\x08\x00\x00\x14\xee\xee\xee\xee\xee\xee\xee\xee"

        message = "\x50\x00\x80"

        message += bytes(bytearray([self.aliveSeq]))

        #message += "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
                  #"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xd0\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee"
        message += "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        message = self.format37ByteMessage(message)
        
        #if Debug_Mode >= 2:
        #print "***** SEQUENCE {}".format(self.aliveSeq)


        self.sendData(header + message)
        
        self.aliveSeq += 1
        if self.aliveSeq > 6:
            self.aliveSeq = 0

    def walker(self, ):
        self.zoneTotal = Zone_Amount

        logging.info("Reading (" + str(Zone_Amount) + ") zone names...")

        header = "\xaa\x25\x00\x04\x08\x00\x00\x14\xee\xee\xee\xee\xee\xee\xee\xee"

        for x in range(16, 65535, 32):
            message = "\xe2\x00"
            zone = x
            zone = list(struct.pack("H", zone))
            swop = zone[0]
            zone[0] = zone[1]
            zone[1] = swop

            temp = "".join(zone)
            # #print " ".join(hex(ord(i)) for i in temp)
            message += temp

            message += "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
            # #print " ".join(hex(ord(i)) for i in message)
            reply = self.readDataRaw(header + self.format37ByteMessage(message))

            logging.info(reply)
            # #print " ".join(hex(ord(i)) for i in reply)

            time.sleep(0.3)

        return


if __name__ == '__main__':

    State_Machine = 0
    attempts = 3
    #print "logging to file %s" % LOG_FILE
    speciallogging = False
    interruptCountdown = 0
    interrupt = 0
    #keepalivecount = 0

    while True:

        if speciallogging:
            #print "Special logging after errorsstate %d" % State_Machine
            logging.info("Special logging after errors state %d" % State_Machine)

        # -------------- Read Config file ----------------
        if State_Machine <= 0:
            print "reading  config"
            #logging.info("Reading config.ini file...")
            #logger.info("Reading config.ini file...")

            try:

                Config = ConfigParser.ConfigParser()
                Config.read(["config.ini","conf/config.ini"])
                LOG_FILE = Config.get("Application","Log_File")
                #log_handler = logging.handlers.TimedRotatingFileHandler(LOG_FILE,when="D",interval=1,backupCount=5)
                if LOG_FILE.startswith('stream://'):
                   LOG_FILE = LOG_FILE.replace('stream://', '')
                   #log_handler.handlers.StreamHandler(stream=eval(LOG_FILE))
                   log_handler = logging.StreamHandler(stream=eval(LOG_FILE))
                else:
                   log_handler = logging.handlers.TimedRotatingFileHandler(LOG_FILE,when="D",interval=1,backupCount=5, encoding=None)
                print "finished setting log"
                formatter = logging.Formatter(LOG_FORMAT)
                log_handler.setLevel(logging.DEBUG)
                log_handler.setFormatter(formatter)
                logging.info("logging complete")
                logging.debug("logging complete")

                #log_handler2 writes to console (and syslog)
                #log_handler2 = logging.StreamHandler()
                #log_handler2.setLevel(logging.DEBUG)
                logger.setLevel(logging.DEBUG)
                #log_handler2.setFormatter(formatter)
                #logger.addHandler(log_handler2)
                logger.addHandler(log_handler)
                logger.info("logging complete")
                if os.path.isfile(LOG_FILE) :
                    logging.info("Rolling old log over")
                    try:
                       logger.addhandler[1].doRollover()
                    except:
                       pass


                Alarm_Model = Config.get("Alarm", "Alarm_Model")
                Alarm_Registry_Map = Config.get("Alarm", "Alarm_Registry_Map")
                Alarm_Event_Map = Config.get("Alarm", "Alarm_Event_Map")
                Zone_Amount = int(Config.get("Alarm", "Zone_Amount"))
                if Zone_Amount % 2 != 0:
                    Zone_Amount += 1
                passw = Config.get("IP150", "Password")
                user = Config.get("IP150", "Pincode")
                IP150_IP = Config.get("IP150", "IP")
                IP150_Port = int(Config.get("IP150", "IP_Software_Port"))
                MQTT_IP = Config.get("MQTT Broker", "IP")
                MQTT_Port = int(Config.get("MQTT Broker", "Port"))
                mqtt_username = Config.get("MQTT Broker", "Mqtt_Username")
                mqtt_password = Config.get("MQTT Broker", "Mqtt_Password")

                Topic_Publish_Events = Config.get("MQTT Topics", "Topic_Publish_Events")
                Events_Payload_Numeric = int(Config.get("MQTT Topics", "Events_Payload_Numeric"))
                Topic_Subscribe_Control = Config.get("MQTT Topics", "Topic_Subscribe_Control")
                Startup_Publish_All_Info = Config.get("MQTT Topics", "Startup_Publish_All_Info")
                Topic_Publish_Labels = Config.get("MQTT Topics", "Topic_Publish_Labels")
                Topic_Publish_AppState = Config.get("MQTT Topics", "Topic_Publish_AppState")
                Startup_Update_All_Labels = Config.get("Application", "Startup_Update_All_Labels")
                Topic_Publish_ZoneState = Config.get("MQTT Topics", "Topic_Publish_ZoneState")
                Topic_Publish_ArmState = Config.get("MQTT Topics", "Topic_Publish_ArmState")
                Topic_Publish_Heartbeat  = Config.get("MQTT Topics", "Topic_Publish_Heartbeat")
                Publish_Status_Factor = int(Config.get("MQTT Topics","Publish_Status_Factor"))
                Topic_Publish_Status  = Config.get("MQTT Topics", "Topic_Publish_Status")
                Publish_Static_Topic = Config.get("MQTT Topics", "Publish_Static_Topic")
                try:
                    Publish_Zones_OpenClosed = int(Config.get("MQTT Topics", "Publish_Zones_OpenClosed"))
                except:
                    Publish_Zones_OpenClosed = 0
                if Publish_Zones_OpenClosed == 1:
                    ZonesOff = "CLOSED"
                    ZonesOn = "OPEN"

                Debug_Mode = int(Config.get("Application", "Debug_Mode"))
                Auto_Logoff = Config.get("Application", "Auto_Logoff")
                Logoff_Delay = int(Config.get("Application", "Logoff_Delay"))

                if Debug_Mode > 0:
                   logging.info("Setting loglevel to debug")
                   logging.debug("Logging Set to debug")
                   logging.info("logging set to debug") 

                logging.info("config.ini file read successfully: %d" % Debug_Mode)
                #print "config read"
                State_Machine += 1

            except Exception, e:
                logging.error("******************* Error reading config.ini file (will use defaults): %s" % e)
                State_Machine = 1
                attempts = 3
        # -------------- MQTT ----------------
        elif State_Machine == 1:

            try:

                if speciallogging:
                    logging.info("State machine 1: starting client again")

                logging.info("State01:Attempting connection to MQTT Broker: " + MQTT_IP + ":" + str(MQTT_Port))
                client = mqtt.Client("paradoxip")

                if mqtt_password == '':
                    mqtt_password = None

                if mqtt_username != '':
                    client.username_pw_set(mqtt_username, mqtt_password)



                client.on_connect = on_connect
                client.on_message = on_message

                client.will_set(Topic_Publish_Heartbeat,"OFF",qos=0,retain=False)
                client.connect(MQTT_IP, MQTT_Port, MQTT_KeepAlive)

                client.publish(Topic_Publish_AppState + "/$version",version,retain=True)

                client.loop_start()

                #client.subscribe(Topic_Subscribe_Control + "#")

                logging.info("State01:MQTT client subscribed to control messages on topic: " + Topic_Subscribe_Control + "#")

                client.publish(Topic_Publish_AppState,"State Machine 1, Connected to MQTT Broker",1,True)

                State_Machine += 1

            except Exception, e:

                logging.error( "MQTT connection error (" + str(attempts) + ": " + str(e))
                time.sleep(Poll_Speed * 5)
                attempts -= 1

                if attempts < 1:
                    logging.error( "State01:Error within State_Machine: {0}: {1}".format(State_Machine,e))
                    State_Machine -= 1
                    logging.error( "State01:Going to State_Machine: " + str(State_Machine))
                    attempts = 3

        # -------------- Login to IP Module ----------------
        elif State_Machine == 2 and Polling_Enabled == 1:

            try:
                if speciallogging:
                    logging.info("State machine 2 + polling: starting calarm communication again")

                logging.info("State02:Connecting to IP Module")
                client.publish(Topic_Publish_AppState, "State Machine 2, Connecting to IP Module...", 1, True)

                comms = connect_ip150socket(IP150_IP, IP150_Port)
                client.publish(Topic_Publish_AppState,
                               "State Machine 2, Connected to IP Module, unlocking...",
                               1, True)

                myAlarm = paradox(comms, client,0, 3, Alarm_Event_Map, Alarm_Registry_Map)

                #loading partition states from config so user can customise.
                if Config.has_section('Alarm_Partition_States'):
                    options_states = Config.options('Alarm_Partition_States')
                    for option in options_states:
                        myAlarm.Alarm_Partition_States[option.upper()] = Config.get('Alarm_Partition_States',option)

                if not myAlarm.login(passw,user, Debug_Mode):
                    logging.info("State02:Failed to login & unlock to IP module, check if another app is using the port. Retrying... ")
                    client.publish(Topic_Publish_AppState,
                                   "State Machine 2, Failed to login & unlock to IP module, check if another app is using the port. Retrying... ",
                                   1, True)
                    comms.close()
                    time.sleep(Poll_Speed * 20)
                else:
                    client.publish(Topic_Publish_AppState, "State Machine 2, Logged into IP Module successfully", 1, True)
                    logging.info("State02: Logged into IP modeule successfully")
                    State_Machine += 1
                    speciallogging = False

            except Exception, e:

                logging.error( "State02:Error attempting connection to IP module ({0}): {1}".format(attempts, e))
                client.publish(Topic_Publish_AppState,
                               "State Machine 2, Exception, retrying... ({0}): {1}".format(attempts, e),1, True)
                time.sleep(Poll_Speed * 5)
                attempts -= 1

                if attempts < 1:
                    logging.error("State02:Error within State_Machine: " + str(State_Machine) + ": " + repr(e))
                    client.publish(Topic_Publish_AppState, "State Machine 2, Error, moving to previous state", 1, True)
                    State_Machine -= 1
                    logging.error("Going to State_Machine: " + str(State_Machine))
                    attempts = 3
        # -------------- Reading Labels ----------------
        elif State_Machine == 3 and Polling_Enabled == 1:

            try:
                if speciallogging:
                    logging.info("State03: Polling enabled: Special logging")

                if Startup_Update_All_Labels == "True" and myAlarm.skipLabelUpdate() == 0:
                    logging.info("State03: Preading labels")
                    client.publish(Topic_Publish_AppState, "State Machine 3, Reading labels from alarm", 1, True)

                    logging.info("State03:Updating all labels from alarm")
                    myAlarm.updateAllLabels(Startup_Publish_All_Info, Topic_Publish_Labels, Debug_Mode)

                    logging.info("State03:Updating zone and alarm status")
                    myAlarm.updateZoneAndAlarmStatus(Startup_Publish_All_Info, Debug_Mode)

                    State_Machine += 1
                    logging.info("State03:Listening for events...")
                    client.publish(Topic_Publish_AppState, "State Machine 4, Listening for events...", 1, True)
                else:

                    State_Machine += 1
                    logging.info("State03:Listening for events...")
                    client.publish(Topic_Publish_AppState, "State Machine 4, Listening for events...", 1, True)
            except Exception, e:

                logging.error("State03:Error reading labels: %s " % repr(e))
                client.publish(Topic_Publish_AppState, "State Machine 3, Exception: {0}".format(e), 1, True)
                time.sleep(Poll_Speed * 5)
                attempts -= 1

                if attempts < 1:
                    logging.error("State03:Error within State_Machine: {0}: {1}".format(State_Machine, e))
                    client.publish(Topic_Publish_AppState, "State Machine 3, Error, moving to previous state", 1, True)
                    State_Machine -= 1
                    logging.error("State03:Going to State_Machine: " + str(State_Machine))

            Alarm_Control_Action = 0
            attempts = 3
            # -------------- Checking Events & Actioning Controls ----------------
        elif State_Machine == 4 and Polling_Enabled == 1:

            try:
                if speciallogging:
                    logging.info("State04: Special logging (polling enabled)")

                # Test for new events & publish to broker
                interrupt = myAlarm.testForEvents(Events_Payload_Numeric, Publish_Static_Topic, Debug_Mode)

                if interrupt == 1:
                    interruptCountdown = Logoff_Delay
                    State_Machine = 20
                    interrupt = 0


                # Test for pending Alarm Control
                if Alarm_Control_Action == 1:
                    logging.info("State04: Alarm Control Action: Alarm loging and starting events")
                    myAlarm.login(passw,user)
                    myAlarm.controlAlarm(Alarm_Control_Partition, Alarm_Control_NewState, Debug_Mode)
                    Alarm_Control_Action = 0
                    logging.info("State04:Listening for events...")
                    client.publish(Topic_Publish_AppState, "State Machine 4, Listening for events...", 1, True)

                # Test for pending Force Output Control
                if Output_FControl_Action == 1:
                    logging.info("State04:OutputFControl_Action: Loging and listenting")
                    myAlarm.login(passw,user)
                    myAlarm.controlPGM(Output_FControl_Number, Output_FControl_NewState, Debug_Mode)
                    Output_FControl_Action = 0
                    logging.info("State04:Listening for events...")
                    client.publish(Topic_Publish_AppState, "State Machine 4, Listening for events...", 1, True)

                # Test for pending Pulse Output Control
                if Output_PControl_Action == 1:
                    logging.info("State04:OutputPControl_Action:Loging and listenting")
                    myAlarm.login(passw,user)
                    myAlarm.controlPGM(Output_PControl_Number, Output_PControl_NewState, Debug_Mode)
                    time.sleep(0.5)
                    if Output_PControl_NewState.upper() == "ON":
                        myAlarm.controlPGM(Output_PControl_Number, "OFF", Debug_Mode)
                    else:
                        myAlarm.controlPGM(Output_PControl_Number, "ON", Debug_Mode)

                    Output_PControl_Action = 0
                    logging.info("State04:Listening for events...")
                    client.publish(Topic_Publish_AppState, "State Machine 4, Listening for events...", 1, True)

                time.sleep(1)
                client.loop(.1)

                if Debug_Mode >= 2:
                    logging.info("Calling keepalive " + str(myAlarm.keepalivecount))

                myAlarm.keepAlive(Debug_Mode)
                myAlarm.keepalivecount = myAlarm.keepalivecount + 1
                #myAlarm.keepAlivePAI(Debug_Mode)
                #    keepalivecount = keepalivecount + 1
                #else:
                #logging.debug("Calling modified keep alive")
                #print("Calling modified keep alive")
                #myAlarm.updateZoneAndAlarmStatus("False", Debug_Mode)
                #keepalivecount = 0


            except Exception, e:

                logging.error("State04:Error during normal poll: {0}, Attemp: {1}".format(e.message,attempts))
                client.publish(Topic_Publish_AppState, "State Machine 4, Exception: {0}".format(e.message), 1, True)
                time.sleep(Poll_Speed * 5)
                attempts -= 1
                logging.error("State04:Setting state machine to 1, hoping it will try to login to panal again.")
                State_Machine = 1

                if attempts < 1:
                    logging.error("State04:Error within State_Machine: {0}: {1}".format(str(State_Machine), e.message))
                    State_Machine -= 1
                    logging.error("State04:Going to State_Machine: " + str(State_Machine))
                    client.publish(Topic_Publish_AppState, "State Machine 4, Error, moving to previous state", 1, True)
                    attempts = 3
                logging.error("State04:Passing on the error: " + str(State_Machine))
                speciallogging = True
                continue


        elif Polling_Enabled == 0 and State_Machine <= 4:

            logging.info("State04:Disabling polling & disconnecting from Alarm")
            client.publish(Topic_Publish_AppState, "Polling: Disabled", 1, True)
            comms.close()
            State_Machine = 10

            logging.info("State04:Polling Disabled")

        elif Polling_Enabled == 1 and State_Machine > 4:
            logging.info("Polling enabled false, setting statement 2")
            State_Machine = 2

        elif State_Machine == 10:
            logging.info("State10: Sleep")
            time.sleep(3)

        elif State_Machine == 20:
            logging.info("State20: 3rd Party interrupted")
            myAlarm.disconnect()
            comms.shutdown(1)
            comms.close()

            for x in range(0, interruptCountdown):
                if x % 5 == 0:
                    logging.info("Delay remaining: " + str(interruptCountdown - x) + " seconds")
                time.sleep(1)

            State_Machine = 2


