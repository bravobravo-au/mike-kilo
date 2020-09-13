"""
Copyright (c) 2019, bravobravo-au
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    1. Redistributions of source code must retain the above copyright notice, this
       list of conditions and the following disclaimer.

    2. Redistributions in binary form must reproduce the above copyright notice,
        this list of conditions and the following disclaimer in the documentation
        and/or other materials provided with the distribution.

    3. Neither the name of the copyright holder nor the names of its
        contributors may be used to endorse or promote products derived from
        this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
import paho.mqtt.client as mqtt
from time import strftime, gmtime, sleep
from datetime import datetime
import configparser
import argparse
import json
import csv
import os
import sys
import signal

mqtt_connected = False

def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    if rc == 0:
        mqtt_connected = True 
    else:
        mqtt_connected = False

def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False



buffer = []

parser = argparse.ArgumentParser()
parser.add_argument("--config", help="Configuration file to use default config.ini")
parser.add_argument("--debug", help="print debugging to console")
parser.add_argument("--format", help="Set the output format to JSON or CSV")
args = parser.parse_args()

if args.debug:
    print("Console debug turned on")
    args.debug = True
else:
    args.debug = False

config = configparser.ConfigParser()
if args.config:
    config.read(args.config)
else:
    config.read('config.ini')
    
if args.format:
    outputFormat = args.format
else:
    outputFormat = 'CSV'

mqtt_host                           = config['DEFAULT']['MQTT_HOST']
mqtt_port                           = config['DEFAULT']['MQTT_PORT']
mqtt_client_name                    = config['DEFAULT']['MQTT_CLIENT_NAME']
mqtt_power_topic                    = config['DEFAULT']['MQTT_POWER_TOPIC']
buffer_length                       = config['DEFAULT']['BUFFER_LENGTH']
buffer_filename_prefix              = config['DEFAULT']['BUFFER_FILENAME_PREFIX']
buffer_filename_suffix              = config['DEFAULT']['BUFFER_FILENAME_SUFFIX']
log_dir                             = config['DEFAULT']['LOG_DIR']

if 'MQTT_USERNAME' in config['DEFAULT']:
    mqtt_username                   = config['DEFAULT']['MQTT_USERNAME'] 
    mqtt_password                   = config['DEFAULT']['MQTT_PASSWORD']
else:
    mqtt_username                   = None
    mqtt_password                   = None

def write_buffer_to_log():
    global buffer
    dateStr = datetime.utcnow().strftime('%Y-%m-%d')
    dirPath = log_dir + '/' + datetime.utcnow().strftime('%Y-%m')

    try:
        os.makedirs(dirPath)
    except FileExistsError:
        pass

    filename = dirPath + '/' + buffer_filename_prefix + dateStr + "." + buffer_filename_suffix

    with open(filename, "a+") as logfile:
        for item in buffer:
            if outputFormat == "CSV":
                logfile.write(
				"sensor=%s,DateTime=%s,YesterdayKWH=%s,LifeTimeKWH=%s,PowerFactor=%s,Voltage=%s,Current=%s,ActivePower=%s,ReactivePower=%s,ApparentPower=%s\n" % 
				( 
				item['Sensor'],
                                item['DateTime'],
                                item['YesterdayKWH'],
                                item['LifeTimeKWH'],
                                item['PowerFactor'],
                                item['Voltage'],
                                item['Current'],
                                item['ActivePower'],
                                item['ReactivePower'],
                                item['ApparentPower'],
				)
			)
            if outputFormat == "JSON":
                logfile.write(json.dumps(item) + "\n")
    buffer = []


def on_message(client, userdata, message):
    global buffer

    jsonDecode = json.loads( str(message.payload.decode("utf-8")) )

    if args.debug == True:
        print("%s -- %s" % (message.topic,jsonDecode))

    value = jsonDecode['ENERGY']

    YesterdayKWH = value['Yesterday']
    LifeTimeKWH = value['Total'] 
    PowerFactor = value['Factor']
    Voltage = value['Voltage']
    Current = value['Current']
    ActivePower = value['Power']
    ReactivePower = value['ReactivePower']
    ApparentPower = value['ApparentPower']

    sensorName = message.topic.split('/')
    
    buffer.append({
            'Sensor':           sensorName[1],
            'DateTime':         datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), 
            'YesterdayKWH':     YesterdayKWH,
            'LifeTimeKWH':      LifeTimeKWH,
            'PowerFactor':      PowerFactor,
            'Voltage':          Voltage,
            'Current':         	Current,
            'ActivePower':      ActivePower,
            'ReactivePower':    ReactivePower,
            'ApparentPower':    ApparentPower,
            })

    if len(buffer) >= int(buffer_length):
        write_buffer_to_log()


client = mqtt.Client(
                                client_id=mqtt_client_name, 
                                clean_session=False, 
                                userdata=None, 
                                transport='tcp',
                        )
if mqtt_username is not None:
        client.username_pw_set(username=mqtt_username, password=mqtt_password)

def mqtt_connect():
    try:        
        client.connect(
                mqtt_host, 
                port=int(mqtt_port), 
                keepalive=10, 
                bind_address=""
                        )
    except:
        pass

def terminateProcess(signalNumber, frame):
    print ('(SIGTERM) terminating process')
    write_buffer_to_log()
    sys.exit()

signal.signal(signal.SIGTERM, terminateProcess)

if mqtt_connected == False:
    mqtt_connect()
client.on_message=on_message
client.on_connect=on_connect
client.on_disconnect=on_disconnect

client.subscribe(mqtt_power_topic)
client.loop_forever(timeout=1.0, max_packets=1, retry_first_connection=False)

