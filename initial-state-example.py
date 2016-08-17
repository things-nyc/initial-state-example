from ISStreamer.Streamer import Streamer
import time
import paho.mqtt.client as mqtt
from base64 import b64decode
import json
import struct
import auth

# Streamer connects to initialstate.com
streamer = Streamer(
    bucket_name=auth.is_bucket_name, 
    bucket_key=auth.is_bucket_key, 
    access_key=auth.is_access_key)

def on_mqtt_connect(client, userdata, flags, rc):
    print("Connected to The Things Network (%s)"%str(rc))
    streamer.log("Log Messages", "Connected to TTN")
    streamer.flush()

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("%s/devices/+/up"%(auth.ttn_username))

i = 0
# The callback for when a PUBLISH message is received from the server.
def on_mqtt_message(client, userdata, msg):
  global i
  i += 1
  streamer.log("Log Messages", "Received packet #%d"%i)
  streamer.flush()
  with open('ttn_rx.data','a') as f:
    now = time.time()
    datastr = "%d %s\r\n"%(now,str(msg.payload))
    f.write(datastr)
    f.flush()
    f.close()

  o = json.loads(msg.payload)
  raw_data = b64decode(o['payload'])
  if len(raw_data) != 7:
    print "packet wrong length: ", len(raw_data)
    return
  seq, pwr, temp, humid = struct.unpack('>HBHH',raw_data)
  pwr -= 2**7
  temp -= 2**15
  temp /= 100.0
  humid -= 2**15
  humid /= 100.0
  streamer.log("Sequence Number", seq)
  streamer.log("Temperature", temp)
  streamer.log("Humidity", humid)
  streamer.log("TX Power", pwr)
  print "Sequence Number", seq
  print "Temperature", temp
  print "Humidity", humid
  print "TX Power", pwr
  print "***********************************"
  streamer.flush()

client = mqtt.Client()
client.on_connect = on_mqtt_connect
client.on_message = on_mqtt_message

client.username_pw_set(auth.ttn_username,auth.ttn_password)
client.connect("staging.thethingsnetwork.org", 1883, 60)

streamer.log("Log Messages", "Stream Started")
streamer.flush()

client.loop_forever()
streamer.close()
