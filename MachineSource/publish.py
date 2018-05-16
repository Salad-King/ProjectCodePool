import spidev
import time
import os
import RPi.GPIO as GPIO
from device_utils import Publisher

# Google cloud Pubsub connection
publisher = Publisher(project='serene-circlet-189907', topic='StreamingTopic')

# Define input pins for IR sensor
read_pin = 21

# Open connection for IR
GPIO.setmode(GPIO.BCM)
GPIO.setup(read_pin, GPIO.IN)

# Open SPI bus
spi = spidev.SpiDev()
spi.open(0,1)
spi.max_speed_hz=1000000
# Function to read SPI data from MCP3008 chip
# Channel must be an integer 0-7
def ReadChannel(channel):
  adc = spi.xfer2([1,(8+channel)<<4,0])
  data = ((adc[1]&3) << 8) + adc[2]
  return data

# Function to convert data to voltage level,
# rounded to specified number of decimal places. 
def ConvertVolts(data,places):
  volts = (data * 3.3) / float(1023)
  volts = round(volts,places)  
  return volts
  
# Function to calculate temperature from
# TMP36 data, rounded to specified
# number of decimal places.
def ConvertTemp(data,places):
  temp = ((data * 330)/float(1023))-50
  temp = round(temp,places)
  return temp

# Define sensor channels
light_channel = 0
temp_channel  = 1

# Define delay between readings
delay = 1

# A way to store and retrive the count of the messages published so far to
#resume te algorithm for unique and sequential message id generation
count = 0
seq_file_name = 'seq_num'
if not os.path.isfile(seq_file_name):
    with open(seq_file_name, 'w+') as f:
         f.write(str(count))
else:
    with open(seq_file_name, 'r') as f:
        count = int(f.read())


while True:
  # Read the light sensor data
  light_level = ReadChannel(light_channel)
  light_volts = ConvertVolts(light_level,2)

  # Read the temperature sensor data
  temp_level = ReadChannel(temp_channel)
  temp_volts = ConvertVolts(temp_level,2)
  temp       = ConvertTemp(temp_level,2)

  smoke = light_level
  temp = temp
  ir = GPIO.input(21)

  # Print out results
  print "--------------------------------------------"  
  print("Smoke : {} ({}V)".format(light_level,light_volts))  
  print("Temp  : {} ({}V) {} deg C".format(temp_level,temp_volts,temp))    
  print("IR detection: {}".format(GPIO.input(21)))
  
  count += 1
  message = "{id},{smoke},{temp},{infra_red}".format(id=count,smoke=smoke, temp=temp, infra_red=ir)
  print('Publishing data: {}'.format(message))
  publisher.publish_message(topic='projects/serene-circlet-189907/topics/StreamingTopic'
                              , message=message
                              , schema='ID:INTEGER,smoke:INTEGER,temp:FLOAT,infra_red:INTEGER'
                              , destination='Streaming.machine_1')
  with open(seq_file_name, 'w+') as f:
     f.write(str(count))
  # Wait before repeating loop
  time.sleep(delay)
 


