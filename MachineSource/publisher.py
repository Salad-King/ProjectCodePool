'''
This file demonstrates how-to use the device_utils to generate or send data to pub sub topic
feel free to add your own data to it
'''

from device_utils import Publisher
from time import sleep
publisher = Publisher(project='serene-circlet-189907', topic='StreamingTopic')

for index in range(0,100):
   message = publisher.get_pi_data()
   print message

'''
    message = publisher.generate_random_formatted_message(message_format='ID,INTEGER:3,FLOAT:5,INTEGER:4,INTEGER:5',
                                                          message_id=index)
    print('Publishing message:' + message)
    publisher.publish_message(topic='projects/serene-circlet-189907/topics/StreamingTopic'
                              , message=message
                              , schema='ID:INTEGER,sensor_1:STRING,sensor_2:STRING,sensor_3:STRING,sensor_4:STRING'
                              , destination='Streaming.RaspberryPiData')

'''
