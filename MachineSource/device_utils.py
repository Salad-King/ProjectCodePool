# Imports the Google Cloud client library
from google.cloud import pubsub_v1
import random


# This class is written to generate random data to be sent to pub sub topic
class Publisher:
    # Instantiates a client
    def __init__(self, project, topic):
        self.publisher = pubsub_v1.PublisherClient()

        # The resource path for the new topic contains the project ID
        # and the topic name.
        self.topic_path = self.publisher.topic_path(
            project, topic)
#        try:
#            topic = self.publisher.create_topic(self.topic_path)
#            print('Topic created: {}'.format(topic))
#        except:
#            print 'Topic Exists, Ignoring creation'

    def get_random_data(self, data_type, length):
        return {
            'INTEGER': str(random.randint(0, 10**int(length))),
            'FLOAT': str(random.randint(0, 10**(int(length)//2))) + '.' + str(random.randint(0, 10**(int(length)//2+1))),
            'STRING': str(random.randint(0, 10**int(length)))
        }.get(data_type)

    def get_pi_data(self):
       ir_reading = str(random.randint(0,1))
       temprature_reading = str(random.randint(31,34))+'.'+str(random.randint(0,99))
       smoke_reading = str(random.randint(200,250))
       return ','.join([ir_reading, temprature_reading, smoke_reading])
  

    def generate_random_formatted_message(self, message_format, message_id):
        format_length = message_format.split(',')
        message = list()

        for each_format in format_length:
            if 'ID' in each_format:
                message.append(str(message_id))
            else:
                data_type, length = each_format.split(':')
                message.append(self.get_random_data(data_type, length))
        return ','.join(message)

    def publish_message(self, topic, message, schema, destination):
        self.publisher.publish(topic=topic,
                               data=message,
                               schema=schema,
                               destination=destination)
