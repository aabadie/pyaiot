import json
import logging
import asyncio

from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.mqtt.constants import QOS_1

logging.basicConfig(format='%(asctime)s - %(name)14s - '
                           '%(levelname)5s - %(message)s')
logger = logging.getLogger("ttn_client")

TTN_REGION = '<your ttn region>'
APP_ID = '<your app id>'
APP_KEY = '<your app key>'
MQTT_HOST = '{}.thethings.network'.format(TTN_REGION)
MQTT_PORT = '1883'
MQTT_URL = 'mqtt://{}:{}@{}:{}'.format(APP_ID, APP_KEY, MQTT_HOST, MQTT_PORT)

@asyncio.coroutine
def start_client():
    """Connect to MQTT broker and subscribe to node ceck ressource."""
    mqtt_client = MQTTClient()
    yield from mqtt_client.connect(MQTT_URL)
    # Subscribe to 'gateway/check' with QOS=1
    yield from mqtt_client.subscribe([('+/devices/+/up', QOS_1)])
    while True:
        try:
            logger.debug("Waiting for incoming MQTT messages from gateway")
            # Blocked here until a message is received
            message = yield from mqtt_client.deliver_message()
        except ClientException as ce:
            logger.error("Client exception: {}".format(ce))
            break
        except Exception as exc:
            logger.error("General exception: {}".format(exc))
            break
        packet = message.publish_packet
        data = packet.payload.data.decode()
        logger.debug("Received message from gateway => {}".format(data))


if __name__ == '__main__':
    logger.setLevel(logging.DEBUG)
    try:
        asyncio.get_event_loop().run_until_complete(start_client())
    except KeyboardInterrupt:
        logger.info("Exiting")
        asyncio.get_event_loop().stop()
