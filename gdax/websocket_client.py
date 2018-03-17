# gdax/WebsocketClient.py
# original author: Daniel Paquin
# mongo "support" added by Drew Rice
#
#
# Template object to receive messages from the gdax Websocket Feed

from __future__ import print_function
import alog
import json
import base64
import hmac
import hashlib
import time
import sys
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException


class WebsocketClient(object):
    def __init__(self, url="wss://ws-feed.gdax.com", products=None,
                 message_type="subscribe", mongo_collection=None,
                 should_print=True, auth=False, api_key="", api_secret="",
                 api_passphrase="", channels=None, heartbeat=True,
                 keepalive=15, heartbeat_delay=2):
        self.api_key = api_key
        self.api_passphrase = api_passphrase
        self.api_secret = api_secret
        self.auth = auth
        self.error = None
        self.heartbeat = heartbeat
        self.keepalive = keepalive
        self.mongo_collection = mongo_collection
        self.products = products
        self.should_print = should_print
        self.stop = False
        self.thread = None
        self.type = message_type
        self.url = url
        self.ws = None
        self.heartbeat_delay = heartbeat_delay
        self.last_heartbeat = 0

        self.channels = channels

        if self.channels is None:
            self.channels = []

    def start(self):
        def _go():
            self._connect()
            self._listen()
            self._disconnect()

        self.stop = False
        self.on_open()
        self.thread = Thread(target=_go)
        self.thread.start()

        try:
            while True:
                time.sleep(1)

                now = time.time()
                heartbeat_delay = now - self.last_heartbeat

                alog.debug(f'## last heartbeat: {self.last_heartbeat} '
                           f'diff: {heartbeat_delay} ###')

                if heartbeat_delay >= self.heartbeat_delay \
                        and self.last_heartbeat > 0:
                    self.stop = True
                    alog.debug('## heartbeat exceeded delay ###')

                if self.stop:
                    self.start()
                    break

        except KeyboardInterrupt:
            sys.exit(0)

    def _connect(self):
        if self.products is None:
            self.products = ["BTC-USD"]
        elif not isinstance(self.products, list):
            self.products = [self.products]

        if self.url[-1] == "/":
            self.url = self.url[:-1]

        if self.heartbeat:
            if 'heartbeat' not in self.channels:
                self.channels.append('heartbeat')

        if self.channels is None:
            sub_params = {'type': 'subscribe', 'product_ids': self.products}
        else:
            sub_params = {'type': 'subscribe', 'product_ids': self.products,
                          'channels': self.channels}

        if self.auth:
            timestamp = str(time.time())
            message = timestamp + 'GET' + '/users/self/verify'
            message = message.encode('ascii')
            hmac_key = base64.b64decode(self.api_secret)
            signature = hmac.new(hmac_key, message, hashlib.sha256)
            signature_b64 = base64.b64encode(signature.digest()).decode('utf-8').rstrip('\n')
            sub_params['signature'] = signature_b64
            sub_params['key'] = self.api_key
            sub_params['passphrase'] = self.api_passphrase
            sub_params['timestamp'] = timestamp

        self.ws = create_connection(self.url)

        print(sub_params)

        self.ws.send(json.dumps(sub_params))

    def _listen(self):
        start_t = time.time()

        while not self.stop:
            try:
                if time.time() - start_t >= self.keepalive:
                    # Set a 30 second ping to keep connection alive
                    self.ws.ping("keepalive")
                    start_t = time.time()

                data = self.ws.recv()
                msg = json.loads(data)



            except ValueError as e:
                self.on_error(e)
            except Exception as e:
                self.on_error(e)
            else:
                if msg.get('type', None) == 'heartbeat':
                    self.check_heartbeat(msg)
                else:
                    self.on_message(msg)

    def _disconnect(self):
        self.ws.send(json.dumps({
            "type": "unsubscribe",
            "channels": self.channels
        }))

        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass

        self.on_close()

    def close(self):
        self.stop = True

    def on_open(self):
        if self.should_print:
            print("-- Subscribed! --\n")

    def on_close(self):
        if self.should_print:
            print("\n-- Socket Closed --")

    def check_heartbeat(self, msg):
        now = time.time()
        self.last_heartbeat = now

    def on_message(self, msg):
        if self.should_print:
            print(msg)
        if self.mongo_collection:  # dump JSON to given mongo collection
            self.mongo_collection.insert_one(msg)

    def on_error(self, e, data=None):
        self.stop = True
        self.error = e
        alog.error('{} - data: {}'.format(e, data))
        self.close()


class HeartbeatDelayExceededException(Exception):
    pass


if __name__ == "__main__":
    import sys
    import gdax

    class MyWebsocketClient(gdax.WebsocketClient):
        def on_open(self):
            self.url = "wss://ws-feed.gdax.com/"
            self.products = ["BTC-USD"]
            self.message_count = 0
            print("Let's count the messages!")

        def on_message(self, msg):
            if msg.get('type', None) == 'heartbeat':
                print(json.dumps(msg, indent=4, sort_keys=True))
            self.message_count += 1

        def on_close(self):
            print("-- Goodbye! --")

    wsClient = MyWebsocketClient()
    print(wsClient.url, wsClient.products)
    wsClient.start()
