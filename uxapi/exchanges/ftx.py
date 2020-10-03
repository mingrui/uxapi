import time
import json
import asyncio

import ccxt
import pendulum

from uxapi import register_exchange
from uxapi import UXSymbol
from uxapi import UXPatch
from uxapi import WSHandler
from uxapi.helpers import (
    hmac,
    contract_delivery_time,
)

@register_exchange('ftx')
class Ftx(UXPatch, ccxt.ftx):
    def describe(self):
        return self.deep_extend(super().describe(), {
            'deliveryHourUTC': 3,

            'urls': {
                'wsapi': 'wss://ftx.com/ws',
            },

            'wsapi': {
                'public': {
                    'ticker': 'ticker?{market}',
                    'orderbook': 'orderbook?{market}',
                    'trade': 'trades?{market}'
                },
                'private': {
                    'fills': 'fills',
                    'myorder': 'orders',
                }
            },
        })

    def _fetch_markets(self, params=None):
        markets = super()._fetch_markets(params)
        for market in markets:
            market['contractValue'] = self.safe_float(market['limits']['amount'], 'min')
            if market['future']:
                if len(market['symbol'].split('-')) == 1:
                    # special contract: BERNIE, TRUMP ...
                    continue
                contract_type = market['symbol'].split('-')[1]
                if contract_type.isnumeric():
                    # futures delivery contract
                    delivery_time = pendulum.from_format(contract_type, 'MMDD')
                    delivery_time = delivery_time.add(hours=self.deliveryHourUTC)
                    market['deliveryTime'] = delivery_time.to_iso8601_string()
                    print(market)
        return markets

    def convert_symbol(self, uxsymbol):
        if uxsymbol.market_type == 'futures':
            delivery_time = contract_delivery_time(
                expiration=uxsymbol.contract_expiration,
                delivery_hour=self.deliveryHourUTC)
            return f'{uxsymbol.base}-{delivery_time:%m%d}'
        else:
            return f'{uxsymbol.base}-{uxsymbol.quote}'

    def wshandler(self, topic_set):
        return FtxWSHandler(self, self.urls['wsapi'], topic_set)

    def convert_topic(self, uxtopic):
        exchange_id, market_type, _, extrainfo = uxtopic
        maintype = uxtopic.maintype
        params = {}
        uxsymbol = UXSymbol(exchange_id, market_type, extrainfo)

        if maintype in ['orderbook', 'ticker', 'trades']:
            template = self.wsapi['public'][maintype]
            params['market'] = self.market_id(uxsymbol)
        elif maintype in ['fills', 'myorder']:
            template = self.wsapi['private'][maintype]
        else:
            raise NotImplementedError

        params = template.format(**params)
        return params


class FtxWSHandler(WSHandler):
    @property
    def login_required(self):
        private_types = {'myorder', 'fills'}
        topic_types = {topic.maintype for topic in self.topic_set}
        for p_type in private_types:
            if p_type in topic_types:
                return True
        return False

    def login_command(self, credentials):
        # https://blog.ftx.com/blog/api-authentication/
        ts = int(time.time() * 1000)
        signature_payload = f'{ts}websocket_login'
        signature = hmac(
            bytes(credentials['secret'], 'utf8'),
            bytes(signature_payload, 'utf8'),
            digest='hex'
        )
        return {
            'args': {
                'key': credentials['apiKey'],
                'sign': signature,
                'time': ts,
            },
            'op': 'login',
        }

    def on_login_message(self, msg):
        print('on_login_message', msg)
        if 'pong' in msg:
            self.on_logged_in()
            raise StopIteration
        return msg

    def create_subscribe_task(self):
        topics = {}
        for topic in self.topic_set:
            converted = self.convert_topic(topic)
            channel, market = self._split_params(converted)
            topics[topic] = {'channel': channel, 'market': market}
        self.pre_processors.append(self.on_subscribe_message)
        self.pending_topics = set(topics)
        return self.awaitables.create_task(self.subscribe(topics), 'subscribe')

    def on_connected(self):
        self.pre_processors.append(self.on_error_message)

    def on_error_message(self, msg):
        if 'error' in msg:
            raise RuntimeError(msg)
        return msg

    def subscribe_commands(self, topic_set):
        commands = []
        for topic, params in topic_set.items():
            channel = params.get('channel')
            market = params.get('market')
            command = {
                'op': 'subscribe',
                'channel': channel,
            }
            if market:
                command['market'] = market
            commands.append(command)
        return commands

    def on_subscribe_message(self, msg):
        msg = json.loads(msg)
        if msg.get('event') == 'subscribed':
            topic = msg['channel']
            self.logger.info(f'{msg} subscribed')
            self.on_subscribed(topic)
            raise StopIteration
        else:
            return msg

    def create_keepalive_task(self):
        self.last_message_timestamp = time.time()
        return super().create_keepalive_task()

    async def keepalive(self):
        interval = 15
        while True:
            await asyncio.sleep(interval)
            now = time.time()
            if now - self.last_message_timestamp >= interval:
                await self.send({'op': 'ping'})

    def on_keepalive_message(self, msg):
        self.last_message_timestamp = time.time()
        if msg == 'pong':
            raise StopIteration
        else:
            return msg

    @staticmethod
    def _split_params(topic):
        if '?' in topic:
            return topic.split('?', maxsplit=1)
        else:
            return topic, None
