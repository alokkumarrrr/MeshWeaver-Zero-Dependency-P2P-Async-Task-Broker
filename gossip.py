import asyncio
import json


class Gossip:

    def __init__(self, node):

        self.node = node
        self.running = False
        self.interval = 10

    def broadcast_peers(self):

        peers = self.node.peers.get_peers()

        if not peers:
            return None

        return {
            "type": "GOSSIP",
            "peers": peers
        }

    def send_to_peer(self, host, port):

        message = self.broadcast_peers()

        if not message:
            return

        data = json.dumps(message)

        self.node.network.send(
            data,
            host,
            port
        )

    async def start(self):

        self.running = True

        print("[GOSSIP] Service started")

        while self.running:

            for host, port in self.node.peers.get_peers():

                if self.node.network:

                    self.send_to_peer(
                        host,
                        port
                    )

            await asyncio.sleep(
                self.interval
            )