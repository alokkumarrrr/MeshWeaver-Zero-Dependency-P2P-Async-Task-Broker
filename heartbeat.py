import asyncio


class Heartbeat:

    def __init__(self, node):

        self.node = node
        self.running = False
        self.interval = 5

    async def start(self):

        self.running = True

        print("[HEARTBEAT] Service started")

        while self.running:

            for host, port in self.node.peers.get_peers():

                if self.node.network:

                    self.node.network.send(
                        "HEARTBEAT",
                        host,
                        port
                    )

            await asyncio.sleep(
                self.interval
            )