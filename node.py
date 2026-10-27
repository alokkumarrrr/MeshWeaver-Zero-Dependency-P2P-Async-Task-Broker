import asyncio
import json

from network import Network
from peer import PeerManager
from scheduler import TaskScheduler
from task import Task
from heartbeat import Heartbeat
from dht import DHT
from gossip import Gossip


class MeshNode:

    def __init__(self, host, port, peer_host=None, peer_port=None):

        self.host = host
        self.port = port

        self.peer_host = peer_host
        self.peer_port = peer_port

        self.network = None
        self.peers = PeerManager()

        self.scheduler = TaskScheduler(
            result_callback=self.handle_task_result
        )

        # Remember which node sent each task
        self.task_sources = {}

        # Heartbeat service
        self.heartbeat = Heartbeat(self)

        # Distributed task registry
        self.dht = DHT()

        # Gossip service
        self.gossip = Gossip(self)

    def handle_task_result(self, result):

        task_id = result["task_id"]

        # Remove completed task from DHT
        self.dht.remove_task(task_id)

        print()
        print("========================================")
        print("          TASK RESULT READY")
        print("========================================")
        print(f"Task ID : {task_id}")
        print(f"Status  : {result['status']}")
        print(f"Message : {result['message']}")
        print("========================================")

        # Find the node that originally sent this task
        source = self.task_sources.get(task_id)

        if source and self.network:

            result_message = json.dumps({
                "type": "TASK_RESULT",
                "task_id": task_id,
                "status": result["status"],
                "message": result["message"]
            })

            self.network.send(
                result_message,
                source[0],
                source[1]
            )

            print(
                f"[RESULT] Sent result -> "
                f"{source[0]}:{source[1]}"
            )

    async def start(self):

        print("========================================")
        print("          MESHWEAVER NODE")
        print("========================================")
        print(f"Node Address : {self.host}:{self.port}")
        print("Status       : ONLINE")
        print("Protocol     : UDP / Asyncio")
        print("========================================")

        loop = asyncio.get_running_loop()

        transport, protocol = await loop.create_datagram_endpoint(
            lambda: NodeProtocol(self),
            local_addr=(self.host, self.port)
        )

        self.network = Network(transport)

        # Start task worker
        asyncio.create_task(
            self.scheduler.worker()
        )

        # Start heartbeat service
        asyncio.create_task(
            self.heartbeat.start()
        )

        # Start gossip service
        asyncio.create_task(
            self.gossip.start()
        )

        await asyncio.sleep(1)

        # Send PING to all known peers
        for host, port in self.peers.get_peers():

            self.network.send(
                "PING",
                host,
                port
            )

        try:

            await asyncio.Future()

        finally:

            transport.close()


class NodeProtocol(asyncio.DatagramProtocol):

    def __init__(self, node):

        self.node = node

    def connection_made(self, transport):

        self.transport = transport

    def datagram_received(self, data, addr):

        message = data.decode()

        # --------------------------------
        # PING
        # --------------------------------

        if message == "PING":

            self.node.network.send(
                "PONG",
                addr[0],
                addr[1]
            )

            print(
                f"[PING] Reply sent -> "
                f"{addr[0]}:{addr[1]}"
            )

        # --------------------------------
        # HEARTBEAT
        # --------------------------------

        elif message == "HEARTBEAT":

            self.node.network.send(
                "HEARTBEAT_ACK",
                addr[0],
                addr[1]
            )

        # --------------------------------
        # HEARTBEAT ACK
        # --------------------------------

        elif message == "HEARTBEAT_ACK":

            pass

        # --------------------------------
        # PONG
        # --------------------------------

        elif message == "PONG":

            print(
                f"[SUCCESS] Peer {addr} is alive!"
            )

        # --------------------------------
        # JSON MESSAGES
        # --------------------------------

        elif message.startswith("{"):

            try:

                task_data = json.loads(message)

            except json.JSONDecodeError:

                print(
                    "[ERROR] Invalid JSON message"
                )

                return

            # --------------------------------
            # GOSSIP MESSAGE
            # --------------------------------

            if task_data.get("type") == "GOSSIP":

                received_peers = task_data.get(
                    "peers",
                    []
                )

                new_peers = 0

                for peer in received_peers:

                    host = peer[0]
                    port = int(peer[1])

                    # Don't add ourselves
                    if (
                        host == self.node.host
                        and port == self.node.port
                    ):
                        continue

                    before = len(
                        self.node.peers.get_peers()
                    )

                    self.node.peers.add_peer(
                        host,
                        port
                    )

                    after = len(
                        self.node.peers.get_peers()
                    )

                    if after > before:
                        new_peers += 1

                if new_peers > 0:

                    print(
                        f"[GOSSIP] Learned "
                        f"{new_peers} new peer(s)"
                    )

            # --------------------------------
            # TASK MESSAGE
            # --------------------------------

            elif task_data.get("type") == "TASK":

                task_id = task_data["task_id"]
                task_name = task_data["name"]
                payload = task_data["payload"]

                # Remember where this task came from
                self.node.task_sources[task_id] = addr

                # Register task in DHT
                self.node.dht.register_task(
                    task_id,
                    self.node.host,
                    self.node.port
                )

                task = Task(
                    name=task_name,
                    payload=payload
                )

                # Preserve original task ID
                task.id = task_id

                print()
                print("========================================")
                print("          TASK RECEIVED")
                print("========================================")
                print(f"Task ID : {task.id}")
                print(f"Task    : {task.name}")
                print(f"Payload : {task.payload}")
                print("========================================")

                # Add task to scheduler
                asyncio.create_task(
                    self.node.scheduler.add_task(task)
                )

            # --------------------------------
            # TASK RESULT MESSAGE
            # --------------------------------

            elif task_data.get("type") == "TASK_RESULT":

                task_id = task_data["task_id"]
                status = task_data["status"]
                message = task_data["message"]

                # Remove completed task from DHT
                self.node.dht.remove_task(task_id)

                print()
                print("========================================")
                print("        TASK RESULT RECEIVED")
                print("========================================")
                print(f"From    : {addr}")
                print(f"Task ID : {task_id}")
                print(f"Status  : {status}")
                print(f"Message : {message}")
                print("========================================")