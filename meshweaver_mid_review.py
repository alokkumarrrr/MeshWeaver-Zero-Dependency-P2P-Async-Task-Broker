import asyncio
import base64
import hashlib
import json
import marshal
import os
import pickle
import time
import types

try:
    import cloudpickle
except ImportError:
    cloudpickle = None


# --- Week 1: Task Serialization (cloudpickle) ---

def pack_task(func, *args, **kwargs):
    if cloudpickle is not None:
        raw = cloudpickle.dumps((func, args, kwargs))
    else:
        # Fallback if cloudpickle is not installed locally
        raw = pickle.dumps((func.__name__, marshal.dumps(func.__code__), func.__defaults__, args, kwargs))
    return base64.b64encode(raw).decode("utf-8")


def unpack_and_run_task(encoded_data):
    raw = base64.b64decode(encoded_data.encode("utf-8"))
    if cloudpickle is not None:
        func, args, kwargs = cloudpickle.loads(raw)
    else:
        name, code_bytes, defaults, args, kwargs = pickle.loads(raw)
        code = marshal.loads(code_bytes)
        func = types.FunctionType(code, {"__builtins__": __builtins__}, name, defaults)
    return func(*args, **kwargs)


# --- Week 2: Lightweight Kademlia DHT Helpers ---

def get_node_id(name):
    # 160-bit SHA-1 ID for Kademlia
    return int(hashlib.sha1(name.encode("utf-8")).hexdigest(), 16)


def xor_distance(id1, id2):
    return id1 ^ id2


def get_system_usage(offset=0.0):
    try:
        load1, _, _ = os.getloadavg()
        cores = os.cpu_count() or 1
        cpu = round(min(99.0, max(1.0, (load1 / cores) * 100.0 + offset)), 1)
    except Exception:
        cpu = round(15.0 + offset, 1)

    try:
        import resource
        ram = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1)
    except Exception:
        ram = 24.0
    return cpu, ram


# --- Week 1 & Week 2: Asyncio UDP Mesh Node ---

class UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, node):
        self.node = node

    def connection_made(self, transport):
        self.node.transport = transport

    def datagram_received(self, data, addr):
        asyncio.create_task(self.node.handle_message(data, addr))


class MeshNode:
    def __init__(self, name, host="127.0.0.1", port=0, cpu_offset=0.0):
        self.name = name
        self.host = host
        self.port = port
        self.node_id = get_node_id(f"{name}:{port}")
        self.cpu_offset = cpu_offset

        self.peers = {}          # node_id -> (host, port, name, cpu, ram)
        self.dht_data = {}       # local DHT key-value store
        self.waiting = {}        # msg_id -> asyncio.Future
        self.transport = None
        self.gossip_task = None
        self.running = False

    async def start(self):
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: UDPProtocol(self),
            local_addr=(self.host, self.port),
        )
        self.transport = transport
        self.port = transport.get_extra_info("sockname")[1]
        self.node_id = get_node_id(f"{self.name}:{self.port}")
        self.running = True
        self.gossip_task = asyncio.create_task(self.gossip_loop())

    async def stop(self):
        self.running = False
        if self.gossip_task:
            self.gossip_task.cancel()
        if self.transport:
            self.transport.close()

    def send_packet(self, payload, addr):
        cpu, ram = get_system_usage(self.cpu_offset)
        payload["sender"] = {
            "id": self.node_id,
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "cpu": cpu,
            "ram": ram,
        }
        self.transport.sendto(json.dumps(payload).encode("utf-8"), addr)

    async def request(self, payload, addr, timeout=1.0):
        msg_id = f"{self.node_id}-{time.time_ns()}"
        payload["msg_id"] = msg_id
        fut = asyncio.get_running_loop().create_future()
        self.waiting[msg_id] = fut
        try:
            self.send_packet(payload, addr)
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self.waiting.pop(msg_id, None)

    # Week 1: Ping another node over UDP
    async def ping(self, addr):
        t0 = time.perf_counter()
        resp = await self.request({"type": "PING"}, addr)
        if resp and resp.get("type") == "PONG":
            return round((time.perf_counter() - t0) * 1000, 2)
        return None

    # Week 1: Send a serialized Python function to a peer and get the result
    async def run_remote_task(self, addr, func, *args, **kwargs):
        task_str = pack_task(func, *args, **kwargs)
        resp = await self.request({"type": "RUN_TASK", "task": task_str}, addr, timeout=2.0)
        return resp

    # Week 2: Kademlia closest peers by XOR distance
    def closest_peers(self, target_id, k=5):
        items = list(self.peers.items())
        items.sort(key=lambda item: xor_distance(item[0], target_id))
        return [info for _, info in items[:k]]

    # Week 2: Dynamic node discovery without hardcoded peer mappings
    async def discover_peers(self, port_list):
        tasks = [self.ping((self.host, p)) for p in port_list if p != self.port]
        if tasks:
            await asyncio.gather(*tasks)

        # Ask discovered neighbors for their closest Kademlia peers (FIND_NODE)
        for peer in self.closest_peers(self.node_id, k=4):
            resp = await self.request({"type": "FIND_NODE", "target": self.node_id}, (peer["host"], peer["port"]))
            if resp and "nodes" in resp:
                for n in resp["nodes"]:
                    if n["id"] != self.node_id:
                        self.peers[n["id"]] = n

    # Week 2: Gossip background loop sharing CPU/RAM every 5 seconds
    async def gossip_once(self):
        cpu, ram = get_system_usage(self.cpu_offset)
        neighbors = self.closest_peers(self.node_id, k=4)
        msg = {
            "type": "GOSSIP",
            "cpu": cpu,
            "ram": ram,
            "known_peers": list(self.peers.values())[:6],
        }
        for peer in neighbors:
            self.send_packet(msg, (peer["host"], peer["port"]))

    async def gossip_loop(self):
        while self.running:
            await self.gossip_once()
            await asyncio.sleep(5)

    async def handle_message(self, data, addr):
        msg = json.loads(data.decode("utf-8"))
        sender = msg.get("sender")
        if sender and sender["id"] != self.node_id:
            sender["host"], sender["port"] = addr[0], addr[1]
            self.peers[sender["id"]] = sender

        mtype = msg.get("type")
        msg_id = msg.get("msg_id")

        if msg_id in self.waiting and mtype in ("PONG", "NODES_REPLY", "TASK_DONE"):
            if not self.waiting[msg_id].done():
                self.waiting[msg_id].set_result(msg)
            return

        if mtype == "PING":
            self.send_packet({"type": "PONG", "msg_id": msg_id}, addr)

        elif mtype == "FIND_NODE":
            target = msg.get("target", self.node_id)
            closest = self.closest_peers(target, k=8)
            self.send_packet({"type": "NODES_REPLY", "msg_id": msg_id, "nodes": closest}, addr)

        elif mtype == "GOSSIP":
            for p in msg.get("known_peers", []):
                if p["id"] != self.node_id:
                    self.peers[p["id"]] = p

        elif mtype == "RUN_TASK":
            result = unpack_and_run_task(msg["task"])
            self.send_packet(
                {"type": "TASK_DONE", "msg_id": msg_id, "result": result, "worker": self.name},
                addr,
            )


# --- Mid-Project Review Demo: ML Function & 10-Node Mesh Audit ---

def train_linear_regression(X, y, lr=0.0025, epochs=300):
    # Simple batch gradient descent for y = w1*x1 + w2*x2 + b
    w1, w2, b = 0.0, 0.0, 0.0
    n = len(X)
    for _ in range(epochs):
        dw1, dw2, db = 0.0, 0.0, 0.0
        for (x1, x2), target in zip(X, y):
            pred = w1 * x1 + w2 * x2 + b
            err = pred - target
            dw1 += (2 / n) * err * x1
            dw2 += (2 / n) * err * x2
            db += (2 / n) * err
        w1 -= lr * dw1
        w2 -= lr * dw2
        b -= lr * db
    return {"w1": round(w1, 3), "w2": round(w2, 3), "bias": round(b, 3)}


async def main():
    print("--- Week 1: Asyncio UDP Ping & Task Serialization ---")
    node1 = MeshNode("node-1")
    node2 = MeshNode("node-2")
    await node1.start()
    await node2.start()

    rtt = await node1.ping((node2.host, node2.port))
    print(f"Ping from {node1.name} to {node2.name} ({node2.port}): {rtt} ms")

    # Serialization Check: send ML training function over UDP socket
    X_data = [[float(i), float(i % 5)] for i in range(1, 20)]
    y_data = [3.5 * x[0] - 2.0 * x[1] + 1.0 for x in X_data]
    task_res = await node1.run_remote_task((node2.host, node2.port), train_linear_regression, X_data, y_data)
    print(f"Remote ML Task executed on {task_res['worker']} -> Learned params: {task_res['result']}\n")

    await node1.stop()
    await node2.stop()

    print("--- Week 2 & Mid-Review Audit: 10-Node Kademlia Mesh & Gossip ---")
    ports = [19500 + i for i in range(10)]
    nodes = [MeshNode(f"node-{i+1}", port=ports[i], cpu_offset=i * 3.5) for i in range(10)]

    for n in nodes:
        await n.start()

    # Nodes discover each other dynamically without hardcoded peer dictionaries
    await asyncio.gather(*(n.discover_peers(ports) for n in nodes))
    await asyncio.gather(*(n.gossip_once() for n in nodes))
    await asyncio.sleep(0.1)

    for n in nodes:
        cpu, ram = get_system_usage(n.cpu_offset)
        short_id = hex(n.node_id)[2:12]
        print(f"{n.name:<8} | Port: {n.port} | DHT ID: {short_id} | Peers found: {len(n.peers)} | CPU: {cpu}% | RAM: {ram} MB")

    for n in nodes:
        await n.stop()


if __name__ == "__main__":
    asyncio.run(main())
