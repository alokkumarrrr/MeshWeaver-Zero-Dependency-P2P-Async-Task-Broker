import asyncio
import sys
import json

from node import MeshNode
from task import Task


async def main():

    port = 9001
    peer_ports = []

    if len(sys.argv) > 1:
        port = int(sys.argv[1])

    if len(sys.argv) > 2:
        peer_ports = [
            int(value)
            for value in sys.argv[2:]
        ]

    node = MeshNode(
        "127.0.0.1",
        port
    )

    # Add peers
    for peer_port in peer_ports:

        node.peers.add_peer(
            "127.0.0.1",
            peer_port
        )

    # Start node
    node_task = asyncio.create_task(
        node.start()
    )

    # Wait for the network to become ready
    for _ in range(20):

        if node.network is not None:
            break

        await asyncio.sleep(0.1)

    # Make sure node started successfully
    if node.network is None:

        print(
            "[ERROR] Node failed to start."
        )

        return

    # Create task
    task = Task(
        name="remote_demo_task",
        payload={
            "message": f"Hello from Node {port}"
        }
    )

    print()
    print("========================================")
    print("          NEW TASK")
    print("========================================")
    print(f"Task ID : {task.id}")
    print(f"Task    : {task.name}")
    print(f"Payload : {task.payload}")
    print("========================================")

    # Send task to the first peer
    if peer_ports:

        peer_port = peer_ports[0]

        message = json.dumps({
            "type": "TASK",
            "task_id": task.id,
            "name": task.name,
            "payload": task.payload["message"]
        })

        node.network.send(
            message,
            "127.0.0.1",
            peer_port
        )

        print(
            f"[TASK] Sent task to "
            f"127.0.0.1:{peer_port}"
        )

    else:

        # If no peer exists, run task locally
        await node.scheduler.add_task(task)

    # Keep node running
    await node_task


if __name__ == "__main__":
    asyncio.run(main())