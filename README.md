# MeshWeaver

## Zero-Dependency P2P Async Task Broker

MeshWeaver is a lightweight peer-to-peer asynchronous task broker built entirely with Python's standard library.

It demonstrates how multiple independent nodes can communicate over UDP, discover peers dynamically, distribute tasks, execute them asynchronously, and return task results across the network.

---

## 🚀 Project Overview

Traditional task-processing systems commonly depend on a central server or external message broker.

MeshWeaver explores a different approach:

> **Every node can communicate directly with other nodes and participate in task processing.**

Each MeshWeaver node contains:

* UDP networking
* Peer management
* Asynchronous task scheduling
* Background worker
* Heartbeat monitoring
* Gossip-based peer discovery
* Distributed task registry
* Task result handling

The project uses **zero external Python dependencies**.

---

## ✨ Key Features

### 1. Peer-to-Peer Communication

Nodes communicate directly using UDP datagrams.

```text
Node A  ───────── UDP ─────────>  Node B
```

No central broker is required for basic node-to-node communication.

---

### 2. Asynchronous Task Processing

Tasks are placed into an asynchronous queue and processed by a worker.

```text
Task
 ↓
Scheduler
 ↓
Async Queue
 ↓
Worker
 ↓
Completed Result
```

The scheduler uses Python's `asyncio` framework.

---

### 3. Remote Task Execution

A node can send a task to another node.

Example:

```text
Node 9001
   │
   │ TASK
   ▼
Node 9002
   │
   │ Execute
   ▼
Task Completed
   │
   │ TASK_RESULT
   ▼
Node 9001
```

---

### 4. Heartbeat Monitoring

Nodes periodically send heartbeat messages to known peers.

```text
Node A ── HEARTBEAT ──> Node B
Node A <─ HEARTBEAT_ACK ─ Node B
```

This provides a simple mechanism for checking whether a peer is reachable.

---

### 5. Gossip-Based Peer Discovery

Nodes exchange their known peer information using a lightweight gossip mechanism.

For example:

```text
Node 9003
   │
   │ knows 9002
   ▼
Node 9002
   │
   │ knows 9001
   ▼
Node 9003 learns about 9001
```

During the 3-node integration test, Node 9003 initially knew Node 9002 and later discovered Node 9001 through gossip.

---

### 6. Distributed Task Registry

MeshWeaver includes a lightweight DHT-style task registry.

The registry records where an active task is being processed.

```text
Task ID
   │
   ▼
Node Address
```

When the task is completed, its entry is removed from the registry.

---

### 7. Task Result Propagation

After a worker completes a task, the result is sent back to the node that originally submitted the task.

Example:

```text
9003
 │
 │ TASK
 ▼
9002
 │
 │ PROCESS
 ▼
9002
 │
 │ TASK_RESULT
 ▼
9003
```

---

## 🏗️ Architecture

```text
                         MeshWeaver Network

        ┌──────────────────────┐
        │      Node 9001       │
        │                      │
        │  Peer Manager        │
        │  Scheduler           │
        │  Worker              │
        │  Heartbeat           │
        │  Gossip              │
        │  DHT                 │
        └──────────┬───────────┘
                   │
                   │ UDP
                   │
        ┌──────────▼───────────┐
        │      Node 9002       │
        │                      │
        │  Peer Manager        │
        │  Scheduler           │
        │  Worker              │
        │  Heartbeat           │
        │  Gossip              │
        │  DHT                 │
        └──────────┬───────────┘
                   │
                   │ UDP / Gossip
                   │
        ┌──────────▼───────────┐
        │      Node 9003       │
        │                      │
        │  Peer Manager        │
        │  Scheduler           │
        │  Worker              │
        │  Heartbeat           │
        │  Gossip              │
        │  DHT                 │
        └──────────────────────┘
```

Every node runs the same core architecture.

---

## 🔄 Task Lifecycle

A task follows this basic lifecycle:

```text
                 ┌─────────────┐
                 │    TASK     │
                 │   CREATED   │
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │    TASK     │
                 │    SENT     │
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │    TASK     │
                 │   RECEIVED  │
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │ DHT REGISTER│
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │  SCHEDULER  │
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │    WORKER   │
                 │   RUNNING   │
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │  COMPLETED  │
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │ DHT REMOVE  │
                 └──────┬──────┘
                        │
                        ▼
                 ┌─────────────┐
                 │ TASK RESULT │
                 └─────────────┘
```

---

## 📁 Project Structure

```text
MeshWeaver/
│
├── main.py
├── node.py
├── network.py
├── peer.py
├── task.py
├── scheduler.py
├── gossip.py
├── heartbeat.py
├── dht.py
│
└── README.md
```

### File Responsibilities

| File           | Responsibility                                            |
| -------------- | --------------------------------------------------------- |
| `main.py`      | Starts a MeshWeaver node and creates a demonstration task |
| `node.py`      | Core node logic and UDP message handling                  |
| `network.py`   | Sends UDP messages                                        |
| `peer.py`      | Manages known peers                                       |
| `task.py`      | Defines task objects and task states                      |
| `scheduler.py` | Asynchronous task queue and worker                        |
| `gossip.py`    | Peer information exchange                                 |
| `heartbeat.py` | Periodic peer health checking                             |
| `dht.py`       | Tracks active task locations                              |
| `README.md`    | Project documentation                                     |

---

## 🛠️ Technologies Used

* Python 3
* `asyncio`
* UDP / Datagram Networking
* JSON
* Object-Oriented Programming
* Asynchronous Programming
* Peer-to-Peer Networking
* Gossip Protocol Concept
* Distributed Task Registry Concept

### External Dependencies

**None**

MeshWeaver uses only Python's standard library.

---

## 💻 Requirements

Install:

```text
Python 3.x
```

No external packages are required.

---

## ▶️ How to Run

Open PowerShell or Command Prompt.

Navigate to the project:

```powershell
cd D:\MeshWeaver
```

### Run a single node

```powershell
python main.py 9001
```

The node will execute a task locally.

---

## 🌐 Run Multiple Nodes

### Terminal 1

```powershell
python main.py 9001 9002
```

### Terminal 2

```powershell
python main.py 9002 9001 9003
```

### Terminal 3

```powershell
python main.py 9003 9002
```

This creates a three-node MeshWeaver network.

---

## 🧪 3-Node Integration Test

The project was tested using three independent nodes:

```text
9001
 │
 ├───────────┐
 │           │
 ▼           ▼
9002 <─────> 9003
```

The test demonstrated:

* Peer connections
* PING/PONG communication
* Heartbeat monitoring
* Remote task transmission
* Task scheduling
* Worker execution
* DHT registration
* DHT cleanup
* Task result propagation
* Dynamic gossip-based peer discovery

Example discovery:

```text
Node 9003 initially knows:

9002

After Gossip:

9002
9001
```

Output:

```text
[PEER] Added peer -> 127.0.0.1:9001
[GOSSIP] Learned 1 new peer(s)
```

---

## 📡 Communication Messages

MeshWeaver uses simple UDP messages.

### PING

Checks whether a peer is reachable.

```text
PING
```

### PONG

Response to a PING request.

```text
PONG
```

### HEARTBEAT

Periodic peer health message.

```text
HEARTBEAT
```

### HEARTBEAT_ACK

Heartbeat response.

```text
HEARTBEAT_ACK
```

### TASK

Contains task information.

```json
{
    "type": "TASK",
    "task_id": "example-id",
    "name": "remote_demo_task",
    "payload": "Hello"
}
```

### TASK_RESULT

Returns the execution result.

```json
{
    "type": "TASK_RESULT",
    "task_id": "example-id",
    "status": "COMPLETED",
    "message": "Task completed successfully"
}
```

### GOSSIP

Shares known peer information.

```json
{
    "type": "GOSSIP",
    "peers": [
        ["127.0.0.1", 9001],
        ["127.0.0.1", 9002]
    ]
}
```

---

## 🧠 Main Concepts Demonstrated

MeshWeaver was designed as a practical learning project around distributed systems.

It demonstrates:

### Networking

Direct node-to-node communication using UDP.

### Concurrency

Multiple background services operate using `asyncio`.

### Distributed Processing

Tasks can be sent to another node for execution.

### Peer Discovery

Nodes can learn about additional peers through gossip.

### Fault Awareness

Heartbeat messages provide basic peer reachability information.

### State Tracking

The DHT-style registry tracks active task locations.

### Message-Based Architecture

Nodes communicate through structured messages.

---

## ⚠️ Current Limitations

MeshWeaver is an educational distributed-systems prototype rather than a production-ready task broker.

Current limitations include:

* UDP does not guarantee message delivery.
* No message encryption is implemented.
* No authentication between peers.
* The DHT is a lightweight task registry rather than a full production DHT.
* Task execution is currently simulated.
* There is no persistent task storage.
* There is no advanced failure recovery mechanism.
* Peer membership is maintained in memory.

These limitations provide opportunities for future development.

---

## 🔮 Future Improvements

Possible future versions could include:

* Reliable message delivery
* Task retries
* Task prioritization
* Persistent storage
* Authentication
* Encryption
* Worker load balancing
* Better failure detection
* Task replication
* Persistent distributed storage
* Web-based monitoring dashboard
* Docker-based multi-node deployment
* Real distributed hash-ring implementation

---

## 🎯 Project Goal

The primary goal of MeshWeaver is to understand how distributed nodes can coordinate without relying on a centralized task broker.

The project combines:

```text
Networking
     +
Async Programming
     +
Task Scheduling
     +
Peer Discovery
     +
Distributed State
     =
MeshWeaver
```

---

## 📌 Project Status

**Status: Completed Prototype**

Core functionality has been tested using multiple local UDP nodes.

Verified components:

```text
✅ Peer-to-Peer Communication
✅ UDP Networking
✅ Async Task Scheduler
✅ Remote Task Execution
✅ Worker Processing
✅ Heartbeat Monitoring
✅ Gossip Peer Discovery
✅ Distributed Task Registry
✅ Task Result Propagation
✅ Three-Node Integration Test
```

---

## 👨‍💻 Author

**MeshWeaver — Zero-Dependency P2P Async Task Broker**

Built as a distributed-systems learning and portfolio project using Python's standard library.
