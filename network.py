class Network:

    def __init__(self, transport):

        self.transport = transport

    def send(self, message, host, port):

        data = message.encode()

        self.transport.sendto(
            data,
            (host, port)
        )

        # Keep background traffic quiet
        if message in (
            "HEARTBEAT",
            "HEARTBEAT_ACK"
        ):
            return

        # Keep gossip traffic quiet
        if message.startswith('{"type": "GOSSIP"'):
            return

        print(
            f"[NETWORK] Sent '{message}' "
            f"-> {host}:{port}"
        )