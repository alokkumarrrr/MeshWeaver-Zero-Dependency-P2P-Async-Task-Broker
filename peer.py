class PeerManager:

    def __init__(self):
        self.peers = set()

    def add_peer(self, host, port):
        peer = (host, port)

        if peer not in self.peers:
            self.peers.add(peer)

            print(
                f"[PEER] Added peer -> "
                f"{host}:{port}"
            )

    def remove_peer(self, host, port):
        peer = (host, port)

        if peer in self.peers:
            self.peers.remove(peer)

            print(
                f"[PEER] Removed peer -> "
                f"{host}:{port}"
            )

    def get_peers(self):
        return list(self.peers)