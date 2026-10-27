import uuid


class Task:

    def __init__(self, name, payload=None):
        self.id = str(uuid.uuid4())
        self.name = name
        self.payload = payload
        self.status = "PENDING"

    def mark_running(self):
        self.status = "RUNNING"

    def mark_completed(self):
        self.status = "COMPLETED"

    def mark_failed(self):
        self.status = "FAILED"

    def __repr__(self):
        return (
            f"Task("
            f"id={self.id}, "
            f"name={self.name}, "
            f"status={self.status}"
            f")"
        )