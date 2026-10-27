class DHT:

    def __init__(self):

        self.task_locations = {}

    def register_task(self, task_id, host, port):

        self.task_locations[task_id] = (
            host,
            port
        )

        print(
            f"[DHT] Task registered -> "
            f"{task_id} at {host}:{port}"
        )

    def find_task(self, task_id):

        return self.task_locations.get(task_id)

    def remove_task(self, task_id):

        if task_id in self.task_locations:

            del self.task_locations[task_id]

            print(
                f"[DHT] Task removed -> "
                f"{task_id}"
            )

    def all_tasks(self):

        return dict(self.task_locations)