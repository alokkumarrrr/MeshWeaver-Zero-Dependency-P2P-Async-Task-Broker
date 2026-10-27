import asyncio


class TaskScheduler:

    def __init__(self, result_callback=None):
        self.queue = asyncio.Queue()
        self.running = False
        self.result_callback = result_callback

    async def add_task(self, task):

        await self.queue.put(task)

        print(
            f"[SCHEDULER] Task queued -> "
            f"{task.id}"
        )

    async def get_next_task(self):

        task = await self.queue.get()

        print(
            f"[SCHEDULER] Task selected -> "
            f"{task.id}"
        )

        return task

    def pending_tasks(self):
        return self.queue.qsize()

    async def worker(self):

        self.running = True

        print("[WORKER] Task worker started")

        while self.running:

            task = await self.get_next_task()

            try:

                task.mark_running()

                print(
                    f"[WORKER] Running task -> "
                    f"{task.id}"
                )

                # Simulate asynchronous work
                await asyncio.sleep(2)

                task.mark_completed()

                # Create task result
                result = {
                    "task_id": task.id,
                    "status": task.status,
                    "message": "Task completed successfully"
                }

                print(
                    f"[WORKER] Task completed -> "
                    f"{task.id}"
                )

                print(
                    f"[RESULT] {result}"
                )

                if self.result_callback:
                    self.result_callback(result)

            except Exception as error:

                task.mark_failed()

                result = {
                    "task_id": task.id,
                    "status": task.status,
                    "message": str(error)
                }

                print(
                    f"[WORKER] Task failed -> "
                    f"{task.id} | {error}"
                )

                print(
                    f"[RESULT] {result}"
                )

                if self.result_callback:
                    self.result_callback(result)