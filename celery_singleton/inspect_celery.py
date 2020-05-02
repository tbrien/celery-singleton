from celery import Celery

class CeleryTask:
    def __init__(self, name, args, kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs

def list_celery_tasks_with_name(app: Celery, name: str) -> [CeleryTask]:
    active_tasks = app.control.inspect().active()
    if not active_tasks:
        return []
    global_task_list = []
    try:
        for worker, task_list in active_tasks.items():
            for task in task_list:
                if not task['name'] == name:
                    continue
                celery_task = CeleryTask(task["name"], task["args"], task["kwargs"])
                global_task_list.append(celery_task)
    except Exception:
        pass
    return global_task_list

def are_worker_active(app: Celery) -> bool:
    worke_pong = app.control.inspect().ping()
    if not worke_pong:
        return False
    try:
        for worker, response in worke_pong.items():
            if response['ok']:
                return True
    except Exception:
        pass
    return False