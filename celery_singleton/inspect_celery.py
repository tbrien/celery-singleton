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
                print("ADD : " + str(task))
                print("\tname : " + str(celery_task.name))
                print("\targs : " + str(celery_task.args))
                print("\tkwargs : " + str(celery_task.kwargs))
    except Exception:
        pass
    return global_task_list