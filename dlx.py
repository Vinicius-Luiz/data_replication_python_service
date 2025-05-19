from trempy.Loggings.Logging import ReplicationLogger
from trempy.Messages.MessageDlx import MessageDlx
from trempy.Shared.Utils import Utils
from trempy.Tasks.Task import Task

ReplicationLogger.configure_logging()

task: Task = Utils.read_task_pickle()

consumer_dlx = MessageDlx(task_name=task.task_name)

consumer_dlx.start_consuming()
