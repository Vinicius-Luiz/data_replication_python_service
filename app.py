from ui.ReplicationController import ReplicationController
from trempy.Loggings.Logging import ReplicationLogger

ReplicationLogger.configure_logging()

if __name__ == "__main__":
    controller = ReplicationController()
    controller.display_tabs()