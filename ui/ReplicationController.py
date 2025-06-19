from ui.components.tools.SessionManager import SessionManager
from ui.components.UIComponents import UIComponents


class ReplicationController:
    def __init__(self):
        self.session_manager = SessionManager()
        self.ui_components = UIComponents()
