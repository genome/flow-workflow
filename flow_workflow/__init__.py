import logging
import logging.handlers

try:
    NULL_HANDLER = logging.handlers.NullHandler()
except AttributeError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

    NULL_HANDLER = NullHandler()

logging.getLogger('flow_workflow').addHandler(NULL_HANDLER)
