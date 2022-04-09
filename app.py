import logging
from utility import Utility
from recover import RECOVER

# config
# - modify config.ini to point to your CDM database
# - user running this process must have read-access to CDM database
# - https://nyc-cdrn.atlassian.net/wiki/spaces/REC/overview
class App:

    def __init__(self):  
        self._util = Utility()
        self._util.ConfigureLogging()
        self._util.ConfigureDirectories()

    def Process(self):
        try:
            logging.info('*** Start ***')
            r = RECOVER()
            r.Process()
            logging.info('*** Stop ***')
        except:
            logging.exception("")

app = App()
app.Process()