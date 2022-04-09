import os, logging, configparser, shutil
from datetime import datetime

class Utility:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read("config.ini")
        self._constr = "DRIVER={SQL Server};SERVER="+config["mssql"]["host"]+";DATABASE="+config["mssql"]["database"]+";PORT=1433;Trusted_Connection=yes;"
        self._output_dir = config["general"]["output_dir"] + "\\recover_output"
        self._schema = config["mssql"]["schema"]
        self._site = config["general"]["site"]
        self._dateModel = config["general"]["datamodel"]

    # configure default python logging
    def ConfigureLogging(self):
        path = os.getcwd() + '\logs'
        if not os.path.exists(path):
            os.makedirs(path)
        path = path + '\\logfile_{:%Y%m%d}.txt'.format(datetime.now())
        logging.basicConfig(filename=path, 
        level=logging.DEBUG, format='%(asctime)s %(message)s')
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        logging.getLogger().addHandler(ch)

    # configure directories
    def ConfigureDirectories(self):
        if os.path.exists(self._output_dir):
            shutil.rmtree(self._output_dir)
        os.makedirs(self._output_dir)

    
