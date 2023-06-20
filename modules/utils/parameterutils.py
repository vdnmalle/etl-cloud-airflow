from modules.utils.configreader import configreader


class paramterutils(configreader):
    # even here try to change the methods like s3 paramters,gc parameters etc for convience

    def __init__(self):
        self.temp = None
        self.temp_bucket = None
        self.raw_bucket = None
        self.standard_bucket = None
        self.driver = None
        self.dbname = None
        self.password = None
        self.user = None
        self.port = None
        self.host = None
        self.config = None

    def set_parameters(self):
        self.config = configreader.configreader(self, "<TBD>")
        self.host = self.config['dw_props']['host']
        self.port = self.config['dw_props']['port']
        self.user = self.config['dw_props']['user']
        self.password = self.config['dw_props']['password']
        self.dbname = self.config['dw_props']['dbname']
        self.driver = self.config['dw_props']['driver']
        self.standard_bucket = self.config['storage_props']['standard_bucket']
        self.raw_bucket = self.config['storage_props']['raw_bucket']
        self.temp_bucket = self.config['storage_props']['temp_bucket']
        self.temp = self.config['storage_props']['temp']
