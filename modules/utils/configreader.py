import configparser

# may be plan to chnage this to read s3 config , cloud storage config etc
class configreader:
    def configreader(self,obj):
        config = configparser.ConfigParser
        config.read_string(obj['Body'].read.decode())
        return config