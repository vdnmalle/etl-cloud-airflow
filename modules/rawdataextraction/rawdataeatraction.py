from abc import abstractmethod


class rawdataextraction:
    @abstractmethod
    def datasource(self):
        pass


class readrawdatafromapi(rawdataextraction):
    def datasource(self):
        pass


class readdatafromsalesforce(rawdataextraction):
    def datasource(self):
        pass


class readdatafrombigquery(rawdataextraction):
    def datasource(self):
        pass


class readdatafromsyanpse(rawdataextraction):
    def datasource(self):
        pass


class readdatafromredshift(rawdataextraction):
    def datasource(self):
        pass
