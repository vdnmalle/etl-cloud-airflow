from modules.rawdataextraction import rawdataeatraction
from modules.rawdataextraction.rawdataeatraction import readrawdatafromapi, readdatafromsalesforce, readdatafromsyanpse, \
    readdatafrombigquery, readdatafromredshift


class sourcedata:
    def __init__(self, rawdataextraction):
        self.rawdataextraction = rawdataextraction

    def read_source_data(self):
        return self.rawdataextraction.datasource(self)


def main():
    if rawdataeatraction == "api":
        apidata = readrawdatafromapi()
        source_data = sourcedata(apidata)
        source_data.read_source_data()
    elif rawdataeatraction == "sfdc":
        sfdcdata = readdatafromsalesforce
        soure_data = sourcedata(sfdcdata)
        soure_data.read_source_data()
    elif rawdataeatraction == "bigquery":
        bigquerydata = readdatafrombigquery
        source_data = sourcedata(bigquerydata)
        source_data.read_source_data()
    elif rawdataeatraction == "synapse":
        synapsedata = readdatafromsyanpse
        source_data = sourcedata(synapsedata)
        source_data.read_source_data()
    elif rawdataeatraction == "s3":
        s3data = readdatafromredshift
        source_data = sourcedata(s3data)
        source_data.read_source_data()


if __name__ == '__main__':
    main()
