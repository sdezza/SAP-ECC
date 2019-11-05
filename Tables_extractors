from pyrfc import Connection
from configparser import ConfigParser
import re
import pandas as pd

class main():
    def __init__(self):
        config = ConfigParser()
        config.read('sapnwrfc.cfg')
        params_connection = config._sections['connection']
        self.conn = Connection(**params_connection)

    def qry(self, Fields, SQLTable, Where, MaxRows, FromRow):
        """A function to query SAP with RFC_READ_TABLE"""
        if Fields[0] == '*':
            Fields = ''
        else:
            Fields = [{'FIELDNAME':x} for x in Fields] 
            
        options = [{'TEXT': x} for x in Where] 
        rowcount = MaxRows

        # Here is the call to SAP's RFC_READ_TABLE
        tables = self.conn.call("RFC_READ_TABLE", QUERY_TABLE=SQLTable, DELIMITER='|', FIELDS = Fields, \
                                OPTIONS=options, ROWCOUNT = MaxRows, ROWSKIPS=FromRow)

        # We split out fields and fields_name to hold the data and the column names
        fields = []
        fields_name = []

        data_fields = tables["DATA"] # pull the data part of the result set
        data_names = tables["FIELDS"] # pull the field name part of the result set

        headers = [x['FIELDTEXT'] for x in data_names] # headers extraction
        long_fields = len(data_fields) # data extraction
        long_names = len(data_names) # full headers extraction if you want it

        # now parse the data fields into a list
        for line in range(0, long_fields):
            fields.append(data_fields[line]["WA"].strip())

        # for each line, split the list by the '|' separator
        fields = [x.strip().split('|') for x in fields ]

        return fields, headers



s = main()

# Choose your fields and table
fields = ['MBLNR','MJAHR','ZEILE_I','VGART','BLART','BLAUM','BLDAT','BUDAT','CPUDT','CPUTM','XBLNR','FRATH','BWART_I','WERKS_I','LGORT_I','CHARG_I','INSMK_I','ZUSCH_I','ZUSTD_I','SOBKZ_I','LIFNR_I','KUNNR_I','MATNR_I','KDAUF_I','KDPOS_I','KDEIN_I','WAERS_I','DMBTR_I','BWTAR_I','MENGE_I','MEINS_I','ERFME_I','BPRME_I','EBELN_I','EBELP_I','ELIKZ_I','BUKRS_I','TBNUM_I','TBPOS_I']
table = 'WB2_V_MKPF_MSEG2'
where = ['MJAHR = 2019']

#set maxrows for test purposes
maxrows = 100
fromrow = 0

# query SAP
results, headers = s.qry(fields, table, where, maxrows, fromrow)

# results in DataFrame
results = pd.DataFrame(results)

results.to_csv('tables.csv', index=False, header=headers, encoding='utf-8')
