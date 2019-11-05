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

    def call_query(self, usergroup, query, variant):
        data = self.conn.call("RSAQ_REMOTE_QUERY_CALL", USERGROUP=usergroup, QUERY=query, VARIANT=variant,
                                DATA_TO_MEMORY="X", EXTERNAL_PRESENTATION="Z")
        columns = []
        for i in data['LISTDESC']:
            if i['LID'] == 'G00':
                columns.append(i['FCOL'])
        datastring = ""
        datalist = [list(x.values()) for x in data['LDATA']]
        datastring = datastring.join(str(r) for v in datalist for r in v)
        datastring = datastring.split(';/')[0]
        datastring = datastring.split(';')
        datalist = []
        for i in datastring:
            datalist.append(re.split(',\d\d\d:', i[4:]))

        results = pd.DataFrame(data=datalist, columns=columns)
        return results


# Choose your report and user group
usergroup = "usergroup"
query = "query"
variant = "variant"

s = main()

results = s.call_query(usergroup, query, variant)

print(results.head())

results.to_excel('output.xlsx', index=False)
