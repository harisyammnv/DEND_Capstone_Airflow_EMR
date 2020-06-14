import requests
import pandas as pd
from bs4 import BeautifulSoup


def table_to_array(table):
    """Parses a html segment started with tag <table> followed
    by multiple <tr> (table rows) and inner <td> (table data) tags.
    It returns a list of rows with inner columns.
    Accepts only one <th> (table header/data) in the first row.
    """

    def row_get_data_text(tr, coltag='td'): # td (data) or th (header)
        return [td.get_text(strip=True) for td in tr.findAll(coltag)]

    rows = []
    trs = table.findAll('tr')
    headerow = row_get_data_text(trs[0], 'th')
    if headerow:  # if there is a header row include first
        rows.append(headerow)
        trs = trs[1:]
    for tr in trs:  # for every table row
        rows.append(row_get_data_text(tr, 'td'))  # data row
    return rows


scrape = False
process = True
if scrape:
    URL = 'https://fam.state.gov/fam/09FAM/09FAM010205.html'
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, 'html.parser')
    htmltables = soup.find_all("table")

    table_names = ['visa-issuing-ports.csv','nationality-codes.csv','port-of-entry-codes.csv']
    for tbl,name in zip(htmltables, table_names):
        list_table = table_to_array(tbl)
        df = pd.DataFrame(list_table[1:])
        processed = pd.concat([df[df.columns[:2]],df[df.columns[2:]].rename(columns={2:0,3:1})])
        processed.columns=[list_table[0][0],list_table[0][1]]
        processed.dropna(inplace=True)
        processed = processed.loc[(processed[list_table[0][0]] != '') & (processed[list_table[0][1]] != '')]
        processed.reset_index(inplace=True, drop=True)
        processed.to_csv('C:/Users/harisyam/Desktop/data-engineering-nanodegree/6-Capstone_Project/data/'+name, index=False)
if process:
    df = pd.read_csv('../data/visa-type.csv', sep='\t', header=None)
    df.columns=['visa-type','description']
    df.to_csv('data/visa-type.csv', index=False)