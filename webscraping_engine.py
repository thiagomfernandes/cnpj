import requests, io
from bs4 import BeautifulSoup
import re

def getwebfiles() -> list:
    urlreceita = 'http://200.152.38.155/CNPJ/'
    response = requests.get(urlreceita)
    html = BeautifulSoup(response.text, 'html.parser')
    files = []

    for link in html.find_all('a', href=re.compile("zip")):
        size = link.parent.parent.find_all('td')[3].get_text().strip()        
        files.append((urlreceita + link['href'], size))
        #return files #teste

    return files
