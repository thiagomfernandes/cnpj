import csv, zipfile, requests, io
from io import TextIOWrapper
from mysql_engine import MySqlEngine
from pathlib import Path

def getfriendlysize(bytessize) -> str:
    if isinstance(bytessize, str):
        return bytessize.replace("K","Kb").replace("M","Mb")

    base = 1024
    kilo = base ** 1
    mega = base ** 2
    giga = base ** 3
    tera = base ** 4
    peta = base ** 5

    if bytessize < kilo:
        return f'{bytessize:.1f} bytes'
    elif bytessize < mega:
        return f'{bytessize / kilo:.1f} Kb'
    elif bytessize < giga:
        return f'{bytessize/mega:.1f} Mb'
    elif bytessize < tera:
        return f'{bytessize/giga:.1f} Gb'
    else:
        return 'Are you crazy?'

class FileEngine():

    @staticmethod
    def categorizefile(cnpjfiles, file : str, size = None):        
        if 'Cnaes' in file:
            category = 'cnaes'
        elif 'Empresas' in file:
            category = 'empresas'
        elif 'Estabelecimentos' in file:
            category = 'estabelecimentos'
        elif 'Motivos' in file:
            category = 'motivos'
        elif 'Municipios' in file:
            category = 'municipios'
        elif 'Naturezas' in file:
            category = 'naturezas'
        elif 'Paises' in file:
            category = 'paises'
        elif 'Qualificacoes' in file:
            category = 'qualificacoes'
        elif 'Simples' in file:
            category = 'simples'
        elif 'Socios' in file:
            category = 'socios'
        else:
            return

        if 'http' in file:
            file = {
                "file" : file, 
                "size" : getfriendlysize(size) if size is not None else '? bytes', 
                "status": "Na fila (WEB)"     
                }

        else:
            size = Path(file).stat().st_size
            file = {
                "file" : file, 
                "size" : getfriendlysize(size), 
                "status": "Na fila"
                }

        if cnpjfiles.get(category) is None:
            cnpjfiles.update({category : [file]})
        else:
            fileexist = [
                f
                for files in cnpjfiles.values()
                for f in files
                if f['file'] == file['file']
            ]
            if not fileexist:
                cnpjfiles.get(category).append(file)

    @classmethod
    def readfile(cls, category, file, my : MySqlEngine) -> tuple:
        try:
            if my.createtable(category):

                if 'http' in file:
                    r = requests.get(file, stream=True)
                    z = zipfile.ZipFile(io.BytesIO(r.content))
                else:
                    z = zipfile.ZipFile(file)
                
                with z:
                    for zippedfilename in z.namelist():
                        with z.open(zippedfilename) as f:
                            reader = csv.reader(TextIOWrapper(f, 'ansi'), delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
                            rows = []
                            for row in reader:
                                rows.append(row)
                                if len(rows) >= 10000:
                                    my.insertrows(category, rows)
                                    rows = []
                            if len(rows) > 0:
                                    my.insertrows(category, rows)

                            return (True, f'Dados de {category} inseridos com sucesso')
            else:
                return (False, f'Não foi possível criar a tabela de {category}')

        except:
            return (False, f'Houve um erro na importação dos dados de {category} - Arquivo: {file}')

                            


    