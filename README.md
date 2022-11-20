Importador de arquivos de CNPJ da Receita Federal
=======================

Importador em Python com interface gráfica, que permite obter e carregar em um banco de dados MySql os arquivos da base de dados nacional de CNPJ da Receita Federal. 

Tecnologias utilizadas
--------
- Python
- PyQt5
- MySql
- Requests
- BeatifulSoup
- ZipFile


Funcionalidades
--------

- Obtém os arquivos atualizados via wewbscraping da página oficial da Receita Federal
- Carrega arquivos do computador
- Descompacta e importa em banco de dados MySql (Necessário ter um servidor MySql configurado - local ou remoto)


Como utilizar
----------
Após clonar o diretório:

Crie um ambiente virtual (opcional, mas desejável)

python -m venv venv 

Instale os packages constantes no arquivo requirements.txt

pip install -r .\requirements.txt

Notas
----------

A carga completa dos arquivos, pode levar algumas horas, a base de dados conta com mais de 50 milhões de empresas, e são vários GB de dados, certifique-se que o servidor MySql dispõe dos recursos necessários.
