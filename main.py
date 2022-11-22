import sys
from design.design import *
from PyQt5.QtWidgets import QMainWindow, QApplication, QFileDialog, QTreeWidgetItem, QMessageBox, QPushButton, QTreeWidget
from PyQt5.QtGui import QFont,QBrush
from PyQt5.QtCore import Qt
from mysql_engine import MySqlEngine
from file_engine import FileEngine
from webscraping_engine import getwebfiles
import configparser

'''
venv/scripts/activate
pyuic5 design/design.ui -o design/design.py

'''


class CNPJ(QMainWindow, Ui_MainWindow):
    cnpjfiles : dict = {}

    def __init__(self, parent=None):
        super().__init__(parent)
        super().setupUi(self)
        self.btnsearchfiles.clicked.connect(self.getfileslocal)
        self.btnimportfiles.clicked.connect(self.importfiles)
        self.btnwebreceita.clicked.connect(self.getfilesweb)
        self.treefiles.itemChanged.connect(self.handleItemChanged)

        #initialize db configs
        self.readconfig()

    def readconfig(self):
        config = configparser.ConfigParser()
        config.read('mysql.config')

        self.txtmysqlhost.setText(config['mysql']['host'])
        self.txtmysqluser.setText(config['mysql']['user'])
        self.txtmysqlpass.setText(config['mysql']['pass'])
        self.txtmysqldb.setText(config['mysql']['db'])
    
    def saveconfig(self):
        config = configparser.ConfigParser()
        config.add_section('mysql')
        config.set('mysql','host', self.txtmysqlhost.text())
        config.set('mysql','user', self.txtmysqluser.text())
        config.set('mysql','pass', self.txtmysqlpass.text())
        config.set('mysql','db', self.txtmysqldb.text())
        with open('mysql.config', 'w') as configfile:
            config.write(configfile)

    def printtree(self):
        self.treefiles.clear()
        batention = QBrush(Qt.darkBlue)
        bsuccess = QBrush(Qt.darkGreen)

        for category, files in self.cnpjfiles.items():
            categoryitem = QTreeWidgetItem(self.treefiles)
            categoryitem.setText(0, category)
            categoryitem.setExpanded(True)
            
            for file in files:
                item = QTreeWidgetItem(categoryitem)
                item.setCheckState(0, Qt.Checked if file.get("checked", True) else Qt.Unchecked)
                item.setText(0, f'{file["file"]} {file["size"]} - {file["status"]}')
                item.setStatusTip(0, file["file"])
                
                if 'processando' in file["status"].lower():
                    self.treefiles.scrollToItem(item)
                    font = QFont("", 9, QFont.Bold)                    
                    item.setForeground(0, batention)
                    item.setFont(0, font)

                elif 'sucesso' in file["status"].lower():
                    item.setForeground(0 , bsuccess )

        self.update()
        self.repaint()

    def handleItemChanged(self, item, column):
        checked = item.checkState(column) == QtCore.Qt.Checked
        
        for files in self.cnpjfiles.values():
            for file in files:
                if file['file'] == item.statusTip(0):
                    file.update({'checked' : checked})
                    return

    def getfileslocal(self):
        files, _ = QFileDialog.getOpenFileNames(
            None,
            "Arquivos Tabulados de CNPJ",
            '*.zip'            
        )        
        for file in files:
            FileEngine.categorizefile(self.cnpjfiles, file)
        
        self.printtree()

    def getfilesweb(self):
        files = getwebfiles()
        for file, size in files:
            FileEngine.categorizefile(self.cnpjfiles, file, size)        
        self.printtree()
        
    def preparedb(self, my : MySqlEngine) -> bool:
        my.db = self.txtmysqldb.text()
        my.host = self.txtmysqlhost.text()
        my.user = self.txtmysqluser.text()
        my.password = self.txtmysqlpass.text()

        if not my.canconnect():
            msgBox = QMessageBox()
            msgBox.setIcon(QMessageBox.Information)
            msgBox.setWindowTitle('Erro DB')
            msgBox.setText(f'Não foi possível conectar ao MySql utilizando os parametros informados...')
            msgBox.exec()
            return False

        if not my.dbexists():
            msgBox = QMessageBox()
            msgBox.setIcon(QMessageBox.Information)
            msgBox.setText(f'O database informado não existe. Deseja que o sistema tente criar o database {self.txtmysqldb.text()}?')
            msgBox.setWindowTitle("Criar database")
            msgBox.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            returnValue = msgBox.exec()
            if returnValue == QMessageBox.Yes:
                if not my.createdb():
                    msgBox = QMessageBox()
                    msgBox.setIcon(QMessageBox.Information)
                    msgBox.setWindowTitle('Erro DB')
                    msgBox.setText(f'Não foi possível criar o database {my.db}...')
                    msgBox.exec()
                    return False
        return True

    def importfiles(self):
        if not self.txtmysqlhost.text() or not self.txtmysqluser.text() or not self.txtmysqldb.text():
            msgBox = QMessageBox()
            msgBox.setIcon(QMessageBox.Information)
            msgBox.setWindowTitle('Parâmetros inválidos')
            msgBox.setText('É obrigatório informar o host, user e database para continuar...') 
            msgBox.exec()
            return    
        
        self.saveconfig()

        hascheckedfiles = [
            file
            for files in self.cnpjfiles.values()
            for file in files
            if file['checked']
        ]
        if not hascheckedfiles:
            msgBox = QMessageBox()
            msgBox.setIcon(QMessageBox.Information)
            msgBox.setWindowTitle('Selecione arquivos...')
            msgBox.setText('Carregue e selecione os arquivos que deseja importar...\nClique em [Procurar Arquivos no Computador...] ou [Obter via Receita Federal]') 
            msgBox.exec()
            return

        my = MySqlEngine()

        if not self.preparedb(my):
            return

        for category, files in self.cnpjfiles.items():
            for file in [f for f in files if f['checked']]:                
                file["status"] = 'Processando...' if not 'http' in file['file'] else 'Baixando e processando...'
                self.printtree()
                sucesso, msg = FileEngine.readfile(category, file['file'], my)
                if not sucesso:
                    file["status"] = 'Erro de importação'
                    msgBox = QMessageBox()
                    msgBox.setIcon(QMessageBox.Information)
                    msgBox.setWindowTitle('Erro de importação')
                    msgBox.setText(msg)
                    msgBox.exec()
                else:
                    file["status"] = 'Importado com sucesso'
                self.printtree()

if __name__ == '__main__':
    qt = QApplication(sys.argv)
    cnpj = CNPJ()
    cnpj.show()
    qt.exec_()
