import pymysql
from contextlib import contextmanager

class MySqlEngine:
    host = ''
    user = ''
    password = ''
    db = ''

    
    @contextmanager
    def openconn(self, usingdb = True):
        db2= self.db if usingdb else ''
        conection = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            db= self.db if usingdb else '',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        ) 
        try:
            yield conection
        finally:
            conection.close()

    def canconnect(self) -> bool:
        try:
            with self.openconn(False) as conne:
                return True
        except:
            return False

    
    def dbexists(self) -> bool:
        with self.openconn(False) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SHOW DATABASES LIKE %s;",self.db)
                result = cursor.fetchall()
                if result:
                    return True
        return False

    def createdb(self) -> bool:
        with self.openconn(False) as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute('CREATE DATABASE IF NOT EXISTS {0};'.format(self.db))
                    conn.commit()
                except:
                    return False
        return True

    def createtable(self, category : str) -> bool:
        with self.openconn() as conn:
            with conn.cursor() as cursor:
                try:
                    sql = MySqlEngine.getcreatetablesql(category)
                    cursor.execute(sql)
                    conn.commit()
                except:
                    return False
        return True

    def insertrows(self, category, rows) -> bool:
        with self.openconn() as conn:
            with conn.cursor() as cursor:
                try:
                    sql = MySqlEngine.getinsertsql(category)
                    cursor.executemany(sql, rows)
                    conn.commit()
                except:
                    return False
        return True

    @staticmethod
    def getcreatetablesql(category : str) -> str:       
        if category in ('cnaes','motivos', 'municipios', 'naturezas', 'paises', 'qualificacoes'):
            sql = """CREATE TABLE IF NOT EXISTS `{0}` (
                    `codigo` int(7) NOT NULL,
                    `descricao` varchar(500) DEFAULT NULL,
                    PRIMARY KEY (`codigo`)                    
                    ) DEFAULT CHARSET=utf8;""".format(category)

        elif category == 'empresas':
            sql ="""CREATE TABLE IF NOT EXISTS `empresas` (
                `cnpj_basico` int(10) NOT NULL,
                `nome_razao` varchar(500) DEFAULT NULL,
                `natureza_juridica` int(6) DEFAULT NULL,
                `qualificacao_responsavel` int(6) DEFAULT NULL,
                `capital_social` float DEFAULT NULL,
                `porte_empresa` int(1) DEFAULT NULL,
                `ente_federativo` varchar(100) DEFAULT NULL,
                PRIMARY KEY (`cnpj_basico`)
                ) DEFAULT CHARSET=utf8;"""

        elif category == 'estabelecimentos':
            sql = """CREATE TABLE IF NOT EXISTS `estabelecimentos` (
                `cnpj_basico` int(10) NOT NULL,
                `cnpj_ordem` int(3) NOT NULL,
                `cnpj_dv` int(2) NOT NULL,
                `identificador_matriz_filial` int(1) DEFAULT NULL,
                `nome_fantasia` varchar(500) DEFAULT NULL,
                `situacao_cadastral` int(1) DEFAULT NULL,
                `data_situacao_cadastral` date DEFAULT NULL,
                `motivo_situacao_cadastral` int(4) DEFAULT NULL,
                `nome_cidade_exterior` varchar(500) DEFAULT NULL,
                `pais` int(6) DEFAULT NULL,
                `data_inicio_atividade` date DEFAULT NULL,
                `cnae_principal` varchar(10) DEFAULT NULL,
                `cnae_secundario` varchar(4000) DEFAULT NULL,
                `tipo_logradouro` varchar(200) DEFAULT NULL,
                `logradouro` varchar(1000) DEFAULT NULL,
                `numero` varchar(100) DEFAULT NULL,
                `complemento` varchar(500) DEFAULT NULL,
                `bairro` varchar(200) DEFAULT NULL,
                `cep` varchar(8) DEFAULT NULL,
                `uf` varchar(2) DEFAULT NULL,
                `municipio` int(6) DEFAULT NULL,
                `ddd_telefone1` varchar(2) DEFAULT NULL,
                `telefone1` varchar(10) DEFAULT NULL,
                `ddd_telefone2` varchar(2) DEFAULT NULL,
                `telefone2` varchar(10) DEFAULT NULL,
                `ddd_fax` varchar(2) DEFAULT NULL,
                `fax` varchar(10) DEFAULT NULL,
                `email` varchar(500) DEFAULT NULL,
                `situacao_especial` varchar(100) DEFAULT NULL,
                `data_situacao_especial` date DEFAULT NULL,
                PRIMARY KEY (`cnpj_basico`,`cnpj_ordem`,`cnpj_dv`)
                ) DEFAULT CHARSET=utf8;"""

        elif category == 'simples':
            sql = """CREATE TABLE IF NOT EXISTS `simples` (
                `cnpj_basico` int(8) DEFAULT NULL,
                `opcao_simples` varchar(1) DEFAULT NULL,
                `data_opcao_simples` date DEFAULT NULL,
                `data_exclusao_simples` date DEFAULT NULL,
                `opcao_mei` varchar(1) DEFAULT NULL,
                `data_opcao_mei` date DEFAULT NULL,
                `data_exclusao_mei` date DEFAULT NULL
                ) DEFAULT CHARSET=utf8;"""

        elif category == 'socios':
            sql = """CREATE TABLE IF NOT EXISTS `socios` (
                `cnpj_basico` int(10) NOT NULL,
                `identificador` int(1) DEFAULT NULL,
                `nome_razao` varchar(500) DEFAULT NULL,
                `cpf_cnpj` varchar(14) NOT NULL,
                `qualificacao` int(6) DEFAULT NULL,
                `data_entrada_sociedade` date DEFAULT NULL,
                `pais` int(6) DEFAULT NULL,
                `cpf_representante` varchar(11) DEFAULT NULL,
                `nome_representante` varchar(500) DEFAULT NULL,
                `qualificacao_representante` int(6) DEFAULT NULL,
                `faixa_etaria` int(2) DEFAULT NULL,
                PRIMARY KEY (`cnpj_basico`,`cpf_cnpj`)
                ) DEFAULT CHARSET=utf8;"""
        return sql

    @staticmethod
    def getinsertsql(category : str) -> str:       
        if category in ('cnaes','motivos', 'municipios', 'naturezas', 'paises', 'qualificacoes'):
            sql = """REPLACE INTO {0} 
                    (codigo, descricao) 
                    VALUES 
                    (
                        %s, %s
                    );""".format(category)

        elif category == 'empresas':
            sql = """REPLACE INTO `empresas` 
                    (`cnpj_basico`, 
                    `nome_razao`, 
                    `natureza_juridica`, 
                    `qualificacao_responsavel`, 
                    `capital_social`, 
                    `porte_empresa`, 
                    `ente_federativo`
                    ) 
                    VALUES 
                    (
                        %s, %s, %s, %s, %s, %s, %s
                    );"""

        elif category == 'estabelecimentos':
            sql = """REPLACE INTO `estabelecimentos` 
                    (`cnpj_basico`, 
                    `cnpj_ordem`, 
                    `cnpj_dv`, 
                    `identificador_matriz_filial`, 
                    `nome_fantasia`, 
                    `situacao_cadastral`, 
                    `data_situacao_cadastral`, 
                    `motivo_situacao_cadastral`, 
                    `nome_cidade_exterior`, 
                    `pais`, 
                    `data_inicio_atividade`, 
                    `cnae_principal`, 
                    `cnae_secundario`, 
                    `tipo_logradouro`, 
                    `logradouro`, 
                    `numero`, 
                    `complemento`, 
                    `bairro`, 
                    `cep`, 
                    `uf`, 
                    `municipio`, 
                    `ddd_telefone1`, 
                    `telefone1`, 
                    `ddd_telefone2`, 
                    `telefone2`, 
                    `ddd_fax`, 
                    `fax`, 
                    `email`, 
                    `situacao_especial`, 
                    `data_situacao_especial`
                    )
                    VALUES
                    (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s
                    );"""

        elif category == 'simples':
            sql = """REPLACE INTO `simples` 
                    (`cnpj_basico`, 
                    `opcao_simples`, 
                    `data_opcao_simples`, 
                    `data_exclusao_simples`, 
                    `opcao_mei`, 
                    `data_opcao_mei`, 
                    `data_exclusao_mei`
                    )
                    VALUES
                    (
                        %s, %s, %s, %s, %s, %s, %s
                    );"""

        elif category == 'socios':
            sql = """REPLACE INTO `socios` 
                    (`cnpj_basico`, 
                    `identificador`, 
                    `nome_razao`, 
                    `cpf_cnpj`, 
                    `qualificacao`, 
                    `data_entrada_sociedade`, 
                    `pais`, 
                    `cpf_representante`, 
                    `nome_representante`, 
                    `qualificacao_representante`, 
                    `faixa_etaria`
                    )
                    VALUES
                    (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    );"""
        return sql

