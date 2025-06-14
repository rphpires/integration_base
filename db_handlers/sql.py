import pyodbc
import logging
from typing import List, Dict, Any, Optional, Union
from contextlib import contextmanager
import urllib.parse


class SQLServerDBManager:
    """
    Classe para gerenciar conexões e consultas em banco de dados SQL Server.

    Esta classe utiliza o pyodbc para conectar ao SQL Server, suportando
    autenticação Windows e SQL Server Authentication.
    """

    def __init__(self, server: str, database: str, username: Optional[str] = None,
                 password: Optional[str] = None, driver: str = "ODBC Driver 18 for SQL Server",
                 pool_size: int = 5, integrated_security: bool = False,
                 encrypt: bool = False, trust_server_certificate: bool = True):
        """
        Inicializa o gerenciador de banco SQL Server.

        Args:
            server (str): Nome do servidor SQL Server
            database (str): Nome do banco de dados
            username (str, optional): Nome de usuário (para SQL Authentication)
            password (str, optional): Senha do usuário (para SQL Authentication)
            driver (str): Driver ODBC a ser usado
            pool_size (int): Tamanho do pool de conexões (padrão: 5)
            integrated_security (bool): Se deve usar Windows Authentication (padrão: False)
            encrypt (bool): Se deve usar conexão criptografada
            trust_server_certificate (bool): Se deve confiar no certificado do servidor
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.pool_size = pool_size
        self.integrated_security = integrated_security
        self.encrypt = encrypt
        self.trust_server_certificate = trust_server_certificate
        self.pool = []
        self.pool_in_use = set()

        # Configurar logging com nível DEBUG para diagnóstico
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)

    def _build_connection_string(self) -> str:
        """
        Constrói a string de conexão para o SQL Server.

        Returns:
            str: String de conexão ODBC
        """
        conn_parts = [
            f"DRIVER={{{self.driver}}}",
            f"SERVER={self.server}",
            f"DATABASE={self.database}"
        ]

        # Autenticação: Integrada (Windows) ou SQL Server
        if self.integrated_security:
            conn_parts.append("Trusted_Connection=yes")
        else:
            if not self.username or not self.password:
                raise ValueError("Username e password são obrigatórios quando integrated_security=False")
            conn_parts.append(f"UID={self.username}")
            conn_parts.append(f"PWD={self.password}")

        # Configurações de segurança
        if self.encrypt:
            conn_parts.append("Encrypt=yes")
        else:
            conn_parts.append("Encrypt=no")

        if self.trust_server_certificate:
            conn_parts.append("TrustServerCertificate=yes")

        connection_string = ";".join(conn_parts)
        self.logger.debug(f"Connection string: {connection_string}")
        return connection_string

    def create_pool(self) -> None:
        """
        Cria um pool de conexões para melhor performance.
        """
        try:
            connection_string = self._build_connection_string()

            for _ in range(self.pool_size):
                conn = pyodbc.connect(connection_string)
                self.pool.append(conn)

            self.logger.info(f"Pool de conexões criado com sucesso. Tamanho: {self.pool_size}")
        except pyodbc.Error as e:
            self.logger.error(f"Erro ao criar pool de conexões: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        Context manager para obter conexão do pool ou criar uma nova.

        Yields:
            pyodbc.Connection: Conexão com o banco de dados
        """
        connection = None
        from_pool = False

        try:
            # Tentar obter conexão do pool
            if self.pool:
                for conn in self.pool:
                    if conn not in self.pool_in_use:
                        connection = conn
                        self.pool_in_use.add(conn)
                        from_pool = True
                        break

            # Se não conseguiu do pool, criar nova conexão
            if not connection:
                connection_string = self._build_connection_string()
                connection = pyodbc.connect(connection_string)

            yield connection

        except pyodbc.Error as e:
            self.logger.error(f"Erro na conexão: {e}")
            raise
        finally:
            if connection:
                if from_pool:
                    self.pool_in_use.discard(connection)
                else:
                    connection.close()

    def execute_query(self, sql: str, params: Optional[Union[Dict, tuple]] = None,
                      fetch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Executa uma consulta SELECT e retorna os resultados.

        Args:
            sql (str): Query SQL a ser executada
            params (dict ou tuple, optional): Parâmetros para a query
            fetch_size (int): Tamanho do fetch (padrão: 1000)

        Returns:
            List[Dict[str, Any]]: Lista de dicionários com os resultados
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()

                if params:
                    if isinstance(params, dict):
                        # Converter dict para tuple na ordem correta
                        cursor.execute(sql, tuple(params.values()))
                    else:
                        cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                # Obter nomes das colunas
                columns = [column[0] for column in cursor.description]

                # Fetch todos os resultados
                rows = cursor.fetchall()

                # Converter para lista de dicionários
                results = [dict(zip(columns, row)) for row in rows]

                self.logger.info(f"Query executada com sucesso. {len(results)} registros retornados.")
                return results

        except pyodbc.Error as e:
            self.logger.error(f"Erro ao executar query: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            raise

    def execute_dml(self, sql: str, params: Optional[Union[Dict, List[Dict], tuple, List[tuple]]] = None,
                    commit: bool = True) -> int:
        """
        Executa comandos DML (INSERT, UPDATE, DELETE).

        Args:
            sql (str): Comando SQL DML
            params (dict, list, tuple, optional): Parâmetros para o comando
            commit (bool): Se deve fazer commit automático (padrão: True)

        Returns:
            int: Número de linhas afetadas
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()

                if isinstance(params, list):
                    # Executar em lote
                    if params and isinstance(params[0], dict):
                        # Lista de dicionários - converter para lista de tuplas
                        param_tuples = [tuple(p.values()) for p in params]
                        cursor.executemany(sql, param_tuples)
                    else:
                        cursor.executemany(sql, params)
                    affected_rows = cursor.rowcount
                elif params:
                    if isinstance(params, dict):
                        cursor.execute(sql, tuple(params.values()))
                    else:
                        cursor.execute(sql, params)
                    affected_rows = cursor.rowcount
                else:
                    cursor.execute(sql)
                    affected_rows = cursor.rowcount

                if commit:
                    connection.commit()
                    self.logger.info(f"DML executado e commitado. {affected_rows} linhas afetadas.")
                else:
                    self.logger.info(f"DML executado sem commit. {affected_rows} linhas afetadas.")

                return affected_rows

        except pyodbc.Error as e:
            self.logger.error(f"Erro ao executar DML: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            raise

    def execute_procedure(self, procedure_name: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """
        Executa uma stored procedure.

        Args:
            procedure_name (str): Nome da procedure
            params (list, optional): Parâmetros da procedure

        Returns:
            List[Dict[str, Any]]: Resultado da procedure se houver SELECT
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()

                if params:
                    placeholders = ','.join(['?' for _ in params])
                    call_sql = f"EXEC {procedure_name} {placeholders}"
                    cursor.execute(call_sql, params)
                else:
                    cursor.execute(f"EXEC {procedure_name}")

                # Verificar se há resultados para buscar
                results = []
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    results = [dict(zip(columns, row)) for row in rows]

                connection.commit()
                self.logger.info(f"Procedure {procedure_name} executada com sucesso.")
                return results

        except pyodbc.Error as e:
            self.logger.error(f"Erro ao executar procedure {procedure_name}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            raise

    def get_table_data(self, table_name: str, limit: int = 10,
                       filters: Optional[Dict] = None, schema: str = "dbo") -> List[Dict[str, Any]]:
        """
        Consulta genérica para uma tabela.

        Args:
            table_name (str): Nome da tabela
            limit (int): Limite de registros (padrão: 10)
            filters (dict, optional): Filtros para aplicar na consulta
            schema (str): Schema da tabela (padrão: dbo)

        Returns:
            List[Dict[str, Any]]: Lista com os dados da tabela
        """
        base_sql = f"SELECT * FROM {schema}.{table_name}"
        where_conditions = []
        params = []

        if filters:
            for key, value in filters.items():
                if value is not None:
                    where_conditions.append(f"{key} = ?")
                    params.append(value)

        if where_conditions:
            base_sql += " WHERE " + " AND ".join(where_conditions)

        if limit > 0:
            base_sql = f"SELECT TOP {limit} * FROM ({base_sql}) AS subquery"

        return self.execute_query(base_sql, tuple(params) if params else None)

    def get_available_drivers(self) -> List[str]:
        """
        Lista os drivers ODBC disponíveis no sistema.

        Returns:
            List[str]: Lista de drivers disponíveis
        """
        try:
            drivers = pyodbc.drivers()
            self.logger.info(f"Drivers ODBC disponíveis: {drivers}")
            return drivers
        except Exception as e:
            self.logger.error(f"Erro ao listar drivers: {e}")
            return []

    def test_connection_detailed(self) -> Dict[str, Any]:
        """
        Testa a conexão com informações detalhadas de diagnóstico.

        Returns:
            Dict[str, Any]: Informações detalhadas do teste
        """
        result = {
            'success': False,
            'connection_string': '',
            'drivers_available': [],
            'error_message': '',
            'server_info': {}
        }

        try:
            # Listar drivers disponíveis
            result['drivers_available'] = self.get_available_drivers()

            # Construir e mostrar string de conexão
            connection_string = self._build_connection_string()
            result['connection_string'] = connection_string
            self.logger.info(f"Tentando conectar com: {connection_string}")

            with self.get_connection() as connection:
                cursor = connection.cursor()

                # Teste básico
                cursor.execute("SELECT 1 as test")
                test_result = cursor.fetchone()

                # Informações do servidor
                cursor.execute("SELECT @@VERSION as version, @@SERVERNAME as server_name, DB_NAME() as database_name")
                server_info = cursor.fetchone()

                if test_result and test_result[0] == 1:
                    result['success'] = True
                    result['server_info'] = {
                        'version': server_info[0] if server_info else 'N/A',
                        'server_name': server_info[1] if server_info else 'N/A',
                        'database_name': server_info[2] if server_info else 'N/A'
                    }
                    self.logger.info("✓ Teste de conexão detalhado bem-sucedido.")

        except Exception as e:
            result['error_message'] = str(e)
            self.logger.error(f"✗ Teste de conexão detalhado falhou: {e}")

        return result

    def test_connection(self) -> bool:
        """
        Testa a conexão com o banco de dados (método simples).

        Returns:
            bool: True se a conexão foi bem-sucedida
        """
        result = self.test_connection_detailed()
        return result['success']

    # def get_tables_list(self, schema: str = "dbo") -> List[str]:
    #     """
    #     Retorna informações sobre as colunas de uma tabela.

    #     Args:
    #         table_name (str): Nome da tabela
    #         schema (str): Schema da tabela (padrão: dbo)

    #     Returns:
    #         List[Dict[str, Any]]: Informações das colunas
    #     """
    #     sql = """
    #         SELECT
    #             COLUMN_NAME,
    #             DATA_TYPE,
    #             IS_NULLABLE,
    #             COLUMN_DEFAULT,
    #             CHARACTER_MAXIMUM_LENGTH,
    #             NUMERIC_PRECISION,
    #             NUMERIC_SCALE
    #         FROM INFORMATION_SCHEMA.COLUMNS
    #         WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
    #         ORDER BY ORDINAL_POSITION
    #     """
    #     return self.execute_query(sql, (table_name, schema))

    def close_pool(self) -> None:
        """
        Fecha o pool de conexões.
        """
        if self.pool:
            for conn in self.pool:
                try:
                    conn.close()
                except Exception:
                    pass
            self.pool.clear()
            self.pool_in_use.clear()
            self.logger.info("Pool de conexões fechado.")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_pool()


# Exemplo de uso
if __name__ == "__main__":
    # Configuração de logging
    logging.basicConfig(level=logging.INFO)

    # Dados de conexão - Exemplo com SQL Server Authentication
    db_config_sql = {
        'server': 'RPH-SRV',  # ou 'servidor.dominio.com'
        'database': 'W_Access',
        'username': 'seu_usuario',
        'password': 'sua_senha',
        'integrated_security': False  # SQL Server Authentication
    }

    # Dados de conexão - Exemplo com Windows Authentication
    db_config_windows = {
        'server': 'RPH-SRV',
        'database': 'MPPR',
        'integrated_security': True  # Windows Authentication
    }

    # Teste com SQL Server Authentication
    print("=== Testando SQL Server Authentication ===")
    try:
        db = SQLServerDBManager(**db_config_sql)

        # Teste detalhado de conexão
        test_result = db.test_connection_detailed()
        print(f"Drivers disponíveis: {test_result['drivers_available']}")
        print(f"String de conexão: {test_result['connection_string']}")

        if test_result['success']:
            print("✓ Conexão SQL Server Auth estabelecida com sucesso!")
            print(f"Servidor: {test_result['server_info']}")

            # Listar tabelas
            tables = db.get_tables_list()
            print(f"\n📋 Tabelas encontradas: {len(tables)}")
            for table in tables[:5]:  # Mostrar apenas as primeiras 5
                print(f"  - {table}")

        else:
            print(f"✗ Falha na conexão: {test_result['error_message']}")

    except Exception as e:
        print(f"❌ Erro SQL Server Auth: {e}")

    # Teste com Windows Authentication
    print("\n=== Testando Windows Authentication ===")
    try:
        db_win = SQLServerDBManager(**db_config_windows)

        test_result = db_win.test_connection_detailed()
        print(f"String de conexão: {test_result['connection_string']}")

        if test_result['success']:
            print("✓ Conexão Windows Auth estabelecida com sucesso!")
            print(f"Servidor: {test_result['server_info']}")

            tables = db_win.get_tables_list()
            print(f"📋 Tabelas encontradas: {len(tables)}")
        else:
            print(f"✗ Falha na conexão: {test_result['error_message']}")

    except Exception as e:
        print(f"❌ Erro Windows Auth: {e}")

    # Uso com pool de conexões (recomendado para aplicações)
    print("\n" + "=" * 50)
    print("Testando com pool de conexões:")

    try:
        with SQLServerDBManager(**db_config_sql, pool_size=3) as db:
            # Criar pool
            db.create_pool()

            # Múltiplas consultas usando o pool
            for i in range(3):
                tables = db.get_tables_list()
                print(f"Consulta {i+1}: {len(tables)} tabelas encontradas")

    except Exception as e:
        print(f"❌ Erro com pool: {e}")

    # Exemplos de diferentes configurações de autenticação
    print("\n" + "=" * 60)
    print("📝 EXEMPLOS DE CONFIGURAÇÃO:")
    print("=" * 60)

    print("\n1️⃣ SQL Server Authentication:")
    print("""
    db_config = {
        'server': 'localhost\\SQLEXPRESS',
        'database': 'MinhaBaseDados',
        'username': 'meu_usuario',
        'password': 'minha_senha',
        'integrated_security': False
    }
    """)

    print("2️⃣ Windows Authentication:")
    print("""
    db_config = {
        'server': 'servidor.dominio.com',
        'database': 'MinhaBaseDados',
        'integrated_security': True,
        'encrypt': False,
        'trust_server_certificate': True
    }
    """)

    print("3️⃣ Diagnóstico de problemas de conexão:")
    print("""
    # Para diagnosticar problemas, use:
    db = SQLServerDBManager(**db_config)
    test_result = db.test_connection_detailed()
    print("Drivers:", test_result['drivers_available'])
    print("String:", test_result['connection_string'])
    print("Erro:", test_result['error_message'])
    """)

    print("3️⃣ Servidor remoto com configurações de segurança:")
    print("""
    db_config = {
        'server': '192.168.1.100,1433',
        'database': 'MinhaBaseDados',
        'username': 'app_user',
        'password': 'senha_segura',
        'integrated_security': False,
        'encrypt': True,
        'trust_server_certificate': False
    }
    """)
