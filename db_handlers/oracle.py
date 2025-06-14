import oracledb
import logging
from typing import List, Dict, Any, Optional, Union
from contextlib import contextmanager


class OracleDBManager:
    """
    Classe para gerenciar conexões e consultas em banco de dados Oracle.

    Esta classe utiliza o modo Thin do oracledb, que não requer instalação
    do Oracle Client, funcionando apenas com Python 3.7+ e oracledb >= 1.1
    """

    def __init__(self, username: str, password: str, dsn: str,
                 pool_size: int = 5):
        """
        Inicializa o gerenciador de banco Oracle.

        Args:
            username (str): Nome de usuário do banco
            password (str): Senha do usuário
            dsn (str): Data Source Name no formato Easy Connect
            pool_size (int): Tamanho do pool de conexões (padrão: 5)
        """
        self.username = username
        self.password = password
        self.dsn = dsn
        self.pool_size = pool_size
        self.pool = None

        # Configurar logging
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def create_pool(self) -> None:
        """
        Cria um pool de conexões para melhor performance.
        """
        try:
            self.pool = oracledb.create_pool(
                user=self.username,
                password=self.password,
                dsn=self.dsn,
                min=1,
                max=self.pool_size,
                increment=1
            )
            self.logger.info(f"Pool de conexões criado com sucesso. Tamanho: {self.pool_size}")
        except oracledb.Error as e:
            self.logger.error(f"Erro ao criar pool de conexões: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        Context manager para obter conexão do pool ou criar uma nova.

        Yields:
            oracledb.Connection: Conexão com o banco de dados
        """
        connection = None
        try:
            if self.pool:
                connection = self.pool.acquire()
            else:
                connection = oracledb.connect(
                    user=self.username,
                    password=self.password,
                    dsn=self.dsn
                )
            yield connection
        except oracledb.Error as e:
            self.logger.error(f"Erro na conexão: {e}")
            raise
        finally:
            if connection:
                if self.pool:
                    self.pool.release(connection)
                else:
                    connection.close()

    def execute_query(self, sql: str, params: Optional[Dict] = None,
                      fetch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Executa uma consulta SELECT e retorna os resultados.

        Args:
            sql (str): Query SQL a ser executada
            params (dict, optional): Parâmetros para a query
            fetch_size (int): Tamanho do fetch (padrão: 1000)

        Returns:
            List[Dict[str, Any]]: Lista de dicionários com os resultados
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                cursor.arraysize = fetch_size

                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                # # Obter nomes das colunas
                # columns = [desc[0] for desc in cursor.description]

                # Fetch todos os resultados
                rows = cursor.fetchall()

                # # Converter para lista de dicionários
                # results = [dict(zip(columns, row)) for row in rows]

                self.logger.info(f"Query executada com sucesso. {len(rows)} registros retornados.")
                return rows

        except oracledb.Error as e:
            self.logger.error(f"Erro ao executar query: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            raise

    def execute_dml(self, sql: str, params: Optional[Union[Dict, List[Dict]]] = None,
                    commit: bool = True) -> int:
        """
        Executa comandos DML (INSERT, UPDATE, DELETE).

        Args:
            sql (str): Comando SQL DML
            params (dict ou list, optional): Parâmetros para o comando
            commit (bool): Se deve fazer commit automático (padrão: True)

        Returns:
            int: Número de linhas afetadas
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()

                if isinstance(params, list):
                    # Executar em lote
                    cursor.executemany(sql, params)
                    affected_rows = cursor.rowcount
                elif params:
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

        except oracledb.Error as e:
            self.logger.error(f"Erro ao executar DML: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            raise

    def execute_procedure(self, procedure_name: str, params: Optional[Dict] = None) -> Dict:
        """
        Executa uma stored procedure.

        Args:
            procedure_name (str): Nome da procedure
            params (dict, optional): Parâmetros da procedure

        Returns:
            dict: Parâmetros de saída da procedure
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()

                if params:
                    result = cursor.callproc(procedure_name, list(params.values()))
                    # Mapear resultado de volta para os nomes dos parâmetros
                    output_params = dict(zip(params.keys(), result))
                else:
                    cursor.callproc(procedure_name)
                    output_params = {}

                self.logger.info(f"Procedure {procedure_name} executada com sucesso.")
                return output_params

        except oracledb.Error as e:
            self.logger.error(f"Erro ao executar procedure {procedure_name}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            raise

    def get_pessoa_controle_acesso(self, limit: int = 10,
                                   filters: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Consulta específica para a view VW_WA_PESSOA_CONTROLE_ACESSO.

        Args:
            limit (int): Limite de registros (padrão: 10)
            filters (dict, optional): Filtros para aplicar na consulta

        Returns:
            List[Dict[str, Any]]: Lista com os dados das pessoas
        """
        base_sql = "SELECT * FROM EADM.VW_WA_PESSOA_CONTROLE_ACESSO"
        where_conditions = []
        params = {}

        if filters:
            for key, value in filters.items():
                if value is not None:
                    where_conditions.append(f"{key} = :{key}")
                    params[key] = value

        if where_conditions:
            base_sql += " WHERE " + " AND ".join(where_conditions)

        if limit > 0:
            base_sql += f" AND ROWNUM <= {limit}"
        elif not where_conditions and limit > 0:
            base_sql += f" WHERE ROWNUM <= {limit}"

        return self.execute_query(base_sql, params)

    def test_connection(self) -> bool:
        """
        Testa a conexão com o banco de dados.

        Returns:
            bool: True se a conexão foi bem-sucedida
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    self.logger.info("Teste de conexão bem-sucedido.")
                    return True
                return False
        except Exception as e:
            self.logger.error(f"Teste de conexão falhou: {e}")
            return False

    def close_pool(self) -> None:
        """
        Fecha o pool de conexões.
        """
        if self.pool:
            self.pool.close()
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

    # Dados de conexão
    db_config = {
        'username': 'seu_usuario',
        'password': 'sua_senha',
        'dsn': 'oraprd2.mppr:1521/wxsp1'
    }

    # Uso básico sem pool
    try:
        db = OracleDBManager(**db_config)

        # Testar conexão
        if db.test_connection():
            print("Conexão estabelecida com sucesso!")

            # Consulta específica
            pessoas = db.get_pessoa_controle_acesso(limit=5)
            print(f"\nEncontrados {len(pessoas)} registros:")
            for pessoa in pessoas:
                print(f"  - {pessoa}")

            # Consulta personalizada
            sql_custom = """
                SELECT COUNT(*) as TOTAL_REGISTROS
                FROM EADM.VW_WA_PESSOA_CONTROLE_ACESSO
            """
            resultado = db.execute_query(sql_custom)
            print(f"\nTotal de registros na view: {resultado[0]['TOTAL_REGISTROS']}")

    except Exception as e:
        print(f"Erro: {e}")

    # Uso com pool de conexões (recomendado para aplicações)
    print("\n" + "=" * 50)
    print("Testando com pool de conexões:")

    try:
        with OracleDBManager(**db_config, pool_size=3) as db:
            # Criar pool
            db.create_pool()

            # Múltiplas consultas usando o pool
            for i in range(3):
                pessoas = db.get_pessoa_controle_acesso(limit=2)
                print(f"Consulta {i+1}: {len(pessoas)} registros")

    except Exception as e:
        print(f"❌ Erro com pool: {e}")
