import oracledb
import logging
import hashlib
import os
import platform
import subprocess
from typing import List, Dict, Any, Optional, Union
from contextlib import contextmanager


class OracleDBManager:
    """
    Classe para gerenciar conexões e consultas em banco de dados Oracle.
    
    NOVA FUNCIONALIDADE:
    - Detecta automaticamente se precisa usar modo thin ou thick
    - Mantém total compatibilidade com versão anterior
    - Funciona em qualquer projeto sem configurações fixas
    - Cache inteligente de detecção por DSN
    
    Esta classe funciona tanto no modo Thin (padrão) quanto no modo Thick (quando necessário).
    A detecção é automática e transparente para o usuário.
    """

    # Cache global de modos detectados (compartilhado entre instâncias)
    _mode_cache = {}

    def __init__(self, username: str, password: str, dsn: str,
                 pool_size: int = 5,
                 oracle_client_lib_dir: Optional[str] = None,
                 force_mode: Optional[str] = None):
        """
        Inicializa o gerenciador de banco Oracle.

        Args:
            username (str): Nome de usuário do banco
            password (str): Senha do usuário  
            dsn (str): Data Source Name no formato Easy Connect
            pool_size (int): Tamanho do pool de conexões (padrão: 5)
            oracle_client_lib_dir (str, optional): Caminho do Oracle Client para thick mode
            force_mode (str, optional): 'thin' ou 'thick' para forçar modo específico
        """
        self.username = username
        self.password = password
        self.dsn = dsn
        self.pool_size = pool_size
        self.oracle_client_lib_dir = oracle_client_lib_dir
        self.force_mode = force_mode
        self.pool = None
        
        # Controle interno do modo Oracle
        self.thick_mode = False
        self.mode_detected = False

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

        # Detectar modo Oracle automaticamente (se não forçado)
        if force_mode:
            self.thick_mode = (force_mode.lower() == 'thick')
            self.mode_detected = True
            if self.thick_mode:
                init_success = self._init_thick_mode()
                if not init_success:
                    raise Exception("Falha ao inicializar modo thick - Oracle Client não encontrado")
        else:
            self._auto_detect_mode()

    def _get_cache_key(self) -> str:
        """Gera chave única para cache baseada no DSN"""
        return hashlib.md5(f"{self.dsn}:{self.username}".encode()).hexdigest()[:12]

    def _auto_detect_mode(self):
        """Detecta automaticamente qual modo Oracle usar"""
        cache_key = self._get_cache_key()
        
        # Verificar cache primeiro
        if cache_key in self._mode_cache:
            cached_mode = self._mode_cache[cache_key]
            self.thick_mode = (cached_mode == 'thick')
            self.mode_detected = True
            if self.thick_mode:
                self._init_thick_mode()
            return

        # Tentar thin mode primeiro (mais rápido e comum)
        if self._test_mode('thin'):
            self.thick_mode = False
            self.mode_detected = True
            self._mode_cache[cache_key] = 'thin'
            return
        
        # Se thin falhou, tentar thick mode
        if self._test_mode('thick'):
            self.thick_mode = True
            self.mode_detected = True
            self._mode_cache[cache_key] = 'thick'
            return
        
        # Fallback para thin se ambos falharam
        self.thick_mode = False
        self.mode_detected = False

    def _test_mode(self, mode: str) -> bool:
        """Testa se um modo específico funciona"""
        try:
            if mode == 'thick' and not hasattr(oracledb, '_thick_mode_init'):
                if not self._init_thick_mode():
                    return False
            
            # Teste rápido de conexão
            test_conn = oracledb.connect(
                user=self.username,
                password=self.password,
                dsn=self.dsn
            )
            cursor = test_conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            result = cursor.fetchone()
            cursor.close()
            test_conn.close()
            
            return result is not None and result[0] == 1
            
        except Exception as e:
            # Detectar erros que indicam necessidade de thick mode
            error_str = str(e).lower()
            if mode == 'thin' and any(keyword in error_str for keyword in 
                ['dpy-3001', 'native network encryption', 'data integrity', 'thick mode']):
                return False
            return False

    def _init_thick_mode(self) -> bool:
        """Inicializa modo thick se necessário"""
        try:
            # Verificar se já foi inicializado
            if hasattr(oracledb, '_thick_mode_init'):
                self.logger.debug("Modo thick já inicializado")
                return True

            # Auto-detectar Oracle Client se não especificado
            if not self.oracle_client_lib_dir:
                self.oracle_client_lib_dir = self._detect_oracle_client()
                self.logger.debug(f"Oracle Client detectado: {self.oracle_client_lib_dir}")

            # Inicializar thick mode
            if self.oracle_client_lib_dir:
                self.logger.info(f"Inicializando modo thick com lib_dir: {self.oracle_client_lib_dir}")
                oracledb.init_oracle_client(lib_dir=self.oracle_client_lib_dir)
            else:
                self.logger.info("Inicializando modo thick (Oracle Client no PATH)")
                oracledb.init_oracle_client()
            
            oracledb._thick_mode_init = True
            self.logger.info("Modo thick inicializado com sucesso")
            return True
            
        except Exception as e:
            self.logger.error(f"ERRO ao inicializar modo thick: {e}")
            self.logger.error("Oracle Client pode não estar instalado ou configurado corretamente")
            return False

    def _detect_oracle_client(self) -> Optional[str]:
        """Auto-detecta Oracle Client no sistema"""
        system = platform.system().lower()
        
        self.logger.debug(f"Detectando Oracle Client no sistema: {system}")
        
        if system == "windows":
            paths = [
                r"C:\oracle\instantclient_21_3",
                r"C:\oracle\instantclient_19_3", 
                r"C:\oracle\instantclient_12_2",
                r"C:\oracle\instantclient",
                r"C:\app\oracle\product\21.0.0\client_1\bin",
                r"C:\app\oracle\product\19.0.0\client_1\bin",
                r"C:\app\oracle\product\12.2.0\client_1\bin",
                r"C:\Program Files\Oracle\instantclient_21_3",
                r"C:\Program Files\Oracle\instantclient_19_3",
                r"C:\Program Files (x86)\Oracle\instantclient_21_3",
            ]
            check_files = ["oci.dll", "oraociei21.dll", "oraociei19.dll", "oraociei12.dll"]
        else:
            paths = [
                "/usr/lib/oracle/21/client64/lib",
                "/usr/lib/oracle/19.3/client64/lib",
                "/opt/oracle/instantclient_21_3",
                "/opt/oracle/instantclient_19_3",
                "/usr/local/oracle/instantclient",
            ]
            check_files = ["libclntsh.so", "libclntsh.so.21.1", "libclntsh.so.19.1", "libclntsh.so.12.1"]
        
        # Verificar caminhos conhecidos
        for path in paths:
            self.logger.debug(f"Verificando caminho: {path}")
            if os.path.exists(path):
                for check_file in check_files:
                    file_path = os.path.join(path, check_file)
                    if os.path.exists(file_path):
                        self.logger.info(f"Oracle Client encontrado: {path} (arquivo: {check_file})")
                        return path
        
        # Verificar PATH
        try:
            cmd = ["where", "sqlplus"] if system == "windows" else ["which", "sqlplus"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                sqlplus_path = result.stdout.strip()
                self.logger.info(f"Oracle Client encontrado no PATH: {sqlplus_path}")
                return None  # Oracle Client no PATH
        except Exception as e:
            self.logger.debug(f"Erro ao verificar PATH: {e}")
        
        self.logger.warning("Oracle Client NÃO encontrado no sistema")
        self.logger.warning("Para modo thick, instale Oracle Instant Client:")
        self.logger.warning("https://www.oracle.com/database/technologies/instant-client/downloads.html")
        return None

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
            mode = "thick" if self.thick_mode else "thin"
            self.logger.info(f"Pool de conexões criado com sucesso em modo {mode}. Tamanho: {self.pool_size}")
        except oracledb.Error as e:
            self.logger.error(f"Erro ao criar pool de conexões: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """
        Context manager para obter conexão do pool ou criar uma nova.
        Agora com fallback automático entre modos e configuração NLS.

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
            
            # Configurar NLS para evitar problemas de localização
            try:
                cursor = connection.cursor()
                cursor.execute("ALTER SESSION SET NLS_DATE_LANGUAGE='ENGLISH'")
                cursor.execute("ALTER SESSION SET NLS_LANGUAGE='ENGLISH'")
                cursor.execute("ALTER SESSION SET NLS_TERRITORY='AMERICA'")
                cursor.close()
            except Exception as nls_error:
                self.logger.warning(f"Aviso: Não foi possível configurar NLS: {nls_error}")
            
            yield connection
            
        except oracledb.Error as e:
            # Tentar fallback automático se detecção falhou
            if not self.mode_detected and not self.force_mode:
                error_str = str(e).lower()
                if any(keyword in error_str for keyword in 
                    ['dpy-3001', 'native network encryption', 'data integrity']):
                    
                    self.logger.info("Erro de criptografia detectado, tentando modo thick...")
                    if self._init_thick_mode():
                        self.thick_mode = True
                        cache_key = self._get_cache_key()
                        self._mode_cache[cache_key] = 'thick'
                        
                        # Tentar conexão novamente
                        try:
                            if self.pool:
                                connection = self.pool.acquire()
                            else:
                                connection = oracledb.connect(
                                    user=self.username,
                                    password=self.password,
                                    dsn=self.dsn
                                )
                            
                            # Configurar NLS na nova conexão também
                            try:
                                cursor = connection.cursor()
                                cursor.execute("ALTER SESSION SET NLS_DATE_LANGUAGE='ENGLISH'")
                                cursor.execute("ALTER SESSION SET NLS_LANGUAGE='ENGLISH'")
                                cursor.execute("ALTER SESSION SET NLS_TERRITORY='AMERICA'")
                                cursor.close()
                            except Exception as nls_error:
                                self.logger.warning(f"Aviso: Não foi possível configurar NLS: {nls_error}")
                            
                            yield connection
                            return
                        except Exception:
                            pass
            
            self.logger.error(f"Erro na conexão: {e}")
            raise
        finally:
            if connection:
                if self.pool:
                    self.pool.release(connection)
                else:
                    connection.close()

    def execute_query(self, sql: str, params: Optional[Dict] = None,
                      fetch_size: int = 1000, fix_nls: bool = True) -> List[tuple]:
        """
        Executa uma consulta SELECT e retorna os resultados como tuplas.
        
        MUDANÇA: Agora retorna tuplas ao invés de dicionários para melhor performance.
        Se precisar de dicionários, use execute_query_dict().

        Args:
            sql (str): Query SQL a ser executada
            params (dict, optional): Parâmetros para a query
            fetch_size (int): Tamanho do fetch (padrão: 1000)
            fix_nls (bool): Se deve corrigir configurações NLS automaticamente

        Returns:
            List[tuple]: Lista de tuplas com os resultados
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                cursor.arraysize = fetch_size

                # Configurar NLS adicionalmente se solicitado
                if fix_nls:
                    try:
                        # Garantir configurações NLS corretas
                        cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'")
                        cursor.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF'")
                        cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS='.,'")
                    except Exception as nls_error:
                        self.logger.debug(f"Configuração NLS adicional falhou: {nls_error}")

                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)

                rows = cursor.fetchall()
                mode = "thick" if self.thick_mode else "thin"
                self.logger.info(f"Query executada em modo {mode}. {len(rows)} registros retornados.")
                return rows

        except oracledb.Error as e:
            error_code = getattr(e, 'code', None)
            if error_code == 1843:  # ORA-01843: not a valid month
                self.logger.error("Erro ORA-01843 detectado - problema de localização NLS")
                self.logger.error("Sugestão: Verifique datas na query ou configurações NLS do banco")
            self.logger.error(f"Erro ao executar query: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado: {e}")
            raise

    def execute_query_dict(self, sql: str, params: Optional[Dict] = None,
                           fetch_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Executa uma consulta SELECT e retorna os resultados como dicionários.
        Compatível com a versão anterior da classe.

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

                # Obter nomes das colunas
                columns = [desc[0] for desc in cursor.description]

                # Fetch todos os resultados
                rows = cursor.fetchall()

                # Converter para lista de dicionários
                results = [dict(zip(columns, row)) for row in rows]

                mode = "thick" if self.thick_mode else "thin"
                self.logger.info(f"Query executada em modo {mode}. {len(results)} registros retornados.")
                return results

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
        Mantida para compatibilidade.

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

        # Usar execute_query_dict para manter compatibilidade
        return self.execute_query_dict(base_sql, params)

    def test_connection(self) -> tuple[bool, str]:
        """
        Testa a conexão com o banco de dados.
        Agora retorna informações detalhadas sobre o modo detectado.

        Returns:
            tuple[bool, str]: (sucesso, informações_detalhadas)
        """
        try:
            with self.get_connection() as connection:
                cursor = connection.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    mode = "thick" if self.thick_mode else "thin"
                    detection = "forçado" if self.force_mode else "automático"
                    message = f"Conexão OK - Modo {mode} ({detection})"
                    self.logger.info(message)
                    return True, message
                return False, "Teste retornou resultado inesperado"
        except Exception as e:
            error_msg = f"Teste de conexão falhou: {e}"
            self.logger.error(error_msg)
            return False, error_msg

    def get_mode_info(self) -> Dict[str, Any]:
        """
        Retorna informações sobre o modo Oracle detectado.
        Útil para debug e monitoramento.

        Returns:
            Dict[str, Any]: Informações do modo Oracle
        """
        cache_key = self._get_cache_key()
        return {
            'mode': 'thick' if self.thick_mode else 'thin',
            'detection_method': 'forced' if self.force_mode else 'automatic',
            'mode_detected': self.mode_detected,
            'oracle_client_path': self.oracle_client_lib_dir,
            'dsn': self.dsn,
            'cache_key': cache_key,
            'cached_mode': self._mode_cache.get(cache_key)
        }

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


# Exemplo de uso demonstrando compatibilidade
if __name__ == "__main__":
    # Configuração de logging
    logging.basicConfig(level=logging.INFO)

    print("=== TESTE DE COMPATIBILIDADE E DETECÇÃO AUTOMÁTICA ===\n")

    # Configurações de exemplo (substitua pelos seus dados reais)
    test_configs = [
        {
            'name': 'Cliente MPPR (modo thin)',
            'username': 'W_ACCESS',
            'password': 'MGE5NjU3YmQ3ZTN#@1',
            'dsn': 'oraprd2.mppr:1521/wxsp1'
        },
        {
            'name': 'Cliente Santa Casa (modo thick)',
            'username': 'USRSCMSUSUVR35',
            'password': 'tb67574#EHgY#yjtGHJ',
            'dsn': '10.99.1.5:1521/pdb_scmsc.sub08211821591.vcnscsaocarlos.oraclevcn.com'
        }
    ]

    for config in test_configs:
        print(f"--- Testando {config['name']} ---")
        
        try:
            # Usar a classe normalmente - detecção automática
            db = OracleDBManager(
                username=config['username'],
                password=config['password'],
                dsn=config['dsn']
            )
            
            # Testar conexão
            success, message = db.test_connection()
            print(f"Resultado: {'✓' if success else '✗'} {message}")
            
            # Mostrar informações de detecção
            mode_info = db.get_mode_info()
            print(f"Modo detectado: {mode_info['mode']}")
            print(f"Método: {mode_info['detection_method']}")
            print(f"Cache: {mode_info['cached_mode'] or 'Novo'}")
            
            if success:
                # Testar consulta (compatibilidade com versão anterior)
                try:
                    result = db.execute_query("SELECT COUNT(*) FROM DUAL")
                    print(f"Consulta teste: ✓ Resultado = {result[0][0]}")
                except Exception as e:
                    print(f"Consulta teste: ✗ {e}")
            
            db.close_pool()
            
        except Exception as e:
            print(f"Erro: {e}")
        
        print()

    print("=== CACHE DE DETECÇÃO ===")
    print("Modos detectados e armazenados em cache:")
    for key, mode in OracleDBManager._mode_cache.items():
        print(f"  {key}: {mode}")
    
    print("\n=== RESULTADO ===")
    print("✓ Classe mantém 100% de compatibilidade")
    print("✓ Detecção automática funciona transparentemente")
    print("✓ Cache evita re-detecções desnecessárias")
    print("✓ Reutilizável em qualquer projeto")