import sqlite3
import hashlib
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path


class LocalCache:
    """
    Cache SQLite persistente e otimizado para qualquer classe de banco de dados.
    Trabalha diretamente com tuplas para máxima performance.
    Cache principal: sem TTL (dados indefinidamente)
    Cache de deletados: TTL de 24 horas com limpeza automática
    """

    def __init__(self, db_connection_class,
                 cache_file: str = None,
                 keep_deleted_hours: int = 24):
        """
        Args:
            db_connection_class: Instância de qualquer classe de banco
            cache_file: Nome do arquivo de cache (auto-gerado se None)
            keep_deleted_hours: Horas para manter registros deletados (padrão: 24h)
        """
        self.db_connection = db_connection_class

        if cache_file is None:
            cache_file = self._generate_persistent_cache_name()

        self.cache_file = Path(cache_file)
        self.keep_deleted_hours = keep_deleted_hours
        self._init_cache_db()

    def _generate_persistent_cache_name(self) -> str:
        """
        Gera nome de cache consistente baseado nos dados de conexão.
        """
        class_name = self.db_connection.__class__.__name__

        # Extrair dados de conexão para criar identificador único
        connection_info = []
        common_attrs = ['username', 'user', 'dsn', 'host', 'server', 'database', 'connection_string']

        for attr in common_attrs:
            if hasattr(self.db_connection, attr):
                value = getattr(self.db_connection, attr)
                if value:
                    connection_info.append(f"{attr}={value}")

        if not connection_info:
            connection_info = [repr(self.db_connection)]

        connection_str = "|".join(connection_info)
        connection_hash = hashlib.md5(connection_str.encode()).hexdigest()[:8]

        return f"cache_{class_name}_{connection_hash}.db"

    def _init_cache_db(self):
        """Inicializa banco SQLite para cache"""
        with sqlite3.connect(self.cache_file) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS query_cache (
                    query_hash TEXT PRIMARY KEY,
                    query_sql TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_rows INTEGER
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS row_cache (
                    query_hash TEXT,
                    row_hash TEXT,
                    row_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (query_hash, row_hash)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS deleted_rows (
                    query_hash TEXT,
                    row_hash TEXT,
                    row_data TEXT,
                    deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (query_hash, row_hash)
                )
            """)

            conn.execute("CREATE INDEX IF NOT EXISTS idx_query_hash ON row_cache(query_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_deleted_query ON deleted_rows(query_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_deleted_at ON deleted_rows(deleted_at)")

    def _execute_on_database(self, sql: str, params: Dict = None) -> List[tuple]:
        """
        Executa query na classe de banco fornecida.
        """
        try:
            if hasattr(self.db_connection, 'execute_query'):
                result = self.db_connection.execute_query(sql, params)
            elif hasattr(self.db_connection, 'query'):
                result = self.db_connection.query(sql, params)
            elif hasattr(self.db_connection, 'fetch_all'):
                result = self.db_connection.fetch_all(sql, params)
            elif hasattr(self.db_connection, 'select'):
                result = self.db_connection.select(sql, params)
            else:
                raise AttributeError(
                    f"Classe {self.db_connection.__class__.__name__} não possui método "
                    "compatível (execute_query, query, fetch_all, ou select)"
                )

            return self._ensure_tuple_format(result)

        except Exception as e:
            raise Exception(f"Erro ao executar query no banco: {e}")

    def _ensure_tuple_format(self, result: Any) -> List[tuple]:
        """Garante formato de lista de tuplas"""
        if not result:
            return []

        if isinstance(result, list) and len(result) > 0:
            first_item = result[0]
            if isinstance(first_item, tuple):
                return result
            elif isinstance(first_item, list):
                return [tuple(row) for row in result]
            elif isinstance(first_item, dict):
                return [tuple(row.values()) for row in result]
        elif isinstance(result, tuple):
            return [result]

        return []

    def _get_query_hash(self, sql: str, params: Dict = None) -> str:
        """Gera hash único para query + parâmetros"""
        query_str = sql + str(params or {})
        return hashlib.md5(query_str.encode()).hexdigest()

    def _get_row_hash(self, row: tuple) -> str:
        """Gera hash único para uma tupla"""
        row_str = '|'.join(str(item) for item in row)
        return hashlib.md5(row_str.encode()).hexdigest()

    def _cache_exists(self, query_hash: str) -> bool:
        """Verifica se existe entrada para esta query no cache"""
        with sqlite3.connect(self.cache_file) as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM query_cache WHERE query_hash = ?
            """, (query_hash,))
            return cursor.fetchone()[0] > 0

    def _cleanup_old_deleted_records(self):
        """
        Remove registros deletados antigos automaticamente (24h).
        Executado automaticamente a cada uso do cache.
        """
        with sqlite3.connect(self.cache_file) as conn:
            cursor = conn.execute("""
                DELETE FROM deleted_rows
                WHERE datetime(deleted_at) < datetime('now', '-{} hours')
            """.format(self.keep_deleted_hours))
            
            deleted_count = conn.total_changes
            if deleted_count > 0:
                print(f"DEBUG - Limpeza automática: {deleted_count} registros deletados removidos (>{self.keep_deleted_hours}h)")
            
            return deleted_count

    def process_select(self, sql: str, params: Dict = None) -> Dict[str, Any]:
        """
        Método principal: executa SELECT e retorna apenas mudanças.
        Cache principal: sem TTL (dados indefinidamente)
        Cache deletados: limpeza automática de 24h

        Args:
            sql: Script SQL SELECT
            params: Parâmetros opcionais da query

        Returns:
            Dict com 'data' (apenas mudanças), 'added', 'removed', 'cache_hit', etc.
        """
        # Limpeza automática dos registros deletados antigos
        self._cleanup_old_deleted_records()
        
        query_hash = self._get_query_hash(sql, params)

        # Verificar se cache existe (cache principal nunca expira)
        cache_exists = self._cache_exists(query_hash)

        print(f"DEBUG - Query hash: {query_hash[:12]}...")
        print(f"DEBUG - Cache exists: {cache_exists}")
        print(f"DEBUG - Cache principal: SEM TTL (dados indefinidamente)")
        print(f"DEBUG - Cache deletados: TTL de {self.keep_deleted_hours}h")

        # Executar query no banco
        new_data = self._execute_on_database(sql, params)

        # Comparar com cache SQLite
        with sqlite3.connect(self.cache_file) as conn:
            # Preparar novos dados
            new_data_by_hash = {}
            new_hashes = set()
            for row_tuple in new_data:
                row_hash = self._get_row_hash(row_tuple)
                new_hashes.add(row_hash)
                new_data_by_hash[row_hash] = row_tuple

            if not cache_exists:
                # Primeira execução - cache não existe
                print(f"DEBUG - Primeira execução: salvando {len(new_data)} registros no cache")
                result = {
                    'data': new_data,
                    'added': new_data,
                    'removed': [],
                    'total_new': len(new_data),
                    'cache_hit': False,
                    'debug_reason': 'first_time'
                }
                self._save_to_cache(conn, query_hash, sql, new_data_by_hash)
                return result

            # Cache existe - comparar (cache principal nunca expira)
            cursor = conn.execute("""
                SELECT row_hash, row_data FROM row_cache WHERE query_hash = ?
            """, (query_hash,))

            cached_data = {}
            cached_hashes = set()
            for row_hash, row_data_json in cursor.fetchall():
                cached_hashes.add(row_hash)
                cached_data[row_hash] = tuple(json.loads(row_data_json))

            print(f"DEBUG - Dados em cache: {len(cached_data)}")
            print(f"DEBUG - Dados novos: {len(new_data_by_hash)}")

            # Calcular diferenças
            added_hashes = new_hashes - cached_hashes
            removed_hashes = cached_hashes - new_hashes

            print(f"DEBUG - Adicionados: {len(added_hashes)}")
            print(f"DEBUG - Removidos: {len(removed_hashes)}")

            added_data = [new_data_by_hash[h] for h in added_hashes]
            removed_data = [cached_data[h] for h in removed_hashes]

            # Mover removidos para histórico
            if removed_hashes:
                self._move_to_deleted(conn, query_hash, removed_hashes, cached_data)

            # Atualizar cache
            self._update_cache(conn, query_hash, sql, new_data_by_hash, added_hashes)

            return {
                'data': added_data,
                'added': added_data,
                'removed': removed_data,
                'total_new': len(new_data),
                'total_cached': len(cached_data),
                'cache_hit': True,
                'debug_reason': 'cache_persistent'
            }

    def get_all_data(self, sql: str, params: Dict = None) -> List[tuple]:
        """
        Retorna TODOS os dados em cache (não apenas mudanças).
        Cache principal nunca expira.
        """
        query_hash = self._get_query_hash(sql, params)

        with sqlite3.connect(self.cache_file) as conn:
            # Como cache principal nunca expira, se existe, é válido
            if not self._cache_exists(query_hash):
                print("DEBUG - Cache não existe, executando no banco")
                return self._execute_on_database(sql, params)

            print("DEBUG - Retornando dados do cache persistente")
            cursor = conn.execute("""
                SELECT row_data FROM row_cache WHERE query_hash = ?
            """, (query_hash,))

            return [tuple(json.loads(row[0])) for row in cursor.fetchall()]

    def get_deleted_records(self, sql: str, params: Dict = None, hours_ago: int = None) -> List[tuple]:
        """
        Retorna registros que foram removidos da tabela.
        Dados deletados são automaticamente limpos após 24h.
        """
        query_hash = self._get_query_hash(sql, params)

        with sqlite3.connect(self.cache_file) as conn:
            if hours_ago:
                cursor = conn.execute("""
                    SELECT row_data, deleted_at FROM deleted_rows
                    WHERE query_hash = ? AND
                          datetime(deleted_at) > datetime('now', '-{} hours')
                    ORDER BY deleted_at DESC
                """.format(hours_ago), (query_hash,))
            else:
                cursor = conn.execute("""
                    SELECT row_data, deleted_at FROM deleted_rows
                    WHERE query_hash = ?
                    ORDER BY deleted_at DESC
                """, (query_hash,))

            results = []
            for row_data_json, deleted_at in cursor.fetchall():
                row_tuple = tuple(json.loads(row_data_json))
                results.append(row_tuple + (deleted_at,))

            return results

    def cleanup_old_deleted_records(self):
        """
        Remove registros deletados antigos manualmente.
        (Também é executado automaticamente a cada uso)
        """
        return self._cleanup_old_deleted_records()

    def clear_cache_completely(self):
        """Limpa todo o cache (manual)"""
        print(f"DEBUG - Limpando cache completamente: {self.cache_file}")

        # Primeira conexão: fazer DELETEs
        with sqlite3.connect(self.cache_file) as conn:
            # Verificar dados antes da limpeza
            cursor = conn.execute("SELECT COUNT(*) FROM row_cache")
            rows_before = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM deleted_rows")
            deleted_before = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM query_cache")
            queries_before = cursor.fetchone()[0]

            print("DEBUG - Antes da limpeza:")
            print(f"  - Linhas em cache: {rows_before}")
            print(f"  - Linhas deletadas: {deleted_before}")
            print(f"  - Queries: {queries_before}")

            # Limpar dados
            conn.execute("DELETE FROM row_cache")
            rows_deleted = conn.total_changes
            print(f"DEBUG - Linhas removidas de row_cache: {rows_deleted}")

            conn.execute("DELETE FROM deleted_rows")
            deleted_removed = conn.total_changes - rows_deleted
            print(f"DEBUG - Linhas removidas de deleted_rows: {deleted_removed}")

            conn.execute("DELETE FROM query_cache")
            queries_removed = conn.total_changes - rows_deleted - deleted_removed
            print(f"DEBUG - Linhas removidas de query_cache: {queries_removed}")

            # Commit explícito
            conn.commit()

        # Segunda conexão: VACUUM (fora da transação)
        conn2 = sqlite3.connect(self.cache_file)
        try:
            conn2.execute("VACUUM")
            print("DEBUG - VACUUM executado")
        finally:
            conn2.close()

        # Terceira conexão: verificar resultado
        with sqlite3.connect(self.cache_file) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM row_cache")
            rows_after = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM deleted_rows")
            deleted_after = cursor.fetchone()[0]

            cursor = conn.execute("SELECT COUNT(*) FROM query_cache")
            queries_after = cursor.fetchone()[0]

            print("DEBUG - Após a limpeza:")
            print(f"  - Linhas em cache: {rows_after}")
            print(f"  - Linhas deletadas: {deleted_after}")
            print(f"  - Queries: {queries_after}")

            total_after = rows_after + deleted_after + queries_after
            if total_after == 0:
                print("DEBUG - Cache limpo com sucesso!")
            else:
                print(f"DEBUG - ERRO: {total_after} registros ainda existem!")

            return total_after == 0

    def get_cache_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas do cache"""
        with sqlite3.connect(self.cache_file) as conn:
            cursor = conn.execute("""
                SELECT
                    COUNT(*) as total_queries,
                    SUM(total_rows) as total_active_rows
                FROM query_cache
            """)
            query_stats = cursor.fetchone()

            cursor = conn.execute("SELECT COUNT(*) FROM deleted_rows")
            deleted_count = cursor.fetchone()[0]

            # Estatísticas de tempo dos dados deletados
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as recent_deleted,
                    MIN(deleted_at) as oldest_deleted,
                    MAX(deleted_at) as newest_deleted
                FROM deleted_rows
                WHERE datetime(deleted_at) > datetime('now', '-{} hours')
            """.format(self.keep_deleted_hours))
            deleted_stats = cursor.fetchone()

            file_size = self.cache_file.stat().st_size if self.cache_file.exists() else 0

            return {
                'total_queries': query_stats[0] or 0,
                'total_active_rows': query_stats[1] or 0,
                'total_deleted_rows': deleted_count,
                'recent_deleted_rows': deleted_stats[0] or 0,
                'oldest_deleted': deleted_stats[1],
                'newest_deleted': deleted_stats[2],
                'cache_file_size_mb': round(file_size / (1024 * 1024), 2),
                'cache_file_path': str(self.cache_file),
                'cache_mode': f'Principal: INDEFINIDO, Deletados: {self.keep_deleted_hours}h TTL'
            }

    def _save_to_cache(self, conn, query_hash: str, sql: str, data_by_hash: Dict[str, tuple]):
        """Salva dados no cache (primeira vez)"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Formato SQLite local

        conn.execute("""
            INSERT OR REPLACE INTO query_cache
            (query_hash, query_sql, created_at, updated_at, total_rows)
            VALUES (?, ?, ?, ?, ?)
        """, (query_hash, sql, now, now, len(data_by_hash)))

        conn.execute("DELETE FROM row_cache WHERE query_hash = ?", (query_hash,))

        rows_to_insert = [
            (query_hash, row_hash, json.dumps(list(row_tuple)))
            for row_hash, row_tuple in data_by_hash.items()
        ]

        conn.executemany("""
            INSERT INTO row_cache (query_hash, row_hash, row_data)
            VALUES (?, ?, ?)
        """, rows_to_insert)

    def _update_cache(self, conn, query_hash: str, sql: str, data_by_hash: Dict[str, tuple], added_hashes: set):
        """Atualiza cache existente"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Formato SQLite local

        conn.execute("""
            UPDATE query_cache
            SET updated_at = ?, total_rows = ?
            WHERE query_hash = ?
        """, (now, len(data_by_hash), query_hash))

        if added_hashes:
            new_rows = [
                (query_hash, row_hash, json.dumps(list(data_by_hash[row_hash])))
                for row_hash in added_hashes
            ]

            conn.executemany("""
                INSERT INTO row_cache (query_hash, row_hash, row_data)
                VALUES (?, ?, ?)
            """, new_rows)

    def _move_to_deleted(self, conn, query_hash: str, removed_hashes: set, cached_data: Dict[str, tuple]):
        """Move registros removidos para histórico"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Formato SQLite local

        deleted_rows = [
            (query_hash, row_hash, json.dumps(list(cached_data[row_hash])), now)
            for row_hash in removed_hashes
        ]

        conn.executemany("""
            INSERT OR REPLACE INTO deleted_rows (query_hash, row_hash, row_data, deleted_at)
            VALUES (?, ?, ?, ?)
        """, deleted_rows)

        placeholders = ','.join(['?' for _ in removed_hashes])
        conn.execute(f"""
            DELETE FROM row_cache
            WHERE query_hash = ? AND row_hash IN ({placeholders})
        """, [query_hash] + list(removed_hashes))


# Exemplo de uso
if __name__ == "__main__":
    # Para testar direto deste arquivo, adicionar path do projeto
    import sys

    # Adicionar diretório raiz do projeto ao path
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))

    try:
        # Agora pode importar normalmente
        from db_handlers.oracle import OracleDBManager

        # Criar conexão e cache (sem TTL no cache principal)
        mppr_db = OracleDBManager('W_ACCESS', 'MGE5NjU3YmQ3ZTN#@1', 'oraprd2.mppr:1521/wxsp1')
        cache = LocalCache(mppr_db, keep_deleted_hours=24)  # Apenas deletados têm TTL

        script = "SELECT * FROM EADM.VW_WA_PESSOA_CONTROLE_ACESSO WHERE ROWNUM <= 10"

        print("=== CACHE PERSISTENTE - SEM TTL NO CACHE PRINCIPAL ===")
        
        # Primeira execução
        print("Primeira execução:")
        result = cache.process_select(script)
        print(f"Dados retornados: {len(result['data'])}")
        print(f"Cache hit: {result['cache_hit']}")
        print(f"Motivo: {result.get('debug_reason', 'N/A')}")

        # Segunda execução (apenas mudanças)
        print("\nSegunda execução:")
        result = cache.process_select(script)
        print(f"Apenas mudanças: {len(result['data'])}")
        print(f"Cache hit: {result['cache_hit']}")
        print(f"Motivo: {result.get('debug_reason', 'N/A')}")

        # Todos os dados
        all_data = cache.get_all_data(script)
        print(f"\nTodos os dados em cache: {len(all_data)}")

        # Registros removidos
        deleted = cache.get_deleted_records(script)
        print(f"Registros removidos: {len(deleted)}")

        # Estatísticas
        stats = cache.get_cache_stats()
        print(f"\nEstatísticas: {stats}")

        print(f"\n🔄 REINICIE A APLICAÇÃO - O CACHE SERÁ MANTIDO!")
        print(f"📁 Arquivo de cache: {cache.cache_file}")
        print(f"💾 Cache principal: INDEFINIDO (nunca expira)")
        print(f"🗑️ Cache deletados: {cache.keep_deleted_hours}h TTL (limpeza automática)")

    except ImportError as e:
        print(f"Erro no import: {e}")
        print("Certifique-se de que:")
        print("1. O arquivo db_handlers/oracle.py existe")
        print("2. A classe OracleDBManager está definida corretamente")
        print("3. Execute este arquivo a partir do diretório do projeto")
    except Exception as e:
        print(f"Erro durante execução: {e}")