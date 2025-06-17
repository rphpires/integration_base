import os
import sys
import threading
import time
import datetime
import traceback
from queue import Queue, Empty
from dataclasses import dataclass
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


# Constantes (você pode ajustar conforme sua aplicação)
try:
    from .functions import get_localtime, is_windows, remove_accents_from_string, format_date
except ImportError:
    # Fallbacks caso não existam nas functions
    CONTROLLER_VERSION = "1.0.0"
    ERROR_LOG_FILE = "logs/ErrorLog.txt"

    def get_localtime():
        return time.localtime()

    def is_windows():
        return os.name == 'nt'

    def remove_accents_from_string(text):
        return text

# Configurações


@dataclass
class TracerConfig:
    trace_file_name: str = 'logs/trace.html'
    error_file_name: str = 'logs/ErrorLog.txt'
    error_file_max_size: int = 1 * 1024 * 1024  # 1MB
    trace_files_limit_count: int = 20
    trace_files_limit_size: int = 4 * 1024 * 1024  # 4MB
    flush_interval: float = 2.0
    queue_timeout: float = 0.1
    enable_trace: bool = bool(os.getenv('ENABLE_TRACE'))

# Estrutura para mensagens na queue


@dataclass
class TraceMessage:
    message: str
    color_name: str
    timestamp: time.struct_time
    thread_name: str


class TracerQueue:
    def __init__(self, config: TracerConfig = None):
        self.config = config or TracerConfig()
        self.message_queue = Queue()
        self.worker_thread = None
        self.running = False
        self.trace_file = None
        self.html_trace = False
        self.screen_trace = self.config.enable_trace
        self.error_to_file = True
        self.last_flush = 0
        self.__last_color = None

        # Cores para classes (mantendo sua configuração original)
        self.class_color_trace = {
            '_MainThread': "lavenderblush",
            "Invenzi": "orange"
        }

        self.shell_colors = {
            "gray": "1;30", "darkred": "31", "red": "1;31", "green": "32",
            "darkgreen": "1;32", "brown": "33", "yellow": "1;33",
            "blue_dark": "34", "blue": "1;34", "purple": "35",
            "magenta": "1;35", "cyan": "36", "lightcyan": "1;36",
            "white": "37", "normal": "0",
        }

        self.html_to_shell_colors = {
            "lightskyblue": "purple", "darkorchid": "purple", "orchid": "magenta",
            "chocolate": "brown", "mediumseagreen": "green", "seagreen": "green",
            "lightseagreen": "green", "olive": "green", "olivedrab": "green",
            "lightgreen": "green", "springgreen": "green", "lime": "green",
            "lawngreen": "green",
        }

        self._setup_error_logging()
        self._start_worker()

    def _setup_error_logging(self):
        """Configura redirecionamento de erro para arquivo"""
        try:
            Path("logs").mkdir(exist_ok=True)
            sys.stderr = open(self.config.error_file_name, "a")
        except Exception:
            self.error_to_file = False

    def _start_worker(self):
        """Inicia thread worker para processar mensagens"""
        self.running = True
        self.worker_thread = threading.Thread(
            target=self._worker_loop,
            name="TracerWorker",
            daemon=True
        )
        self.worker_thread.start()

    def _worker_loop(self):
        """Loop principal da thread worker"""
        while self.running:
            try:
                # Processa mensagens da queue
                message = self.message_queue.get(timeout=self.config.queue_timeout)
                self._process_message(message)
                self.message_queue.task_done()
            except Empty:
                # Timeout - verifica flush e continua
                self._check_flush()
                continue
            except Exception as e:
                print(f"Erro no worker do tracer: {e}")

    def _process_message(self, trace_msg: TraceMessage):
        """Processa uma mensagem de trace"""
        self._check_error_log_file()

        if not self.html_trace and not self.screen_trace:
            return

        # Formata a mensagem
        date_str = format_date(trace_msg.timestamp)
        formatted_msg = f"{date_str} - {trace_msg.message}"
        formatted_msg = remove_accents_from_string(formatted_msg)

        # Processa para tela
        if self.screen_trace:
            self._trace_to_screen(formatted_msg, trace_msg.color_name)

        # Processa para HTML
        if self.html_trace:
            # x.year, x.month, x.day, x.hour, x.minute, x.second,
            fd = "%04d_%02d_%02d_%02d_%02d_%02d" % (
                trace_msg.timestamp.year, trace_msg.timestamp.month,
                trace_msg.timestamp.day, trace_msg.timestamp.hour,
                trace_msg.timestamp.minute, trace_msg.timestamp.second
            )
            self._trace_to_html(formatted_msg, trace_msg.color_name, fd)

    def _check_flush(self):
        """Verifica se precisa fazer flush do arquivo"""
        if self.trace_file:
            try:
                current_time = time.monotonic()
                if current_time - self.last_flush > self.config.flush_interval:
                    self.trace_file.flush()
                    self.last_flush = current_time
            except Exception as ex:
                print(f"Erro no flush do trace: {ex}")

    def trace_message(self, msg: str):
        print(msg)
        if not self.running:
            return
        
        if Path("../TraceEnable.txt") and not self.html_trace:
            self.set_html_trace(True)

        # Obtém informações da thread atual
        current_thread = threading.current_thread()
        thread_name = current_thread.getName()

        # Processa nome da thread
        if thread_name.startswith("AdjustedTypeName_"):
            thread_type = thread_name.replace("AdjustedTypeName_", "")
        else:
            if 'Thread' not in thread_name and not msg.startswith(thread_name):
                msg = f"{thread_name} {msg}"

            try:
                thread_type = str(type(current_thread)).split("'")[1].split('.')[1]
            except IndexError:
                thread_type = ''

        # Determina cor
        color_name = self.class_color_trace.get(thread_type, "white")

        # Cria mensagem e adiciona à queue
        trace_msg = TraceMessage(
            message=msg,
            color_name=color_name,
            timestamp=get_localtime(),
            thread_name=thread_name
        )

        try:
            # Non-blocking put - se a queue estiver cheia, descarta a mensagem
            self.message_queue.put_nowait(trace_msg)
        except Exception:
            # Queue cheia - pode implementar estratégia de descarte se necessário
            pass

    def set_screen_trace(self, value: bool):
        """Ativa/desativa trace para tela"""
        self.screen_trace = value

    def set_html_trace(self, value: bool):
        """Ativa/desativa trace para HTML"""
        if value == self.html_trace:
            return

        self.html_trace = value
        if not self.html_trace and self.trace_file:
            try:
                self.trace_file.close()
                self.trace_file = None
            except IOError:
                pass

            # Limpa arquivos antigos
            if is_windows():
                os.system("del ..\\logs\\trace* 2> nul")
            else:
                os.system("rm logs/trace* 2> /dev/null")

    def shutdown(self):
        """Para o tracer de forma limpa"""
        self.running = False
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=1)

        if self.trace_file:
            try:
                self.trace_file.close()
            except Exception:
                pass

    # Métodos auxiliares (mantidos da implementação original com pequenos ajustes)
    def _check_error_log_file(self):
        if not self.error_to_file:
            return

        try:
            size = sys.stderr.tell()
            if size > self.config.error_file_max_size:
                if is_windows():
                    sys.stderr.close()

                x = get_localtime()
                fd = "%04d_%02d_%02d_%02d_%02d_%02d" % (
                    x.year, x.month, x.day, x.hour, x.minute, x.second
                )

                if is_windows():
                    pattern = 'logs/ErrorLog_%s.txt'
                else:
                    pattern = 'logs/ErrorLog_%s.txt.gz'

                self._handle_new_log_file(self.config.error_file_name, pattern, fd)
                sys.stderr = open(self.config.error_file_name, 'w')
        except IOError:
            pass

    def _handle_new_log_file(self, file_name, file_pattern, fd):
        target = file_pattern % fd
        limit_count = self.config.trace_files_limit_count

        if not is_windows():
            target += ".tmp"
            limit_count -= 1

        try:
            os.rename(file_name, target)
        except OSError:
            pass

        self._remove_extra_files(file_pattern % "*", limit_count)

        if not is_windows():
            cmd = "{ "
            cmd += f"/bin/gzip -c {target} > {target[:-4]} 2> /dev/null ; "
            cmd += f"/bin/rm -f {target} 2> /dev/null; "
            cmd += "/bin/rm -f logs/trace_*.dat.tmp 2> /dev/null; "
            cmd += "/bin/rm -f logs/ErrorLog_*.txt.gz.tmp 2> /dev/null; "
            cmd += "} &"
            os.system(cmd)

    def _remove_extra_files(self, pattern, limit):
        if is_windows():
            import glob
            files = glob.glob(pattern)
            if len(files) > limit:
                files.sort()
                for f in files[:-limit]:
                    os.remove(f)
        else:
            os.system("rm -f logs/*.txt.gz.tmp 2> /dev/null")
            os.system(f"rm -f `ls -r {pattern} 2> /dev/null | tail -n +{limit + 1}`")

    def _trace_to_screen(self, msg, color_name):
        actual_color = self.html_to_shell_colors.get(color_name, color_name)
        shell_color_code = self.shell_colors.get(actual_color, "0")
        color_escape = f"\033[{shell_color_code}m"
        formatted_msg = f"{color_escape}{msg}\033[0m"
        print(formatted_msg)
        sys.stdout.flush()

    def _trace_to_html(self, msg, color, fd):
        # Escapa caracteres HTML
        msg = msg.replace('=>', '&rArr;').replace('<', '&lt;').replace('>', '&gt;')
        msg = msg.replace('\r\n', '\n')

        # Permite tags específicas
        for tag in ["code", "b"]:
            msg = msg.replace(f'&lt;{tag}&gt;', f'<{tag}>')
            msg = msg.replace(f'&lt;/{tag}&gt;', f'</{tag}>')

        # Gerencia arquivo HTML
        is_new_file = self._manage_html_file(fd)

        if '***' in msg:
            color = "red"

        msg = msg.strip('\n')

        # Escreve no arquivo
        if self.__last_color != color:
            prefix = "</font>" if not is_new_file else ""
            prefix += f'<font color="{color}">\n'
            content = prefix + msg
            self.__last_color = color
        else:
            content = "\n" + msg

        try:
            self.trace_file.write(content)
        except Exception:
            pass

    def _manage_html_file(self, fd):
        """Gerencia rotação de arquivos HTML"""
        is_new_file = False

        if (not self.trace_file and os.access(self.config.trace_file_name, os.R_OK)) or \
           (self.trace_file and self.trace_file.tell() > self.config.trace_files_limit_size):

            if self.trace_file:
                self.trace_file.write('</font><br>\n</body>\n')
                self.trace_file.close()
                self.trace_file = None

            pattern = 'logs/trace_%s.html' if is_windows() else 'logs/trace_%s.dat'
            self._handle_new_log_file(self.config.trace_file_name, pattern, fd)
            is_new_file = True
            self.__last_color = None

        if not self.trace_file:
            self.trace_file = open(self.config.trace_file_name, 'w')
            self._write_html_header()

        return is_new_file

    def _write_html_header(self):
        """Escreve cabeçalho do arquivo HTML"""
        header = '''<!DOCTYPE html>
<meta content="text/html;charset=utf-8" http-equiv="Content-Type">
<style>
font { white-space: pre; }
</style>
<script>
var original_html = null;
var filter = '';
function filter_log() {
    document.body.style.cursor = 'wait';
    if (original_html == null) {
        original_html = document.body.innerHTML;
    }
    if (filter == '') {
        document.body.innerHTML = original_html;
    } else {
        l = original_html.split("\\n");
        var pattern = new RegExp(".*" + filter.replace('"', '"') + ".*", "i");
        final_html = '<font>';
        for(var i=0; i<l.length; i++){
            if (pattern.test(l[i]))
                final_html += l[i] + '\\n';
        }
        final_html += '</font>';
        document.body.innerHTML = final_html;
    }
    document.body.style.cursor = 'default';
}

document.onkeydown = function(event) {
    if (event.keyCode == 76) {
        var ret = prompt("Enter the filter regular expression. Examples:\\n\\n\\
    CheckFirmwareUpdate\\n\\nID=1 |ID=2 \\n\\nID=2 .*Got message\\n\\n2012-08-31 16:.*(ID=1 |ID=2 )\\n\\n", filter);
        if (ret != null) {
            filter = ret;
            filter_log();
        }
        return false;
    }
}
</script>
<body bgcolor="black" text="white">
'''
        self.trace_file.write(header)


# =====================================================================
# SINGLETON PATTERN PARA GARANTIR UMA ÚNICA INSTÂNCIA
# =====================================================================

_tracer_instance = None
_tracer_lock = threading.Lock()


def get_tracer():
    """Retorna a instância singleton do tracer"""
    global _tracer_instance
    if _tracer_instance is None:
        with _tracer_lock:
            if _tracer_instance is None:  # Double-check locking
                _tracer_instance = TracerQueue()
    return _tracer_instance


def init_tracer(config: TracerConfig = None):
    """Inicializa o tracer com configuração personalizada (opcional)"""
    global _tracer_instance
    with _tracer_lock:
        if _tracer_instance is not None:
            _tracer_instance.shutdown()
        _tracer_instance = TracerQueue(config)
    return _tracer_instance


# Instância global para compatibilidade
tracer = get_tracer()

# =====================================================================
# FUNÇÕES UTILITÁRIAS PARA USO DIRETO EM OUTROS ARQUIVOS
# =====================================================================


def trace(msg):
    """Função simples para trace com remoção de acentos"""
    get_tracer().trace_message(remove_accents_from_string(msg))


def trace_elapsed(msg, reference_utc_time):
    """Trace com cálculo de tempo decorrido em milissegundos"""
    delta = datetime.datetime.utcnow() - reference_utc_time
    if not hasattr(delta, 'total_seconds'):
        get_tracer().trace_message(msg)
        return
    elapsed_ms = int(delta.total_seconds() * 1000)
    msg += " (%d ms)" % elapsed_ms
    get_tracer().trace_message(msg)


def info(msg):
    """Função de conveniência para mensagens informativas"""
    get_tracer().trace_message(msg)


def error(msg):
    """Função para mensagens de erro (aparece destacado e vai para stderr/stdout)"""
    get_tracer().trace_message("****" + msg)
    x = get_localtime()
    timestamp = "%04d/%02d/%02d %02d:%02d:%02d.%06d " % (
        x.year, x.month, x.day, x.hour, x.minute, x.second,
        getattr(x, 'tm_microsecond', 0)  # tm_microsecond pode não existir
    )
    error_msg = "ERROR" + timestamp + msg + '\n'
    sys.stderr.write(error_msg)
    sys.stdout.write(error_msg)


def report_exception(e, do_sleep=True):
    """Relata exceções de forma detalhada com timestamp e informações do sistema"""
    x = get_localtime()
    header = "\n\n************************************************************************\n"
    header += "Exception date: %04d/%02d/%02d %02d:%02d:%02d.%06d \n" % (
        x.year, x.month, x.day, x.hour, x.minute, x.second,
        getattr(x, 'tm_microsecond', 0)
    )
    # header += f"Version {CONTROLLER_VERSION}\n"
    header += "\n"

    # Escreve header nos outputs
    sys.stdout.write(header)
    sys.stderr.write(header)

    # Print do traceback
    traceback.print_exc(file=sys.stdout)

    if is_windows():
        try:
            with open(ERROR_LOG_FILE, 'a') as f:
                f.write(header)
                traceback.print_exc(file=f)
        except Exception:
            pass  # Falha silenciosa se não conseguir escrever no arquivo
    else:
        traceback.print_exc(file=sys.stderr)

    # Identifica o tipo da thread atual
    try:
        thread_type = str(type(threading.current_thread())).split("'")[1].split('.')[1]
    except (IndexError, AttributeError):
        thread_type = 'UNKNOWN'

    # Reporta erro via trace
    error(f"Bypassing exception at {thread_type} ({e})")
    error(f"**** Exception: <code>{traceback.format_exc()}</code>")

    if do_sleep:
        error("Sleeping 2 seconds")
        time.sleep(2.0)


# Função de conveniência para compatibilidade com código existente
def trace_message(msg):
    """Função para compatibilidade com código existente"""
    get_tracer().trace_message(msg)


# Função para controle do tracer
def set_screen_trace(enabled):
    """Ativa/desativa trace na tela"""
    get_tracer().set_screen_trace(enabled)


def set_html_trace(enabled):
    """Ativa/desativa trace em HTML"""
    get_tracer().set_html_trace(enabled)


def shutdown_tracer():
    """Para o tracer de forma limpa"""
    tracer_instance = get_tracer()
    if tracer_instance:
        tracer_instance.shutdown()
