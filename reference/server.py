import socket
import threading
import json
import os
import re
import shlex

PORT =  8080
HOST = '0.0.0.0'
FORMAT = "utf-8"
SIZE = 1024

DATA_FILE = "central_log.jsonl"

SYSLOG_PATTERN = re.compile(
    r"^(?P<timestamp>[A-Za-z]{3}\s+\d+\s+\d{2}:\d{2}:\d{2})\s+" # Feb 22 00:05:38
    r"(?P<host>\S+)\s+"                                        # SYSSVR1
    r"(?P<daemon>[^:]+):\s+"                                   # systemd[1]
    r"(?P<message>.*)$"                                        # rest of msg
)

QUERY_MATCHERS = {
    "SEARCH_DATE": lambda log, term: " ".join(term.split()).lower() in " ".join(log["timestamp"].split()).lower(),
    "SEARCH_HOST": lambda log, term: log["host"].lower() == term.lower(),
    "SEARCH_DAEMON": lambda log, term: log["daemon"].lower().startswith(term.lower()),
    "SEARCH_SEVERITY": lambda log, term: log["severity"].upper() == term.upper(),
    "SEARCH_KEYWORD": lambda log, term: bool(re.search(rf"\b{re.escape(term)}\b", log["message"], re.IGNORECASE)),
    "COUNT_KEYWORD": lambda log, term: bool(re.search(rf"\b{re.escape(term)}\b", log["message"], re.IGNORECASE))
}

### CONCURRENCY ###

class ReadWriteLock:
    def __init__(self):
        self._condition = threading.Condition()
        self._readers = 0
        self._writer_active = False

    def acquire_read(self):
        with self._condition:
            while self._writer_active:
                self._condition.wait()
            self._readers += 1

    def release_read(self):
        with self._condition:
            self._readers -= 1
            if self._readers == 0:
                self._condition.notify_all()

    def acquire_write(self):
        with self._condition:
            while self._writer_active or self._readers > 0:
                self._condition.wait()
            self._writer_active = True

    def release_write(self):
        with self._condition:
            self._writer_active = False
            self._condition.notify_all()

rw_lock = ReadWriteLock()

### STORAGE ###

def init_storage():
    if not os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'w') as f:
            pass
        print(f"[STORAGE] Initialized new storage file: {DATA_FILE}")

def save_logs(parsed_logs_list):
    rw_lock.acquire_write()
    try:
        with open(DATA_FILE, 'a') as f:
            for log_dict in parsed_logs_list:
                f.write(json.dumps(log_dict) + '\n')
    finally:
        rw_lock.release_write()

def clear_logs():
    rw_lock.acquire_write()
    try:
        count = 0
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r') as f:
                count = sum(1 for line in f)
            with open(DATA_FILE, 'w') as f:
                pass
        return count
    finally:
        rw_lock.release_write()

def read_logs():
    rw_lock.acquire_read()
    try:
        with open(DATA_FILE, 'r') as f:
            for line in f:
                yield json.loads(line.strip())
    finally:
        rw_lock.release_read()

### PARSING AND SEARCH ###

def parse_syslog_line(line):
    is_match = SYSLOG_PATTERN.match(line)
    if not is_match:
        return None
    
    data = is_match.groupdict()

    msg_upper = data['message'].upper()
    if "ERROR" in msg_upper or "FAIL" in msg_upper or "FATAL" in msg_upper:
        severity = "ERROR"
    elif "WARN" in msg_upper:
        severity = "WARNING"
    else:
        severity = "INFO"
    
    return {
        "timestamp": data['timestamp'],
        "host": data['host'],
        "daemon": data['daemon'],
        "severity": severity,
        "message": data['message']
    }

def construct_line(log_dict):
    return f"{log_dict['timestamp']} {log_dict['host']} {log_dict['daemon']}: {log_dict['message']}"

def execute_search(search_type, search_term):
    matches = []
    count = 0

    matcher_func = QUERY_MATCHERS.get(search_type)
    if not matcher_func:
        return 0, []

    try:
        for log in read_logs():
            if matcher_func(log, search_term):
                count += 1
                if search_type != "COUNT_KEYWORD":
                    if len(matches) < 50:
                        matches.append(f"{count}. {construct_line(log)}")
    except FileNotFoundError:
        pass 

    return count, matches

### NETWORK PROTOCOL ###

def send_with_length(conn, message):
    encoded = message.encode(FORMAT)
    header = f"{len(encoded):<16}".encode(FORMAT)
    conn.sendall(header + encoded)

### REQUEST HANDLERS ###

def handle_input(conn, addr, data):
    try:
        parts = shlex.split(data.strip())
    except ValueError:
        send_with_length(conn, "[ERROR] Invalid command formatting.")
        return

    if not parts:
        return
    
    command = parts[0].upper()
    args = parts[1:]

    if command == "INGEST":
        handle_ingest(conn, addr, args)
    elif command == "QUERY":
        handle_query(conn, addr, args)
    elif command == "PURGE":
        handle_purge(conn, addr, args)
    else:
        error_msg = f"[ERROR] Unknown Command '{command}'"
        send_with_length(conn, error_msg)

def handle_ingest(conn, addr, args):
    conn.sendall("READY_FOR_FILE".encode(FORMAT))

    buffer = ""
    saved_count = 0

    print(f"[INGEST] Request from Client {addr}...")

    try:
        while True:
            chunk = conn.recv(SIZE).decode(FORMAT)
            if not chunk:
                break

            buffer += chunk
            is_done = False

            if "<EOF>" in buffer:
                buffer = buffer.replace("<EOF>", "")
                is_done = True

            while '\n' in buffer:
                line, buffer = buffer.split('\n', 1)
                line = line.strip()

                if line:
                    parsed_dict = parse_syslog_line(line)
                    if parsed_dict:
                        save_logs([parsed_dict])
                        saved_count += 1

            if is_done:
                break

        if buffer.strip():
            parsed_dict = parse_syslog_line(buffer.strip())
            if parsed_dict:
                save_logs([parsed_dict])
                saved_count += 1

        success_response = f"[SUCCESS] File received and {saved_count} syslog entries parsed and indexed."
        send_with_length(conn, success_response)
        print(f"[INGEST] {success_response}")

    except ConnectionResetError:
        print(f"[INGEST] Client {addr} disconnected during upload.")
        print(f"[INGEST] Partial upload saved: {saved_count} syslog entries indexed.")

    except Exception as e:
        print(f"[INGEST ERROR] Client {addr}: {e}")
        print(f"[INGEST] Partial upload saved: {saved_count} syslog entries indexed.")

def handle_query(conn, addr, args):
    if len(args) < 2:
        send_with_length(conn, "[ERROR] Invalid QUERY command.")
        return
    
    target_ip_port = args[0]
    search_type = args[1].upper()
    search_term = " ".join(args[2:]).strip('""\'\'')

    if search_type not in QUERY_MATCHERS:
        send_with_length(conn, f"[ERROR] Unknown query type '{search_type}'")
        return

    if not search_term:
        send_with_length(conn, "[ERROR] Missing search term.")
        return

    print(f"[QUERY] Client {addr} searching for {search_type}: '{search_term}'")
    
    try:
        count, matches = execute_search(search_type, search_term)

        if search_type == "COUNT_KEYWORD":
            if count == 1:
                response = f"[SUCCESS] The keyword '{search_term}' appears in 1 indexed log entry."
            else:
                response = f"[SUCCESS] The keyword '{search_term}' appears in {count} indexed log entries."
        else:
            if count == 0:
                response = f"[SUCCESS] Found 0 matching entries for {search_type} '{search_term}'."
            else:
                header = f"[SUCCESS] Found {count} matching entries for {search_type} '{search_term}'." 
                if count > 50:
                    header += " (Showing first 50):\n"
                else:
                    header += ":\n"
                response = header + "\n".join(matches)
        
        send_with_length(conn, response)
        print(f"[QUERY] {response.splitlines()[0]}")
        
    except Exception as e:
        print(f"[ERROR] Failed to process query for {addr}: {e}")
        send_with_length(conn, "[ERROR] Internal server error while processing query.")

def handle_purge(conn, addr, args):
    print(f"[PURGE] Request from Client {addr}")
    deleted_count = clear_logs()
    response = f"[SUCCESS] {deleted_count:,} indexed log entries have been erased."
    send_with_length(conn, response)
    print(f"[PURGE] {response}")

### CONNECTION HANDLING ###

def display_active_connection(is_disconnecting=False):
    offset = 2 if is_disconnecting else 1
    conn_count = threading.active_count() - offset
    conn_count = max(0, conn_count)
    if conn_count == 1:
        print(f"[ACTIVE CONNECTIONS] There is currently 1 Client connected to the Server.")
    else:
        print(f"[ACTIVE CONNECTIONS] There are currently {conn_count} Clients connected to the Server.")

def handle_client(conn, addr):

    while True:
        try:
            data = conn.recv(SIZE).decode(FORMAT)
            if not data:
                break
            print()
            print(f"[RECEIVED from Client {addr}] {data}")
            
            handle_input(conn, addr, data)
        except ConnectionResetError:
            break
        except Exception as e:
            print(f"[ERROR] Exception with client {addr}: {e}")
            break

    print()
    print(f"[DISCONNECTED] Client {addr} disconnected.")
    conn.close()
    display_active_connection(is_disconnecting=True)

def start_server():
    init_storage()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    server.settimeout(1)
    print(f"[STARTING] Mini-Splunk is running on Port {PORT}")
    
    try:
        while True:
            try:
                conn, addr = server.accept()
                thread = threading.Thread(target=handle_client, args=(conn, addr))
                thread.start()
                display_active_connection(is_disconnecting=False)
            except socket.timeout:
                pass
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Server interrupted by user. Closing server socket...")
    finally:
        server.close()
        print("[SHUTDOWN] Server shut down successfully.")

if __name__ == "__main__":
    start_server()