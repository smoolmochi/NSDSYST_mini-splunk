import socket
import os
import shlex

SIZE = 1024
FORMAT = "utf-8"

VALID_QUERY_TYPES = {
    "SEARCH_DATE",
    "SEARCH_HOST",
    "SEARCH_DAEMON",
    "SEARCH_SEVERITY",
    "SEARCH_KEYWORD",
    "COUNT_KEYWORD"    
}

HELP_TEXT = """
Available commands:
    INGEST <file_path> <IP:PORT>
    QUERY <IP:Port> SEARCH_DATE <date_or_prefix>
    QUERY <IP:Port> SEARCH_HOST <hostname>
    QUERY <IP:Port> SEARCH_DAEMON <daemon_name>
    QUERY <IP:Port> SEARCH_SEVERITY <severity_level>
    QUERY <IP:Port> SEARCH_KEYWORD <keyword_or_phrase>
    QUERY <IP:Port> COUNT_KEYWORD <keyword_or_phrase>
    PURGE <IP:Port>
    HELP
    EXIT
"""

def connect(target):
    host, port_string = target.rsplit(":", 1)
    port = int(port_string)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((host, port))
    return client

def receive_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Socket connection closed unexpectedly.")
        data += chunk
    return data

def receive_with_length(sock):
    header = receive_exact(sock, 16).decode(FORMAT).strip()
    message_length = int(header)
    return receive_exact(sock, message_length).decode(FORMAT)

def is_valid_target(target):
    if ":" not in target:
        return False

    host, port = target.rsplit(":", 1)

    if not host:
        return False

    if not port.isdigit():
        return False

    return True

def start_client():
    print("[System] Client initialized. Type HELP to see available commands.\n")

    while True:
        try:
            msg = input("client> ")
        except KeyboardInterrupt:
            print("\n[System] Client interrupted by user.")
            break

        if msg.lower() in ['exit', 'quit']: break
        if not msg.strip(): continue

        try:
            parts = shlex.split(msg.strip())
        except ValueError:
            print("[System] Invalid quotation marks in command.\n")
            continue

        if not parts: continue

        command = parts[0].upper()

        if command == "HELP":
            print(HELP_TEXT.strip() + "\n")
            continue

        # INGEST
        try:
            if command == "INGEST":
                if len(parts) != 3:
                    print("[System] Invalid syntax. Use: INGEST <filepath> <IP:Port>\n")
                    continue

                filepath = parts[1]
                target = parts[2]

                if not is_valid_target(target):
                    print("[System] Invalid target format. Use <IP:Port>.\n")
                    continue
                if not os.path.exists(filepath):
                    print(f"[System] Error: File '{filepath}' not found.\n")
                    continue

                print(f"[System Message] Connecting to {target}...")

                client = connect(target)
                client.sendall(msg.encode(FORMAT))

                response = client.recv(SIZE).decode(FORMAT)
                if response != "READY_FOR_FILE":
                    print(f"[Server Response] {response}")
                    client.close()
                    continue

                file_size = os.path.getsize(filepath)
                print(f"[System Message] Uploading syslog ({file_size / 1024:.1f} KB)...")

                with open(filepath, 'r') as f:
                    for line in f:
                        client.sendall(line.encode(FORMAT))

                client.sendall("<EOF>".encode(FORMAT))
                final_response = receive_with_length(client)
                print(f"[Server Response] {final_response}\n")

                client.close()

            # QUERY
            elif command == "QUERY":
                if len(parts) < 4:
                    print("[System] Invalid syntax. Use: QUERY <IP:Port> <SEARCH_TYPE> <term>\n")
                    continue

                target = parts[1]
                query_type = parts[2].upper()
                search_term = " ".join(parts[3:])

                if not is_valid_target(target):
                    print("[System] Invalid target format. Use <IP:Port>.\n")
                    continue
                if query_type not in VALID_QUERY_TYPES:
                    print(f"[System] Invalid query type '{parts[2]}'.\n")
                    continue
                if not search_term.strip():
                    print("[System] Search term cannot be empty.\n")
                    continue

                print("[System Message] Sending query....")

                client = connect(target)
                client.sendall(msg.encode(FORMAT))
                response = receive_with_length(client)
                print(f"[Server Response] {response}\n")

                client.close()

            # PURGE
            elif command == "PURGE":
                if len(parts) != 2:
                    print("[System] Invalid syntax. Use: PURGE <IP:Port>\n")
                    continue

                target = parts[1]

                if not is_valid_target(target):
                    print("[System] Invalid target format. Use <IP:Port>.\n")
                    continue

                print(f"[System Message] Connecting to {target} to purge records...")    
                
                client = connect(target)
                client.sendall(msg.encode(FORMAT))
                response = receive_with_length(client)
                print(f"[Server Response] {response}\n")

                client.close()
                
            else:
                print(f"[System] Unknown command '{parts[0]}'.\n")
                
        except ConnectionRefusedError:
            print(f"[Error] Connection refused. Is the server at {target} running?\n")
        except Exception as e:
            print(f"[Error] Communication failure: {e}\n")

    print("[System] Closing client.")

if __name__ == "__main__":
    start_client()