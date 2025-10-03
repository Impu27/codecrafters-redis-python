import socket  # noqa: F401
import threading


def parse_resp(data: bytes):
    parts = data.split(b"\r\n")
    if not parts or parts[0][0:1] != b"*":
        return []
    #Number of elements in the array
    n = int(parts[0][1:])
    items = []
    idx = 1
    for _ in range(n):
        if parts[idx][0:1] == b"$":
            length = int(parts[idx][1:])
            value = parts[idx+1][:length].decode()
            items.append(value)
            idx += 2
        else:
            idx += 1
    return items


def encode_bulk_string(s :str) -> bytes:
    return f"${len(s)}\r\n{s}\r\n".encode()


def handle_client(connection):
    #Handling all commands from a single client
    while True:
        data = connection.recv(1024)  # read up to 1024 bytes
        if not data:  # client closed connection
            break
        # Parse RESP input into command + args
        command_parts = parse_resp(data)
        if not command_parts:
            continue

        cmd = command_parts[0].upper()  # normalize command name

        if cmd == "PING":
            connection.sendall(b"+PONG\r\n")
        elif cmd == "ECHO" and len(command_parts) > 1:
            arg = command_parts[1]
            connection.sendall(encode_bulk_string(arg))
        else:
            connection.sendall(b"-ERR unknown command\r\n")
    connection.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept() #Accepting a new client
        #Start a new thread to handle this client
        client_thread = threading.Thread(target=handle_client, args=(connection,))
        client_thread.start()
    
    


if __name__ == "__main__":
    main()
