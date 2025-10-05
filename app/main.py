import socket  # noqa: F401
import threading
import time


store = {} #Store in memory key-value pairs and expiry time if present else None 
           #key->(value,expiry time or None)
           #Store list


def encode_simple_string(s :str) -> bytes:
    return f"+{s}\r\n".encode()


def encode_bulk_string(s :str|None) -> bytes:
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()


def encode_integer(n :int) -> bytes:
    return f":{n}\r\n".encode()


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


        elif cmd == "SET":
            key,value = command_parts[1], command_parts[2]
            expiry = None
            #Handling optional PX argument
            if len(command_parts) >= 5 and command_parts[3].upper() == "PX":
                try:
                    ms = int(command_parts[4])
                    expiry = time.time() + (ms / 1000.0)
                except ValueError:
                    pass  # ignore invalid PX values
            store[key] = {
                        "type": "string",
                        "value": value,
                        "expiry": expiry
                        }
            connection.sendall(encode_simple_string("OK"))


        elif cmd == "GET":
            key = command_parts[1]
            if key not in store or store[key]["type"] != "string":
                connection.sendall(encode_bulk_string(None))
                continue
            value, expiry = store[key]["value"], store[key]["expiry"]
            # Check if expired
            if expiry is not None and time.time() > expiry:
                del store[key]
                connection.sendall(encode_bulk_string(None))
            else:
                connection.sendall(encode_bulk_string(value))


        elif cmd == "RPUSH" and len(command_parts) > 2:
            key = command_parts[1]
            value = command_parts[2]
    
            # If key doesn't exist, create a new list
            if key not in store:
                store[key] = {
                            "type": "list",
                            "value": [],
                            "expiry": None
                            }

            # If key exists but isn't a list → return error
            elif store[key]["type"] != "list":
                connection.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                continue

            # Append the new value
            store[key]["value"].extend(value)

            # Return the length of the list as RESP integer
            connection.sendall(encode_integer(len(store[key]["value"])))

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
