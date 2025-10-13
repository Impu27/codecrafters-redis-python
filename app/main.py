import socket  # noqa: F401
import threading
import time


store = {} #Dictionary to Store in memory key-value pairs and expiry time if present else None 
           #key->(value,expiry time or None)
           #Store list


blocked_clients = {}

def encode_simple_string(s :str) -> bytes:
    return f"+{s}\r\n".encode()


def encode_bulk_string(s :str|None) -> bytes:
    if s is None:
        return b"$-1\r\n"
    return f"${len(s)}\r\n{s}\r\n".encode()


def encode_integer(n :int) -> bytes:
    return f":{n}\r\n".encode()


def encode_array(items: list[str]) -> bytes:
    resp = f"*{len(items)}\r\n".encode()
    for item in items:
        resp += encode_bulk_string(item)
    return resp


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
            key, values = command_parts[1], command_parts[2:]
            if key not in store:
                store[key] = {"type": "list", "value": [], "expiry": None}
            elif store[key]["type"] != "list":
                connection.sendall(
                    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
                )
                continue

            store[key]["value"].extend(values)
            new_length = len(store[key]["value"])

            # Unblock one waiting client (FIFO)
            while key in blocked_clients and blocked_clients[key] and store[key]["value"]:
                waiting_conn, wait_event, placeholder = blocked_clients[key].pop(0)
                popped = store[key]["value"].pop(0)
                placeholder["value"] = popped
                wait_event.set()

            connection.sendall(encode_integer(new_length))


        elif cmd == "LRANGE" and len(command_parts) == 4:
            key = command_parts[1]
            try:
                start = int(command_parts[2])
                stop = int(command_parts[3])
            except ValueError:
                connection.sendall(b"-ERR invalid indexes\r\n")
                continue

            # Key doesn't exist → empty array
            if key not in store or store[key]["type"] != "list":
                connection.sendall(b"*0\r\n")
                continue

            lst = store[key]["value"]
            length = len(lst)

            #Convert negative index
            if start < 0:
                start = length + start
            if stop < 0:
                stop = length + stop

            # Clamp indexes to valid range
            if start < 0:
                start = 0
            if stop < 0:
                stop = 0
            if stop >= length:
                stop = length - 1
            
            # Start > stop → empty array
            if start > stop or start >= length:
                connection.sendall(b"*0\r\n")
                continue

            # Slice the list and return as RESP array
            result = lst[start:stop + 1]
            connection.sendall(encode_array(result))

        elif cmd == "LPUSH" and len(command_parts) >= 3:
            key, values = command_parts[1], command_parts[2:]
            if key not in store:
                store[key] = {"type": "list", "value": [], "expiry": None}
            elif store[key]["type"] != "list":
                connection.sendall(b"-ERR wrong type\r\n")
                continue

            lst = store[key]["value"]
            for val in values:
                lst.insert(0, val)
            new_length = len(lst)

            # Unblock one waiting client (FIFO)
            while key in blocked_clients and blocked_clients[key] and store[key]["value"]:
                waiting_conn, wait_event, placeholder = blocked_clients[key].pop(0)
                popped = store[key]["value"].pop(0)
                placeholder["value"] = popped
                wait_event.set()

            connection.sendall(encode_integer(new_length))


        elif cmd == "LLEN" and len(command_parts) == 2:
            key = command_parts[1]

            # If list doesn't exist
            if key not in store:
                connection.sendall(encode_integer(0))
                continue

            # If key exists but is not a list
            if store[key]["type"] != "list":
                connection.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                continue

            # Return length of the list
            lst = store[key]["value"]
            connection.sendall(encode_integer(len(lst)))


        elif cmd == "LPOP" and len(command_parts) >= 2:
            key = command_parts[1]

            # If list doesn't exist
            if key not in store:
                if len(command_parts) == 2:
                    connection.sendall(encode_bulk_string(None))
                else:
                    connection.sendall(b"*0\r\n")  # empty array
                    continue

            # If key exists but isn't a list
            if store[key]["type"] != "list":
                connection.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                continue

            lst = store[key]["value"]

            # If no count → single element mode
            if len(command_parts) == 2:
                if not lst:
                    connection.sendall(encode_bulk_string(None))
                else:
                    value = lst.pop(0)
                    connection.sendall(encode_bulk_string(value))
                continue

            # With count → multiple pop
            try:
                count = int(command_parts[2])
            except ValueError:
                connection.sendall(b"-ERR value is not an integer or out of range\r\n")
                continue

            if count <= 0:
                connection.sendall(b"*0\r\n")  # nothing to pop
                continue

            # Pop min(count, len(lst)) elements
            popped = []
            for _ in range(min(count, len(lst))):
                popped.append(lst.pop(0))

            # Encode as RESP array
            resp = f"*{len(popped)}\r\n"
            for val in popped:
                resp += f"${len(val)}\r\n{val}\r\n"
            connection.sendall(resp.encode())


        elif cmd == "BLPOP" and len(command_parts) == 3:
            key = command_parts[1]
            timeout = float(command_parts[2])

            # 1. Immediate pop if available
            if key in store and store[key]["type"] == "list" and store[key]["value"]:
                value = store[key]["value"].pop(0)
                connection.sendall(encode_array([key, value]))
                continue

            # 2. Block with timeout (0 = infinite)
            wait_event = threading.Event()
            placeholder = {}
            blocked_clients.setdefault(key, []).append((connection, wait_event, placeholder))

            wait_time = None if timeout == 0 else timeout
            waited = wait_event.wait(wait_time)

            if not waited:
            # Timeout -> remove from blocked list
                if (connection, wait_event, placeholder) in blocked_clients.get(key, []):
                    blocked_clients[key].remove((connection, wait_event, placeholder))
                connection.sendall(b"*-1\r\n")
            else:
                # Unblocked by RPUSH/LPUSH
                value = placeholder["value"]
                connection.sendall(encode_array([key, value]))


        elif cmd == "XADD" and len(command_parts) >= 5:
            key = command_parts[1]
            entry_id = command_parts[2]
            field_values = command_parts[3:]

            # Validate field-value pairs
            if len(field_values) % 2 != 0:
                connection.sendall(b"-ERR wrong number of arguments for XADD\r\n")
                continue

            # Create stream if it doesn't exist
            if key not in store:
                store[key] = {"type" : "stream", "value" : []}
            elif store["type"] != "stream":
                connection.sendall(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                continue

            # Convert to dict
            fields = {}
            for i in range (0, len(field_values), 2):
                fields[field_values[i]] = field_values[i+1]

            # Append entry
            entry = {"id": entry_id, "fields": fields}
            store[key]["value"].append(entry)

            # Return entry ID
            connection.sendall(encode_bulk_string(entry_id))


        elif cmd == "TYPE" and len(command_parts) == 2:
            key = command_parts[1]
            if key not in store:
                connection.sendall(encode_simple_string("none"))
            else:
                connection.sendall(encode_simple_string(store[key]["type"]))
        



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
