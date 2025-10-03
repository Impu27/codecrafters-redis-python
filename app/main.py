import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept() # wait for client

    # Keep responding as long as the client keeps sending data
    while True:
        data = connection.recv(1024)  # read up to 1024 bytes
        if not data:  # client closed connection
            break
        connection.sendall(b"+PONG\r\n")  # Respond to any input with "+PONG\r\n"


if __name__ == "__main__":
    main()
