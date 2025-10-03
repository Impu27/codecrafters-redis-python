import socket  # noqa: F401
import threading


def handle_client(connection):
    #Handling all commands from a single client
    while True:
        data = connection.recv(1024)  # read up to 1024 bytes
        if not data:  # client closed connection
            break
        connection.sendall(b"+PONG\r\n")  # Respond to any input with "+PONG\r\n"
    connection.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True():
        connection, _ = server_socket.accept() #Accepting a new client
        #Start a new thread to handle this client
        client_thread = threading.Thread(target=handle_client, args=(connection,))
        client_thread.start()
    
    


if __name__ == "__main__":
    main()
