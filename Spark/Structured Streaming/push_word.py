import socket

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('localhost', 9999))
server.listen(1)

print('waiting...')

conn, addr = server.accept()

while True:
    msg = input('입력 :')
    conn.send((msg + '\n').encode())
