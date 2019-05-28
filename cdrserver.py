import os
import sys
import socket
import logging
import pymysql
import errno
import threading
from datetime import datetime, time

CONST_DELIMITER = ';' # Символ разделитель в получаемой строке
CONST_READ_BUFF_SIZE = 61

CONST_DEFAULT_ADDR = '' # По умолчанию считается localhost
CONST_DEFAULT_PORT = 9090
CONST_NUM_OF_CON = 3

logger = logging.getLogger('main')

class cdr_record:
    def __init__(self, string):
        string = string.decode('utf-8')
        string = string.strip()
        element = string.split(CONST_DELIMITER)

        self.date = element[0]
        self.time = element[1]
        self.pole3 = element[2]
        self.station_number = element[3]
        self.time_incoming_call = element[4]
        self.duration_call = element[5]
        self.external_number = element[6]
        self.pole8 = element[7]
        self.additional_information = element[8]
        self.account_code = element[9]
        self.msn = element[10]
        self.co = element[11]
        self.lcr = element[12]
    
def insert(string):
    element = cdr_record(string)
    connection = ''
    while True:
        try:
                connection = pymysql.connect(
                    host='192.168.23.16', 
                    port=3306, 
                    user='cdrroot', 
                    passwd='31994', 
                    db='cdr'
                )
                logger.info('Conect db')
                break
        except Exception as e:
            logger.info('ERROR' + epr(e))
            continue
        
    with connection.cursor() as cursor:
        sql = ( "INSERT INTO calls (date, time, pole3,"
                    "station_number, duration_call,"
                    "external_number, co)"
                "VALUES (STR_TO_DATE(%s, '%%d.%%m.%%y'),"
                        "STR_TO_DATE(%s, '%%H:%%i:%%s'),"
                        "%s,%s,"
                        "STR_TO_DATE(%s, '%%H:%%i:%%s'),"
                        "%s,%s)"
        )
        cursor.execute(sql,( 
            element.date,
            element.time,
            element.pole3,
            element.station_number,
            element.duration_call,
            element.external_number,
            element.co
        ))

    connection.commit()
    connection.close()

def handle(sock, clinet_ip, client_port):
    logger.info('Start to process request from %s:%d' % (clinet_ip, client_port))
    in_buffer = sock.recv(CONST_READ_BUFF_SIZE)
    
    logger.info('In buffer = ' + repr(in_buffer))

    try:
        insert(in_buffer)
        logger.info('OK insert')
    except Exception as e:
        logger.info('ERROR' + epr(e))

    sock.close()
    logger.info('Done.')

def serve_forever():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((CONST_DEFAULT_ADDR, CONST_DEFAULT_PORT))
    sock.listen(CONST_NUM_OF_CON)

    logger.info('Listning no %s:%d...' % (CONST_DEFAULT_ADDR, CONST_DEFAULT_PORT))
    while True:
        try:
            connection, (client_ip, clinet_port) = sock.accept()
        except IOError as e:
            if e.errno == errno.EINTR:
                continue
            raise
        # запускаем нить
        thread = threading.Thread(
            target=handle,
            args=(connection, client_ip, clinet_port)
        )
        thread.daemon = True
        thread.start()

def main():

    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(thread)s] %(message)s',
        '%H:%M:%S'
    )
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.info('Run')

    serve_forever()


if __name__ == "__main__":
    main()