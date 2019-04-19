import sys
from client import APIClient

server=sys.argv[1]
host=sys.argv[2]
APIClient('http://{}:8080'.format(server)).host_delete(host)