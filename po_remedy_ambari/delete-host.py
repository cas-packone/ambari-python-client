import sys
from client import APIClient

server=sys.argv[1]
host=sys.argv[2]
c=APIClient('http://{}:8080'.format(server))
print(c.host_delete(host))