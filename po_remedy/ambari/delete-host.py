import sys
from client import APIClient

server=sys.argv[1]
from_h=sys.argv[2]
to_h=sys.argv[3]
import time
c=APIClient('http://{}:8080'.format(server))
c.host_delete(to_h)
# c.host_add(to_h)
c.host_clone(from_h,to_h)