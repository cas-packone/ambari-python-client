import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ambari.client import Client

c=Client('http://localhost:8080')
# c.stack.blueprint.delete()
c.stack.register_blueprint_minimal_single()
c.create_cluster('test',['master'])