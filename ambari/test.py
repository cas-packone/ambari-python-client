import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ambari.client import Client

c=Client('http://10.0.88.152:8080')
# c.cluster.delete()
# c.stack.blueprint.delete()
c.stack.register_blueprint_typical_triple()
c.create_cluster('packone',['master1.packone','master2.packone','slave.packone'])#,VDF_url='http://10.0.88.2/hdp/centos7/HDP-3.1.0.0-78.xml')