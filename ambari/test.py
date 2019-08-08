import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ambari.client import Client

c=Client('http://172.31.10.108:8080')
# c.cluster.delete()
for vd in c.version_definitions: vd.delete()
for blp in c.blueprints: blp.delete()
# c.stack.blueprint.delete()
c.create_cluster('packone',['packone'],VDF_url='http://public-repo-1.hortonworks.com/HDP/centos7/2.x/updates/2.6.5.0/HDP-2.6.5.0-292.xml', size='minimal_single')
# for r in reversed(c.cluster.requests):
#     print(r)
#     for t in r.tasks:
#         print(t)
# print(c.cluster.services[-1].start())