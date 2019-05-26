Ambari python client based on ambari rest api.

===================
Install
===================
pip install ambari

===================
Command line
===================
ambari -h

ambari localhost:8080 cluster create test typical_triple master1 master2 slave

ambari localhost:8080 service start ZOOKEEPER

ambari localhost:8080 host delete server2

===================
Python modular
===================

from ambari.client import Client

client=Client('http://localhost:8080')

for s in client.cluster.services:

    print(s.name)

    s.start()