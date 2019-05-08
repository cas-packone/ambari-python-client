Amabri python client based on ambari rest api.

===================
Install
===================
pip install ambari

===================
Command line
===================
ambari -h

ambari localhost:8080 service start ZOOKEEPER

ambari localhost:8080 host remove server2

===================
Python modular
===================

from ambari.client import Client

client=Client('http://localhost:8080')

for s in client.cluster.services:

    print(s.name)

    s.start()