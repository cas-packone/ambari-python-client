Amabri python client based on ambari rest api.

===================
Install
===================
pip install ambari

===================
Command line
===================
ambari -h

===================
Python modular
===================

from ambari.client import Client

client=Client('http://localhost:8080')

for s in client.cluster.services: print(s.name)