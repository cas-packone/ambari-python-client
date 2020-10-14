# ambari

Ambari Python client based on Ambari REST API.

## Install

```sh
pip install ambari
```

## Usage

### CLI

```sh
ambari -h

ambari localhost:8080 cluster create test typical_triple master1 master2 slave

ambari localhost:8080 service start ZOOKEEPER

ambari localhost:8080 host delete server2
```

### Module

```py
from ambari.client import Client

client=Client('http://localhost:8080')

for s in client.cluster.services:

    print(s.name)

    s.start()
```
