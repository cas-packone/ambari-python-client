import argparse
from .client import Client
from .cluster import Host

parser = argparse.ArgumentParser(description='ambari cmd line.')
parser.add_argument('server', metavar='S', nargs=1,
                    help='the ambari server host:port to connect.')
parser.add_argument('target', choices=['stack', 'cluster', 'host', 'service', 'request'], metavar='T', nargs=1,
                    help='the target resource to perform an action on.')
parser.add_argument('action', metavar='A', nargs=1,
                    help='the action to be performed on the target resource.')
parser.add_argument('opts', metavar='O', nargs='*',
                    help='the extra argument for the action.')
args = parser.parse_args()

client=Client('http://'+args.server[0])

def run():
    func=globals()['{}_{}'.format(args.target[0],args.action[0])]
    func()

def cluster_create():
    (name,size)=args.opts[0:2]
    hosts=args.opts[2:]
    client.create_cluster(name,hosts=hosts,size=size)

def cluster_create_from_vdf():
    (VDF_url,name,size)=args.opts[0:3]
    hosts=args.opts[3:]
    client.create_cluster(name,hosts=hosts,VDF_url=VDF_url,size=size)

def host_clone():
    from_h=client.cluster.get_host(args.opts[0])
    to_h=Host(client.cluster,args.opts[1])
    to_h.register()
    to_h.clone(from_h)

def host_delete():
    client.cluster.get_host(args.opts[0]).delete()
    client.cluster.restart_required()

def service_start():
    if args.opts:
        client.cluster.get_service(name=args.opts[0]).start()
    else:
        client.cluster.start()
        client.cluster.restart_required()

def service_stop():
    if args.opts:
        client.cluster.get_service(name=args.opts[0]).stop()
    else:
        client.cluster.stop()

def service_list():
    for s in client.cluster.services:
        print(s.name)

def service_monitorHDFS():
    print(client.cluster.HDFS_usage)

def request_list():
    for r in reversed(client.cluster.requests):
        print(r)
        for t in r.tasks:
            print(t)
        if r.status not in ['IN_PROGRESS', 'PENDING']: break
