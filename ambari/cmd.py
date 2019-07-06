import argparse
from .client import Client

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
    if client.stack.blueprint: client.stack.blueprint.delete()
    blueprint=getattr(client.stack, 'register_blueprint_{}'.format(size))()
    client.create_cluster(name,hosts=hosts,blueprint=blueprint)

def cluster_create_from_vdf():
    (VDF_url,name,size)=args.opts[0:3]
    hosts=args.opts[3:]
    if client.stack.blueprint: client.stack.blueprint.delete()
    blueprint=getattr(client.stack, 'register_blueprint_{}'.format(size))()
    client.create_cluster(name,hosts=hosts,blueprint=blueprint,VDF_url=VDF_url)

def host_clone():
    from_h=client.cluster.get_host(args.opts[0])
    to_h=client.cluster.get_host(args.opts[1])
    to_h.clone(from_h)

def host_delete():
    client.cluster.get_host(args.opts[0]).delete()

def service_start():
    if args.opts:
        client.cluster.get_service(name=args.opts[0]).start()
    else:
        client.cluster.start()

def service_stop():
    if args.opts:
        client.cluster.get_service(name=args.opts[0]).stop()
    else:
        client.cluster.stop()

def service_list():
    for s in client.cluster.services:
        print(s.name)

def request_list():
    for r in reversed(client.cluster.requests):
        print(r)
        for t in r.tasks:
            print(t)
        if r.status not in ['IN_PROGRESS', 'PENDING']: break
