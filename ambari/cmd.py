import argparse
from .client import Client

parser = argparse.ArgumentParser(description='ambari cmd line.')
parser.add_argument('server', metavar='S', nargs=1,
                    help='the ambari server host:port to connect.')
parser.add_argument('target', choices=['stack', 'cluster', 'host', 'service'], metavar='T', nargs=1,
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

def host_clone():
    client.cluster.clone_host(from_h=args.opts[0],to_h=args.opts[1])

def host_remove():
    client.cluster.remove_host(host_name=args.opts[0])

def service_start():
    if args.opts:
        client.cluster.get_service(service_name=args.opts[0]).start()
    else:
        client.cluster.start()

def service_stop():
    if args.opts:
        client.cluster.get_service(service_name=args.opts[0]).stop()
    else:
        client.cluster.stop()
        
def service_list():
    for s in client.cluster.services:
        print(s.name)
