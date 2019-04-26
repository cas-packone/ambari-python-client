import sys
from .client import APIClient

def main():
    server=sys.argv[1]
    host=sys.argv[2]
    print('{}: remove host {}'.format(server,host))
    c=APIClient('http://{}'.format(server))
    c.host_delete(host)

if __name__ == "__main__":
    main()