import sys
from .client import APIClient

def main():
    server=sys.argv[1]
    from_h=sys.argv[2]
    to_h=sys.argv[3]
    print('{}: clone host from {} to {}'.format(server,from_h,to_h))
    c=APIClient('http://{}'.format(server))
    c.host_clone(from_h,to_h)

if __name__ == "__main__":
    main()