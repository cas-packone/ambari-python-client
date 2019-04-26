import sys
from .client import APIClient

def main():
    server=sys.argv[1]
    service=sys.argv[2] if len(sys.argv)>2 else None
    c=APIClient('http://{}'.format(server),retry_refused=True)
    if service:
        c.service_start(service)
    else:
        c.service_start_all()

if __name__ == "__main__":
    main()