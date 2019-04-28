import sys
from .client import APIClient

def main():
    server=sys.argv[1]
    stack_name= sys.argv[2] if len(sys.argv)>2 else 'HDP'
    version= sys.argv[3] if len(sys.argv)>3 else None
    service_name= sys.argv[4] if len(sys.argv)>4 else None
    c=APIClient('http://{}'.format(server))
    if service_name:
        print(c.stack_service_components(stack_name=stack_name,version=version,service_name=service_name))
    else:
        print(c.stack_services(stack_name=stack_name,version=version))

if __name__ == "__main__":
    main()