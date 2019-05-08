import requests
from threading import Thread
import time
import sys

class Client(object):
    def __init__(self,
        url='http://localhost:8080',
        username='admin',
        passwd='admin',
        operation_interval=10,
        retry_refused=False,
        retry_interval=3,
        retry_timeout=90
    ):
        self.url=url+'/api/v1'
        self.username=username
        self.passwd=passwd
        self.retry_interval=retry_interval
        self.operation_interval=operation_interval
        self.retry_timeout=retry_timeout
        self.retry_refused=retry_refused
        self._stacks=None
    #curl -u admin:passwd  -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Stop service "}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://<AMBARI_SERVER_HOSTNAME>:8080/api/v1/clusters/<CLUSTER_NAME>/services/<Service_name>
    def request(self,url='',data=None,call_method=None,status_code=None,wait=False):
        kwargs={
            'url':self.url+url,
            'headers': {'X-Requested-By': 'ambari'},
            'auth': (self.username,self.passwd)
        }
        if data:
            kwargs['data']=data
            if not call_method:
                call_method=requests.put
        elif not call_method:
            call_method=requests.get
        mustend = time.time() + self.retry_timeout
        while time.time() < mustend:
            try:
                ret = call_method(**kwargs)
            except requests.ConnectionError as ce:
                if self.retry_refused:
                    time.sleep(self.retry_interval)
                else:
                    raise ce
            else:
                print(call_method.__name__,self.url+url,ret)
                condition=status_code if status_code else requests.codes.ok
                if ret.status_code == condition:
                    if ret.text:
                        ret = ret.json()
                        if wait: time.sleep(self.operation_interval)
                else:
                    ret=False
                return ret
        raise ce
    def check(self,url='',data=None,call_method=None,status_code=None,wait=False):
        mustend = time.time() + self.retry_timeout
        while time.time() < mustend:
            ret=self.request(url,data=data,call_method=call_method,status_code=status_code,wait=wait)
            if ret: return ret
            time.sleep(self.retry_interval)
        raise Exception('check error!')
    @property
    def stack_info(self):
        return self.request('/stacks')['items']
    @property
    def stacks(self):
        if self._stacks is None:
            self._stacks=[]
            for s in self.stack_info:
                name=s['Stacks']['stack_name']
                vs=self.request('/stacks/'+name)['versions']
                for v in vs:
                    version=v['Versions']['stack_version']
                    self._stacks.append(Stack(client=self,name=name,version=version))
        return self._stacks
    @property
    def cluster_info(self):
        return self.request('/clusters')['items']
    def get_cluster_stack(self,cluster_name):
        stack_info=self.request('/clusters/'+cluster_name)['stack_versions'][0]['ClusterStackVersions']
        for s in self.stacks:
            if s.name==stack_info['stack'] and s.version==stack_info['version']:
                return s
        return None
    @property
    def clusters(self):
        cs=[]
        for c in self.cluster_info:
            name=c['Clusters']['cluster_name']
            cs.append(Cluster(client=self,name=name,stack=self.get_cluster_stack(name)))
        return cs
    @property
    def cluster(self):
        return self.clusters[-1]
    def create_cluster(self,cluster_name,stack):
        c=Cluster(name=cluster_name,client=self,stack=stack)
        c.create()
        return c

class Stack(object):
    def __init__(self,client,name,version):
        self._info=None
        self.client=client
        self.name=name
        self.version=version
        self.url="/stacks/{}/versions/{}".format(self.name,self.version)
    @property
    def info(self):
        if not self._info:
            self._info=self.client.request(self.url)
        return self._info
    @property
    def services(self):
        srvs=[]
        for s in self.info['services']:
            srvs.append(StackService(stack=self,name=s['StackServices']['service_name']))
        return srvs
        
class StackService(object):
    def __init__(self,stack,name):
        self.stack=stack
        self.name=name
        self.url=self.stack.url+"/services/"+self.name
        self._info=None
    @property
    def info(self):
        if not self._info:
            self._info=self.stack.client.request(self.url)
        return self._info
    @property
    def components(self):
        cpns=[]
        for cpn in self.info['components']:
            cpns.append(StackServiceComponent(service=self,name=cpn['StackServiceComponents']['component_name']))
        return cpns

class StackServiceComponent(object):
    def __init__(self,service,name):
        self.service=service
        self.name=name
        self.url=self.service.url+'/components/'+self.name
    @property
    def info(self):
        return self.service.stack.client.request(self.url)

class Cluster(object):
    def __init__(self,client,stack,name):
        self._info=None
        self._hosts=None
        self._services=None
        self.client=client
        self.stack=stack
        self.name=name
        self.url='/clusters/'+name
    def create(self):
        data = '{"Clusters":{"version":"'+self.stack.name+'-'+self.stack.version+'"}}'
        return self.client.request(self.url,data=data,call_method=requests.post,wait=True)
    @property
    def info(self):
        if not self._info:
            self._info=self.client.request(self.url)
        return self._info
    @property
    def services(self):
        if self._services is None:
            self._services=[]
            for s in self.info['services']:
                self._services.append(ClusterService(cluster=self,name=s['ServiceInfo']['service_name']))
        return self._services
    @property
    def hosts(self):
        if self._hosts is None:
            self._hosts=[]
            for h in self.info['hosts']:
                self._hosts.append(Host(cluster=self,name=h['Hosts']['host_name']))
        return self._hosts
    def get_host(self,host_name):
        for h in self.hosts:
            if h.name==host_name:
                return h
        return None
    def get_service(self,service_name):
        for s in self.services:
            if s.name==service_name:
                return s
        return None
    def start(self):
        data='{"RequestInfo":{"context":"_PARSE_.START.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"'+self.name+'"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}'
        return self.client.check(self.url+"/services",data,status_code=202,wait=True)
    def stop(self):
        data='{"RequestInfo":{"context":"_PARSE_.STOP.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"'+self.name+'"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'
        return self.client.request(self.url+"/services",data,wait=True)
    def register_host(self,host_name):
        h=Host(cluster=self, name=host_name)
        h.register()
        return h
    def clone_host(self,from_host_name,to_host_name):
        h=Host(cluster=self, name=to_host_name)
        h.clone(self.get_host(from_host_name))
        return h
    def remove_host(self,host_name):
        self.get_host(host_name).remove()

class ClusterService(object):
    def __init__(self,cluster,name):
        self._info=None
        self.cluster=cluster
        self.name=name
        self.url=self.cluster.url+"/services/"+self.name
    @property
    def info(self):
        if not self._info:
            self._info=self.cluster.client.request(self.url)
        return self._info
    @property
    def components(self):
        cs=[]
        for c in self.info['components']:
            cs.append(ClusterServiceComponent(service=self,name=c['ServiceComponentInfo']['component_name']))
        return cs
    @property
    def status(self):
        return self.cluster.client.request(self.url+'?fields=ServiceInfo/state')['ServiceInfo']['state']
    def maintenance_on(self):
        data='{"RequestInfo":{"context":"Turn on Maintenance for '+self.name+'"},"Body":{"ServiceInfo":{"maintenance_state":"ON"}}}'
        return self.cluster.client.check(self.url,data,wait=True)
    def maintenance_off(self):
        data='{"RequestInfo":{"context":"Turn off Maintenance for '+self.name+'"},"Body":{"ServiceInfo":{"maintenance_state":"OFF"}}}'
        return self.cluster.client.check(self.url,data,wait=True)
    def start(self):
        data='{"RequestInfo": {"context" :"Start service"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
        return self.cluster.client.check(self.url,data,status_code=202,wait=True)
    def stop(self):
        data='{"RequestInfo": {"context" :"Stop service"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}'
        return self.cluster.client.check(self.url,data,wait=True)

class ClusterServiceComponent(object):
    def __init__(self,service,name):
        self.service=service
        self.name=name
        self.url=self.service.url+'/components/'+self.name
    @property
    def info(self):
        return self.service.cluster.client.request(self.url)
    @property
    def host_components(self):
        hs=[]
        for c in self.info['host_components']:
            hs.append(HostComponent(name=c['HostRoles']['component_name'],host=self.service.cluster.get_host(c['HostRoles']['host_name'])))
        return hs

class Host(object):
    def __init__(self,cluster,name):
        self._info=None
        self.cluster=cluster
        self.name=name
        self.url=self.cluster.url+"/hosts/"+self.name
    @property
    def info(self):
        if not self._info:
            self._info=self.cluster.client.request(self.url)
        return self._info
    @property
    def components(self):
        if 'host_components' not in self.info: return ()
        cmpns=[]
        for c in self.info['host_components']:
            cmpns.append(HostComponent(host=self,name=c['HostRoles']['component_name']))
        return cmpns
    def register(self):
        self.cluster.client.check(self.url,call_method=requests.post,status_code=201,wait=True)
        self.cluster._hosts.append(self)
    def remove(self):
        print('host_delete({})'.format(self.name))
        for c in self.components(host):
            c.stop()
            if not c.delete():
                return False
        self.cluster.client.check(self.url,call_method=requests.delete,wait=True)
        self.cluster.client.check(self.url,status_code=404)
        self.cluster._hosts.remove(self)
    def clone(self,from_host):
        print('host_clone({},{})'.format(from_host.name,self.host.name))
        self.register()
        for c in from_host.components:
            cpn=HostComponent(host=self, name=c.name)
            Thread(target=cpn.install).start()
        time.sleep(self.cluster.client.operation_interval)

class HostComponent(object):
    def __init__(self,host,name):
        self._info=None
        self.host=host
        self.name=name
        self.url=self.host.url+'/host_components/'+self.name
    @property
    def info(self):
        if not self._info:
            self._info=self.host.cluster.client.check(self.url)
        return self._info
    @property
    def service_name(self):
        return self.info['HostRoles']['service_name']
    @property
    def install(self):
        self.host.cluster.client.check(self.url,call_method=requests.post,status_code=201,wait=True)
        data='{"RequestInfo":{"context":"Install '+self.name+'","operation_level":{"level":"HOST_COMPONENT","cluster_name":"'+self.host.cluster.name+'","host_name":"'+self.host.name+'","service_name":"'+self.service_name+'"}},"Body":{"HostRoles":{"state":"INSTALLED"}}}'
        return self.host.cluster.client.check(self.url,data=data,status_code=202,wait=True)
    def stop(self):
        data='{"RequestInfo":{"context":"Stop Component"},"Body":{"HostRoles":{"state":"INSTALLED"}}}'
        return self.host.cluster.client.check(self.url,data=data,status_code=200,wait=True)
    def remove(self):
        self.host.cluster.client.check(self.url,call_method=requests.delete)
        return self.host.cluster.client.check(self.url,status_code=404,wait=True)