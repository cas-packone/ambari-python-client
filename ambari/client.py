import requests
from threading import Thread
import time
import json
from .stack import Stack, Blueprint
from .cluster import Cluster

class Client(object):
    def __init__(self,
        url='http://localhost:8080',
        username='admin',
        passwd='admin',
        retry_refused=False,
        retry_interval=3,
        retry_timeout=90,
        footprint=True,
    ):
        self.url=url+'/api/v1'
        self.username=username
        self.passwd=passwd
        self.retry_interval=retry_interval
        self.retry_timeout=retry_timeout
        self.retry_refused=retry_refused
        self.footprint=footprint
        self._stacks=None
    #curl -u admin:passwd  -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Stop service "}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://<AMBARI_SERVER_HOSTNAME>:8080/api/v1/clusters/<CLUSTER_NAME>/services/<Service_name>
    def _request(self,url,data=None,call_method=None,status_code=None):
        kwargs={
            'url':url,
            'headers': {'X-Requested-By': 'ambari'},
            'auth': (self.username,self.passwd)
        }
        if data:
            kwargs['data']=json.dumps(data)
            if not call_method:
                call_method=requests.put
        elif not call_method:
            call_method=requests.get
        mustend = time.time() + self.retry_timeout
        while time.time() < mustend:
            try:
                response = call_method(**kwargs)
            except requests.ConnectionError as ce:
                if self.retry_refused:
                    time.sleep(self.retry_interval)
                else:
                    raise ce
            else:
                try:
                    ret = response.json()
                    # print(response.text)
                except Exception:
                    ret = {}
                condition=status_code if status_code else requests.codes.ok
                if response.status_code != condition:
                    raise Exception('{} {} {}: {}'.format(call_method.__name__,url,response.status_code,ret))
                if self.footprint: print(call_method.__name__,url,response.status_code)
                if 'Requests' in ret and 'status' in ret['Requests']:
                    while True:
                        time.sleep(self.retry_interval)
                        if self._request(ret['href'])['Requests']['request_status']!='IN_PROGRESS':
                            break
                return ret
        raise ce
    def get(self,url):
        return self._request(self.url+url)
    def put(self,url,data,status_code=None):
        return self._request(self.url+url,data=data,status_code=status_code)
    def create(self,url,data=None,status_code=201):
        return self._request(self.url+url,data=data,call_method=requests.post,status_code=status_code)
    def delete(self,url):
        return self._request(self.url+url,call_method=requests.delete)
    @property
    def stack_info(self):
        return self.get('/stacks')['items']
    @property
    def stacks(self):
        if self._stacks is None:
            self._stacks=[]
            for s in self.stack_info:
                name=s['Stacks']['stack_name']
                vs=self.get('/stacks/'+name)['versions']
                for v in vs:
                    version=v['Versions']['stack_version']
                    self._stacks.append(Stack(client=self,name=name,version=version))
        return self._stacks
    @property
    def stack(self):
        return self.stacks[-1]
    def get_stack(self, name, version):
        for s in self.stacks:
            if s.name==name and s.version==version:
                return s
        return None
    @property
    def host_info(self):
        return self.get('/hosts')['items']
    @property
    def blueprints(self):
        bs=[]
        for b in self.get('/blueprints')['items']:
            bs.append(Blueprint(client=self,name=b['Blueprints']['blueprint_name']))
        return bs
    @property
    def version_definitions(self):
        vs=[]
        for v in self.get('/version_definitions')['items']:
            vs.append(VersionDefinition(client=self,id=v['VersionDefinition']['id']))
        return vs
    def register_version_definition(self, url):
        ret = self.create('/version_definitions',data={"VersionDefinition": {"version_url": url}})
        id=ret["resources"][0]['VersionDefinition']['id']
        for v in self.version_definitions:
            if v.id==id: return v
    @property
    def clusters(self):
        cs=[]
        for c in self.get('/clusters')['items']:
            cs.append(Cluster(client=self,name=c['Clusters']['cluster_name']))
        return cs
    @property
    def cluster(self):
        return self.clusters[-1]
    def create_cluster(self, name, hosts, stack=None, blueprint=None, VDF_url=None):
        if not stack: stack=self.stack
        if not blueprint: blueprint=stack.blueprint
        host_groups=[]
        l=len(blueprint.info['host_groups'])
        if l==1:
            hg_names=['master']
        else:
            hg_names=['master1','master2','slave']
        for ni in range(l):
            host_groups.append({'name': hg_names[ni], "hosts" :[{"fqdn" : hosts[ni]}]})
        for h in hosts[l:]:
            host_groups[-1]['hosts'].append({"fqdn" : h})
        data = {
            "blueprint" : blueprint.name,
            "host_groups" : host_groups
        }
        version_definition = self.register_version_definition(VDF_url) if VDF_url else stack.version_definition
        if version_definition:
            if version_definition.stack!=stack: raise Exception('mismatch vdf and stack')
            data['repository_version_id']=version_definition.id
        c=Cluster(client=self, name=name)
        self.create(c.url,data=data,status_code=202)
        return c

class VersionDefinition(object):
    def __init__(self,client,id):
        self.id=id
        self.client=client
        self.url='/version_definitions/{}'.format(self.id)
    @property
    def info(self):
        return self.client.get(self.url)
    @property
    def stack(self):
        name=self.info['VersionDefinition']['stack_name']
        version=self.info['VersionDefinition']['stack_version']
        for s in self.client.stacks:
            if s.name==name and s.version==version:
                return s
        raise Exception('Stack not found')
    def delete(self):
        url=self.stack.url+'/repository_versions/{}'.format(self.id)
        return self.client.delete(url)