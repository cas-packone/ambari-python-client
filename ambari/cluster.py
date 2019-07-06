class Cluster(object):
    def __init__(self,client,name):
        self._info=None
        self._stack=None
        self._hosts=None
        self._services=None
        self.client=client
        self.name=name
        self.url='/clusters/'+name
    def delete(self):
        for h in self.hosts:
            h.remove()
        return self.client.delete(self.url)
    @property
    def requests(self):
        rs=[]
        for r in self.client.get(self.url+'/requests')['items']:
            rs.append(Request(id=r['Requests']['id'],cluster=self))
        return rs
    @property
    def info(self):
        if not self._info:
            self._info=self.client.get(self.url)
        return self._info
    @property
    def stack(self):
        if not self._stack:
            stack_info=self.info['stack_versions'][0]['ClusterStackVersions']
            for s in self.client.stacks:
                if s.name==stack_info['stack'] and s.version==stack_info['version']:
                    self._stack=s
                    break
        return self._stack
    @property
    def blueprint_info(self):
        return self.client.get(self.url+'?format=blueprint')
    @property
    def services(self):
        if self._services is None:
            self._services=[]
            for s in self.info['services']:
                self._services.append(Service(cluster=self,name=s['ServiceInfo']['service_name']))
        return self._services
    @property
    def hosts(self):
        if self._hosts is None:
            self._hosts=[]
            for h in self.info['hosts']:
                self._hosts.append(Host(cluster=self,name=h['Hosts']['host_name']))
        return self._hosts
    def get_host(self,name):
        for h in self.hosts:
            if h.name==name:
                return h
        return None
    def get_service(self,name):
        for s in self.services:
            if s.name==name:
                return s
        return None
    def start(self):
        data='{"RequestInfo":{"context":"_PARSE_.START.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"'+self.name+'"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}'
        return self.client.put(self.url+"/services",data,status_code=202)
    def stop(self):
        data='{"RequestInfo":{"context":"_PARSE_.STOP.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"'+self.name+'"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'
        return self.client.put(self.url+"/services",data)
    def add_hosts(self,host_groups):
        data=[]
        for hname in host_groups:
            data.append({
                "blueprint" : 'default',
                "host_group" : host_groups[hname],
                "host_name" : hname
            })
        self.client.create(self.url+'/hosts',data=data)

class Request(object):
    def __init__(self,cluster,id):
        self.cluster=cluster
        self.id=id
        self.url=self.cluster.url+"/requests/{}".format(self.id)
        self._tasks=None
        self._info=None
        self._description=None
    def __str__(self):
        return 'req {} || {} || {}'.format(self.id, self.description, self.status)
    @property
    def info(self):
        inf=self.cluster.client.get(self.url) if not self._info else self._info
        if not self._info and inf['Requests']['request_status'] not in ['IN_PROGRESS', 'PENDING']:
            self._info=inf
        return inf
    @property
    def status(self):
        return self.info['Requests']['request_status']
    @property
    def description(self):
        return self.info['Requests']['request_context']
    @property
    def tasks(self):
        if not self._tasks:
            self._tasks=[]
        for t in self.info['tasks']:
            self._tasks.append(Task(self, t['Tasks']['id']))
        return self._tasks

class Task(object):
    def __init__(self,request,id):
        self.request=request
        self.id=id
        self.url=self.request.url+"/tasks/{}".format(self.id)
        self._info=None
        self._description=None
    def __str__(self):
        return 'req: {} || tsk: {} || {} || {}'.format(self.request.id, self.id, self.description, self.status)
    @property
    def info(self):
        inf=self.request.cluster.client.get(self.url) if not self._info else self._info
        if not self._info and inf['Tasks']['status'] not in ['IN_PROGRESS', 'PENDING']:
            self._info=inf
        return inf
    @property
    def status(self):
        return self.info['Tasks']['status']
    @property
    def description(self):
        return self.info['Tasks']['command_detail']

class Service(object):
    def __init__(self,cluster,name):
        self._info=None
        self.cluster=cluster
        self.name=name
        self.url=self.cluster.url+"/services/"+self.name
    @property
    def info(self):
        if not self._info:
            self._info=self.cluster.client.get(self.url)
        return self._info
    @property
    def components(self):
        cs=[]
        for c in self.info['components']:
            cs.append(ServiceComponent(service=self,name=c['ServiceComponentInfo']['component_name']))
        return cs
    def get_component(self,name):
        for c in self.components:
            if c.name==name:
                return c
        return None                     
    @property
    def quicklinks(self):
        qs=[]      
        for q in self.cluster.stack.get_service(self.name).quicklinks:  
            for c in self.components:
                if q.component.name == c.name: 
                    qs.append(QuickUrl(
                        component=self.get_component(q.component.name),
                        url=q.url.replace("%@",c.host_components[-1].host.name)
                    ))
        return qs 
    @property
    def status(self):
        return self.cluster.client.get(self.url+'?fields=ServiceInfo/state')['ServiceInfo']['state']
    def start(self):
        if self.maintenance_status=='ON':
            self.maintenance_off()
        data={"RequestInfo": {"context" :"Start service"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}
        return self.cluster.client.put(self.url,data,status_code=202)
    def stop(self):
        data={"RequestInfo": {"context" :"Stop service"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}
        return self.cluster.client.put(self.url,data)
    @property
    def maintenance_status(self):
        return self.info['ServiceInfo']['maintenance_state']
    def maintenance_on(self):
        data={"RequestInfo":{"context":"Turn on Maintenance for '+self.name+'"},"Body":{"ServiceInfo":{"maintenance_state":"ON"}}}
        return self.cluster.client.put(self.url,data)
    def maintenance_off(self):
        data={"RequestInfo":{"context":"Turn off Maintenance for '+self.name+'"},"Body":{"ServiceInfo":{"maintenance_state":"OFF"}}}
        return self.cluster.client.put(self.url,data)
    
class ServiceComponent(object):
    def __init__(self,service,name):
        self.service=service
        self.name=name
        self.url=self.service.url+'/components/'+self.name
    @property
    def info(self):
        return self.service.cluster.client.get(self.url)
    @property
    def host_components(self):
        hs=[]
        for c in self.info['host_components']:
            hs.append(HostComponent(name=c['HostRoles']['component_name'],host=self.service.cluster.get_host(c['HostRoles']['host_name'])))
        return hs

class Host(object):
    def __init__(self,cluster,name,group=None):
        self.cluster=cluster
        self.name=name
        self.group=group
        self.url=self.cluster.url+"/hosts/"+self.name
    @property
    def info(self):
        return self.cluster.client.get(self.url)
    @property
    def metrics(self):
        return self.cluster.client.get(self.url+'?fields=metrics')
    @property
    def components(self):
        if 'host_components' not in self.info: return ()
        cmpns=[]
        for c in self.info['host_components']:
            cmpns.append(HostComponent(host=self,name=c['HostRoles']['component_name']))
        return cmpns
    def register(self):
        self.cluster.client.create(self.url)
        self.cluster._hosts.append(self)
    def delete(self):
        for c in self.components:
            c.delete()
        self.cluster.client.delete(self.url)
        self.cluster._hosts.remove(self)
    def clone(self,from_host):
        print('host_clone({},{})'.format(from_host.name,self.host.name))
        self.register()
        for c in from_host.components:
            cpn=HostComponent(host=self, name=c.name)
            Thread(target=cpn.install).start()

class HostComponent(object):
    def __init__(self,host,name):
        self._info=None
        self.host=host
        self.name=name
        self.url=self.host.url+'/host_components/'+self.name
    @property
    def info(self):
        if not self._info:
            self._info=self.host.cluster.client.get(self.url)
        return self._info
    @property
    def status(self):
        url=self.host.cluster.url+'/components?ServiceComponentInfo/component_name='+self.name+'&fields=host_components/HostRoles/state'
        ret = self.host.cluster.client.get(url)['items'][0]['host_components']
        for cpn_info in ret:
            cpn_info=cpn_info['HostRoles']
            if cpn_info['host_name']==self.host.name:
                return cpn_info['state']
        return None
    @property
    def service_name(self):
        return self.info['HostRoles']['service_name']
    @property
    def install(self):
        self.host.cluster.client.create(self.url)
        data={"RequestInfo":{"context":"Install '+self.name+'","operation_level":{"level":"HOST_COMPONENT","cluster_name":"'+self.host.cluster.name+'","host_name":"'+self.host.name+'","service_name":"'+self.service_name+'"}},"Body":{"HostRoles":{"state":"INSTALLED"}}}
        return self.host.cluster.client.put(self.url,data=data,status_code=202)
    def start(self):
        data={"RequestInfo": {"context" :"Start Component"}, "Body": {"HostRoles": {"state": "STARTED"}}}
        return self.host.cluster.client.put(self.url,data=data,status_code=202)
    def stop(self):
        data={"RequestInfo":{"context":"Stop Component"},"Body":{"HostRoles":{"state":"INSTALLED"}}}
        return self.host.cluster.client.put(self.url,data=data,status_code=202)
    def delete(self):
        if self.status=='STARTED':
            self.stop()
        return self.host.cluster.client.delete(self.url)
class QuickUrl(object):  
    def __init__(self,component,url):
        self.component=component
        self.url=url
        
        