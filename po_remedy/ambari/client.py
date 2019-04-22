import requests
import time

class APIClient(object):
    def __init__(self,url='http://localhost:8080',username='admin',passwd='admin',retry_interval=3,operation_interval=10,mustend=90):
        self.url=url+'/api/v1/clusters/'
        self.username=username
        self.passwd=passwd
        self.retry_interval=retry_interval
        self.operation_interval=operation_interval
        self.mustend=mustend
        self.headers = {'X-Requested-By': 'ambari'}
        #curl -i -u admin:admin -H "X-Requested-By: ambari"  -X GET http://localhost:8080/api/v1/clusters/
        self.cluster_name=self.preflight()['items'][0]['Clusters']['cluster_name']
    def request_raw(self,url='',data=None,call_method=None):
        kwargs={
            'url':self.url+url,
            'headers':self.headers,
            'auth': (self.username,self.passwd)
        }
        if data:
            kwargs['data']=data
            if not call_method:
                call_method=requests.put
        elif not call_method:
            call_method=requests.get
        ret = call_method(**kwargs)
        print(call_method.__name__,self.url+url,ret)
        return ret
    def preflight(self,url='',data=None,call_method=None,status_code=None):
        ret = self.request_raw(url,data,call_method)
        condition=status_code if status_code else requests.codes.ok
        if ret.status_code == condition:
            if ret.text:
                ret = ret.json()
                time.sleep(self.operation_interval)
        else:
            ret=False
        return ret
    def request(self,url='',data=None,call_method=None,status_code=None):
        return self.preflight(self.cluster_name+url,data,call_method,status_code)
    def check(self,url,data=None,call_method=None,retry_interval=None,mustend=None,status_code=None):
        if not retry_interval: retry_interval=self.retry_interval
        if not mustend: mustend=self.mustend
        mustend = time.time() + mustend
        while time.time() < mustend:
            ret=self.request(url,data=data,call_method=call_method,status_code=status_code)
            if ret: return ret
            time.sleep(retry_interval)
        return False
    #ref:https://community.hortonworks.com/answers/88215/view.html
    #curl -i -u admin:admin -H "X-Requested-By: ambari"  -X PUT  -d '{"RequestInfo":{"context":"_PARSE_.START.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"emr"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}' http://localhost:8080/api/v1/clusters/emr/services
    def service_start_all(self):
        data='{"RequestInfo":{"context":"_PARSE_.START.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"'+self.cluster_name+'"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}'
        return self.request("/services",data)
    #ref:https://community.hortonworks.com/answers/88215/view.html
    #curl -i -u admin:admin -H "X-Requested-By: ambari"  -X PUT  -d '{"RequestInfo":{"context":"_PARSE_.STOP.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"emr"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'  http://localhost:8080/api/v1/clusters/emr/services
    def service_stop_all(self):
        data='{"RequestInfo":{"context":"_PARSE_.STOP.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"'+self.cluster_name+'"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'
        return self.request("/services",data)
    #curl -k -u admin:admin -H "X-Requested-By:ambari" -i -X PUT -d '{"RequestInfo":{"context":"Turn on Maintenance for YARN"},"Body":{"ServiceInfo":{"maintenance_state":"ON"}}}' http://localhost:8080/api/v1/clusters/emr/services/YARN
    def service_maintenance_on(self,service_name):
        data='{"RequestInfo":{"context":"Turn on Maintenance for '+service_name+'"},"Body":{"ServiceInfo":{"maintenance_state":"ON"}}}'
        return self.request("/services/"+service_name,data)
    #curl -k -u admin:admin -H "X-Requested-By:ambari" -i -X PUT -d '{"RequestInfo":{"context":"Turn off Maintenance for YARN"},"Body":{"ServiceInfo":{"maintenance_state":"OFF"}}}' http://localhost:8080/api/v1/clusters/emr/services/YARN
    def service_maintenance_off(self,service_name):
        data='{"RequestInfo":{"context":"Turn off Maintenance for '+service_name+'"},"Body":{"ServiceInfo":{"maintenance_state":"OFF"}}}'
        return self.request("/services/"+service_name,data)
    #curl -u admin:passwd  -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Start service"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://<AMBARI_SERVER_HOSTNAME>:8080/api/v1/clusters/<CLUSTER_NAME>/services/<Service_name>
    def service_start(self,service_name=None):
        if not service_name: return self.service_start_all()
        data='{"RequestInfo": {"context" :"Start service"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
        return self.request("/services/"+service_name,data)
    #curl -u admin:passwd  -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Stop service "}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://<AMBARI_SERVER_HOSTNAME>:8080/api/v1/clusters/<CLUSTER_NAME>/services/<Service_name>
    def service_stop(self,service_name=None):
        if not service_name: return self.service_stop_all()
        data='{"RequestInfo": {"context" :"Stop service"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}'
        return self.request("/services/"+service_name,data)
    #curl -u admin:admin -H "X-Requested-By: ambari" -X GET "http://10.0.88.52:8080/api/v1/clusters/emr/services/YARN?fields=ServiceInfo/state"
    def service_status(self,service_name):
        return self.request("/services/"+service_name+'?fields=ServiceInfo/state')
    #curl --user admin:admin -i -X POST http://AMBARI_SERVER_HOST:8080/api/v1/clusters/CLUSTER_NAME/hosts/NEW_HOST_ADDED
    def host_add(self,host):
        print('host_add({})'.format(host))
        url="/hosts/"+host
        self.check(url,call_method=requests.post,status_code=201)
        return self.check(url)
    #https://cwiki.apache.org/confluence/display/AMBARI/Using+APIs+to+delete+a+service+or+all+host+components+on+a+host
    #curl -u admin:admin -H "X-Requested-By: ambari" -X GET  http://AMBARI_SERVER_HOST:8080/api/v1/clusters/c1/hosts/HOSTNAME
    def host_components(self,host):
        ret = self.request("/hosts/"+host)
        if not ret or 'host_components' not in ret: return ()
        cmpns=[]
        for c in ret['host_components']:
            cmpns.append(c['HostRoles']['component_name'])
        return cmpns
    def host_component(self,host,component):
        url="/hosts/"+host+'/host_components/'+component
        return self.check(url)
    #curl -u admin:admin -X PUT -d '{"RequestInfo":{"context":"Stop Component"},"Body":{"HostRoles":{"state":"INSTALLED"}}}' http://AMBARI_SERVER_HOST:8080/api/v1/clusters/c1/hosts/HOSTNAME/host_components/COMPONENT_NAME
    def  host_component_stop(self, host, component):
        data='{"RequestInfo":{"context":"Stop Component"},"Body":{"HostRoles":{"state":"INSTALLED"}}}'
        return self.request("/hosts/"+host+'/host_components/'+component,data)
    #curl --user admin:admin -i -X POST http://AMBARI_SERVER_HOST:8080/api/v1/clusters/CLUSTER_NAME/hosts/NEW_HOST_ADDED/host_components/DATANODE
    def host_component_add(self,host,component):
        url="/hosts/"+host+'/host_components/'+component
        ret=self.check(url,call_method=requests.post,status_code=201)
        if not ret: return ret
        # data='{"RequestInfo":{"context":"Install '+component+'","operation_level":{"level":"HOST_COMPONENT","cluster_name":"'+self.cluster_name+'","host_name":"'+host+'","service_name":"FLUME"}},"Body":{"HostRoles":{"state":"INSTALLED"}}}'
        # if ret: time.sleep(30)
        ret=self.host_component(host,component)
        service_name=ret['HostRoles']['service_name']
        data='{"RequestInfo":{"context":"Install '+component+'","operation_level":{"level":"HOST_COMPONENT","cluster_name":"'+self.cluster_name+'","host_name":"'+host+'","service_name":"'+service_name+'"}},"Body":{"HostRoles":{"state":"INSTALLED"}}}'
        return self.check(url,data=data,status_code=202)
    #https://cwiki.apache.org/confluence/display/AMBARI/Using+APIs+to+delete+a+service+or+all+host+components+on+a+host
    #curl -u admin:admin -H "X-Requested-By: ambari" -X DELETE http://AMBARI_SERVER_HOST:8080/api/v1/clusters/CLUSTERNAME/hosts/HOSTNAME/host_components/DATANODE
    def host_component_delete(self,host,component):
        url="/hosts/"+host+'/host_components/'+component
        self.check(url,call_method=requests.delete)
        return self.check(url,status_code=404)
    def host_clone(self,from_host,to_host):
        print('host_clone({},{})'.format(from_host,to_host))
        if not self.host_add(to_host): return False
        for c in self.host_components(from_host):
            if not self.host_component_add(to_host,c):
                return False
        return True
    #https://cwiki.apache.org/confluence/display/AMBARI/Using+APIs+to+delete+a+service+or+all+host+components+on+a+host
    #curl -u admin:admin -H "X-Requested-By: ambari" -X DELETE http://AMBARI_SERVER_HOST:8080/api/v1/clusters/CLUSTERNAME/hosts/HOSTNAME
    def host_delete(self,host):
        print('host_delete({})'.format(host))
        ret=False
        for c in self.host_components(host):
            self.host_component_stop(host,c)
            if not self.host_component_delete(host,c):
                return False
        url="/hosts/"+host
        ret = self.check(url,call_method=requests.delete)
        if ret: time.sleep(30)
        return self.check(url,status_code=404)