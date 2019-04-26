import requests
from threading import Thread
import time
import sys

class APIClient(object):
    def __init__(self,url='http://localhost:8080',username='admin',passwd='admin',operation_interval=10,retry_refused=False,retry_interval=3,retry_timeout=90):
        self.url=url+'/api/v1/clusters/'
        self.username=username
        self.passwd=passwd
        if retry_interval: self.retry_interval=retry_interval
        self.operation_interval=operation_interval
        self.retry_timeout=retry_timeout
        self.retry_refused=retry_refused
        self.headers = {'X-Requested-By': 'ambari'}
        #curl -i -u admin:admin -H "X-Requested-By: ambari"  -X GET http://localhost:8080/api/v1/clusters/
        ret=self.check_raw() if self.retry_refused else self.request_raw()
        self.cluster_name=ret['items'][0]['Clusters']['cluster_name']
    def request_raw(self,url='',data=None,call_method=None,status_code=None):
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
        condition=status_code if status_code else requests.codes.ok
        if ret.status_code == condition:
            if ret.text:
                ret = ret.json()
                time.sleep(self.operation_interval)
        else:
            ret=False
        return ret
    def check_raw(self,url='',data=None,call_method=None,retry_timeout=None,status_code=None):
        if not retry_timeout: retry_timeout=self.retry_timeout
        mustend = time.time() + retry_timeout
        while time.time() < mustend:
            try:
                ret=self.
                (url,data=data,call_method=call_method,status_code=status_code)
            except requests.ConnectionError as ce:
                if self.retry_refused:
                    time.sleep(self.retry_interval)
                else:
                    raise ce
            else:
                if ret: return ret
                time.sleep(self.retry_interval)
        return ret
    def request(self,url='',data=None,call_method=None,status_code=None):
        return self.request_raw(self.cluster_name+url,data,call_method,status_code)
    def check(self,url,data=None,call_method=None,retry_timeout=None,status_code=None):
        return self.check_raw(self.cluster_name+url,data=data,call_method=call_method,status_code=status_code, retry_timeout=retry_timeout)
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
            Thread(
                target=self.host_component_add,
                args=(to_host,c)
            ).start()
        time.sleep(self.operation_interval)
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