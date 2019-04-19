import requests

class APIClient(object):
    def __init__(self,url='http://localhost:8080',username='admin',passwd='admin'):
        self.url=url+'/api/v1/clusters/'
        self.username=username
        self.passwd=passwd
        self.headers = {'X-Requested-By': 'ambari'}
        #curl -i -u admin:admin -H "X-Requested-By: ambari"  -X GET http://localhost:8080/api/v1/clusters/
        self.cluster_name=self.request_raw()['items'][0]['Clusters']['cluster_name']
    def request_raw(self,url='',data=None):
        ret=''
        url=self.url+url
        if data:
            ret=requests.put(url, headers=self.headers, data=data, auth=(self.username,self.passwd))
        else:
            ret=requests.get(url, headers=self.headers, auth=(self.username,self.passwd))
        if ret.status_code == requests.codes.ok:
            if ret.text:
                print(ret.text)
                return ret.json()
        return ret
    def request(self,url,data=None):
        return self.request_raw(self.cluster_name+url,data)
    #ref:https://community.hortonworks.com/answers/88215/view.html
    #curl -i -u admin:admin -H "X-Requested-By: ambari"  -X PUT  -d '{"RequestInfo":{"context":"_PARSE_.START.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"emr"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}' http://localhost:8080/api/v1/clusters/emr/services
    def service_start_all(self):
        url="/services"
        data='{"RequestInfo":{"context":"_PARSE_.START.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"'+self.cluster_name+'"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}'
        return self.request(url,data)
    #ref:https://community.hortonworks.com/answers/88215/view.html
    #curl -i -u admin:admin -H "X-Requested-By: ambari"  -X PUT  -d '{"RequestInfo":{"context":"_PARSE_.STOP.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"emr"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'  http://localhost:8080/api/v1/clusters/emr/services
    def service_stop_all(self):
        url="/services"
        data='{"RequestInfo":{"context":"_PARSE_.STOP.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"'+self.cluster_name+'"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'
        return self.request(url,data)
    #curl -k -u admin:admin -H "X-Requested-By:ambari" -i -X PUT -d '{"RequestInfo":{"context":"Turn on Maintenance for YARN"},"Body":{"ServiceInfo":{"maintenance_state":"ON"}}}' http://localhost:8080/api/v1/clusters/emr/services/YARN
    def service_maintenance_on(self,service_name):
        url="/services/"+service_name
        data='{"RequestInfo":{"context":"Turn on Maintenance for '+service_name+'"},"Body":{"ServiceInfo":{"maintenance_state":"ON"}}}'
        return self.request(url,data)
    #curl -k -u admin:admin -H "X-Requested-By:ambari" -i -X PUT -d '{"RequestInfo":{"context":"Turn off Maintenance for YARN"},"Body":{"ServiceInfo":{"maintenance_state":"OFF"}}}' http://localhost:8080/api/v1/clusters/emr/services/YARN
    def service_maintenance_off(self,service_name):
        url="/services/"+service_name
        data='{"RequestInfo":{"context":"Turn off Maintenance for '+service_name+'"},"Body":{"ServiceInfo":{"maintenance_state":"OFF"}}}'
        return self.request(url,data)
    #curl -u admin:passwd  -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Start service"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://<AMBARI_SERVER_HOSTNAME>:8080/api/v1/clusters/<CLUSTER_NAME>/services/<Service_name>
    def service_start(self,service_name=None):
        if not service_name: return self.service_start_all()
        url="/services/"+service_name
        data='{"RequestInfo": {"context" :"Start service"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
        return self.request(url,data)
    #curl -u admin:passwd  -H 'X-Requested-By: ambari' -X PUT -d '{"RequestInfo": {"context" :"Stop service "}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://<AMBARI_SERVER_HOSTNAME>:8080/api/v1/clusters/<CLUSTER_NAME>/services/<Service_name>
    def service_stop(self,service_name=None):
        if not service_name: return self.service_stop_all()
        url="/services/"+service_name
        data='{"RequestInfo": {"context" :"Stop service"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}'
        return self.request(url,data)
    #curl -u admin:admin -H "X-Requested-By: ambari" -X GET "http://10.0.88.52:8080/api/v1/clusters/emr/services/YARN?fields=ServiceInfo/state"
    def service_status(self,service_name):
        url="/services/"+service_name+'?fields=ServiceInfo/state'
        return self.request(url)
    