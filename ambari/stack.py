class Stack(object):
    def __init__(self,client,name,version):
        self._info=None
        self.client=client
        self.name=name
        self.version=version
        self.id='{stack.name}-{stack.version}'.format(stack=self)
        self.url="/stacks/{}/versions/{}".format(self.name,self.version)
    @property
    def info(self):
        if not self._info:
            self._info=self.client.get(self.url)
        return self._info
    @property
    def services(self):
        srvs=[]
        for s in self.info['services']:
            srvs.append(Service(stack=self,name=s['StackServices']['service_name']))
        return srvs
    def get_service(self, name):
        for s in self.services:
            if s.name==name:
                return s
        return None
    def get_component(self, name):
        for s in self.services:
            for c in s.components:
                if c.name==name:
                    return c
        return None
    

    @property
    def version_definition(self):
        for v in self.client.version_definitions:
            if v.stack==self: return v
        return None
    topology={                                                                            
        'ZOOKEEPER':{
            'host_groups': {'slave': ['ZOOKEEPER_SERVER'], 'client': ['ZOOKEEPER_CLIENT']}
        },
        'AMBARI_INFRA_SOLR': {
            'host_groups': {'master2': ['INFRA_SOLR'], 'client': ['INFRA_SOLR_CLIENT']},
            'dependencies': ['ZOOKEEPER']
        },
        'AMBARI_METRICS': {
            'host_groups': {'master2': ['METRICS_COLLECTOR','METRICS_GRAFANA'], 'slave': ['METRICS_MONITOR']},
            'configurations':{'ams-grafana-env': {'metrics_grafana_password':'bigdata'}},
            'dependencies': ['ZOOKEEPER']
        },
        'HDFS': {
            'host_groups': {'master1': ['SECONDARY_NAMENODE'], 'master2': ['NAMENODE'], 'slave': ['DATANODE'], 'client': ['HDFS_CLIENT']},
            'dependencies': ['ZOOKEEPER']
        },
        'YARN': {
            'host_groups': {'master1': ['RESOURCEMANAGER'],  'master2': ['APP_TIMELINE_SERVER'], 'slave': ['NODEMANAGER'], 'client': ['YARN_CLIENT']},
            'dependencies': ['HDFS']
        },
        'HBASE': {
            'host_groups': {'master1': ['HBASE_MASTER'], 'slave': ['HBASE_REGIONSERVER'], 'client': ['HBASE_CLIENT']},
            'dependencies': ['HDFS', 'ZOOKEEPER']
        },
        'TEZ': {
            'host_groups': {'client': ['TEZ_CLIENT']},
            'dependencies': ['HDFS', 'MAPREDUCE2', 'YARN']
        },
        'MAPREDUCE2': {
            'host_groups': {'master2': ['HISTORYSERVER'], 'client': ['MAPREDUCE2_CLIENT']},
            'dependencies': ['HDFS', 'TEZ']
        },
        'HIVE': {
            'host_groups': {'master1': ['HIVE_SERVER'], 'master2': ['HIVE_METASTORE','MYSQL_SERVER'], 'client': ['HIVE_CLIENT']},
            'configurations':{'hive-site': {'javax.jdo.option.ConnectionPassword':'bigdata'}},
            'dependencies': ['MAPREDUCE2', 'TEZ', 'YARN', 'ZOOKEEPER', 'HDFS']
        },
        'SPARK2': {
            'host_groups': {'master1': ['SPARK2_JOBHISTORYSERVER'], 'client': ['SPARK2_CLIENT']},
            'dependencies': ['HDFS', 'MAPREDUCE2','YARN', 'HIVE']
        },
        'PIG': {
            'host_groups': {'client': ['PIG']}
        },
        'ZEPPELIN': {
            'host_groups': {'master1': ['ZEPPELIN_MASTER'],},
            'dependencies': ['SPARK2', 'YARN']
        },
        'KAFKA': {
            'host_groups': {'master2': ['KAFKA_BROKER']},
            'dependencies': ['ZOOKEEPER']
        },
        'STORM': {
            'host_groups': {'master1': ['STORM_UI_SERVER'], 'master2': ['NIMBUS','DRPC_SERVER'], 'slave': ['SUPERVISOR']},
            'dependencies': ['ZOOKEEPER']
        },
        'LOGSEARCH': {
            'host_groups': {'master1': ['LOGSEARCH_SERVER'], 'slave': ['LOGSEARCH_LOGFEEDER']},
            'dependencies': ['AMBARI_INFRA_SOLR']
        },
        'OOZIE': {
            'host_groups': {'master2': ['OOZIE_SERVER'], 'client': ['OOZIE_CLIENT']},
            'configurations':{'oozie-site': {'oozie.service.JPAService.jdbc.password':'bigdata'}},
            'dependencies': ['HDFS', 'MAPREDUCE2', 'YARN']
        },
        'ACCUMULO': {
            'host_groups': {'master1': ['ACCUMULO_MASTER','ACCUMULO_GC'], 'master2': ['ACCUMULO_MONITOR','ACCUMULO_TRACER'], 'slave': ['ACCUMULO_TSERVER'], 'client': ['ACCUMULO_CLIENT']},
            'dependencies': ['HDFS', 'ZOOKEEPER']
        },
        'DRUID': {
            'host_groups': {'master1': ['DRUID_BROKER','DRUID_COORDINATOR'], 'master2': ['DRUID_ROUTER','DRUID_OVERLORD'], 'slave': ['DRUID_MIDDLEMANAGER','DRUID_HISTORICAL']},
            'dependencies': ['HDFS']
        },
        'SQOOP': {
            'host_groups': {'client': ['SQOOP']},
            'dependencies': ['HDFS', 'MAPREDUCE2']
        },
        'ATLAS': {
            'host_groups': {'master1': ['ATLAS_SERVER'], 'client': ['ATLAS_CLIENT']},
            'configurations':{'atlas-env': {'atlas.admin.password':'bigdata'}},
            'dependencies': ['HBASE', 'HDFS', 'AMBARI_INFRA_SOLR', 'KAFKA']
        },
        'KERBEROS': {
            'host_groups': {'client': ['KERBEROS_CLIENT']}
        },
        'KNOX': {
            'host_groups': {'master2': ['KNOX_GATEWAY']},
            'configurations':{'knox-env': {'knox_master_secret':'bigdata'}}
        },
        'RANGER': {
            'host_groups': {'master1': ['RANGER_ADMIN'], 'master2': ['RANGER_USERSYNC','RANGER_TAGSYNC']},
            'dependencies': ['AMBARI_INFRA_SOLR']
        },
        'RANGER_KMS': {
            'host_groups': {'master2': ['RANGER_KMS_SERVER']},
            'dependencies': ['HDFS']
        }
    }
    service_groups={
        'minimal': ['HDFS','ZOOKEEPER','YARN','MAPREDUCE2'],
        'typical': ['HDFS','ZOOKEEPER','YARN','MAPREDUCE2', 'AMBARI_INFRA_SOLR', 'AMBARI_METRICS', 'HBASE', 'PIG'],
        'all': [key for key in topology]
    }
    @property
    def blueprint(self):
        for b in self.client.blueprints:
            if b.stack==self: return b
        return None
    def register_blueprint(self, data, name=None, validate_topology=True):
        data['Blueprints']={'stack_name': self.name, 'stack_version': self.version}
        if not name: name=self.id
        b=Blueprint(client=self.client,name=name)
        url=b.url
        if not validate_topology: url+='?validate_topology=false'
        self.client.create(url, data=data)
        return self.blueprint
    def _complete_services(self, services):
        srvs=[]
        for s in services:
            srvs.append(s)
            if 'dependencies' in self.topology[s]:
                for d in self.topology[s]['dependencies']:
                    if not d in srvs:
                        srvs.append(d)
        return srvs
    def _join_group(self, services, group_name):
        group=[]
        for s in services:
            if group_name not in self.topology[s]['host_groups']: continue
            group+=[{'name': c} for c in self.topology[s]['host_groups'][group_name]]
        return group
    def build_blueprint(self, services):
        services=self._complete_services(services)
        data={
            'host_groups': [
            {'name': 'master1', 'cardinality': '1', 'components': self._join_group(services, 'master1')},
            {'name': 'master2', 'cardinality': '1', 'components': self._join_group(services, 'master2')},
            {'name': 'slave', 'cardinality': '1+', 'components': self._join_group(services, 'slave')}
            ],
            'configurations': []
        }
        data['host_groups'][-1]['components']+=self._join_group(services, 'client')
        data['host_groups'][0]['components']+=data['host_groups'][-1]['components']
        data['host_groups'][1]['components']+=data['host_groups'][-1]['components']
        for s in services:
            if 'configurations' in self.topology[s]:
                for key in self.topology[s]['configurations']:
                    value=self.topology[s]['configurations'][key]
                    data['configurations'].append({key: {"properties": value}})
        return data
    def register_blueprint_minimal_triple(self):
        data=self.build_blueprint(self.service_groups['minimal'])
        return self.register_blueprint(data)
    def register_blueprint_minimal_single(self):
        minimal_services=self.service_groups['minimal']
        data={
            "host_groups" : [{"name" : "master", "components" : self._join_group(minimal_services, 'master1')+self._join_group(minimal_services, 'master2')+self._join_group(minimal_services, 'slave')+self._join_group(minimal_services, 'client')}]
        }
        return self.register_blueprint(data)
    def register_blueprint_typical_triple(self):
        data=self.build_blueprint(self.service_groups['typical'])
        return self.register_blueprint(data)
    def register_blueprint_triple(self):
        data=self.build_blueprint(self.service_groups['all'])
        return self.register_blueprint(data)

class Blueprint(object):
    def __init__(self,client,name):
        self.client=client
        self.name=name
        self.url='/blueprints/'+self.name
        self._info=None
    @property
    def info(self):
        if not self._info:
            self._info=self.client.get(self.url)
        return self._info
    def delete(self):
        return self.client.delete(self.url)
    @property
    def stack(self):
        ret = self.client.get_stack(name=self.info['Blueprints']['stack_name'], version=self.info['Blueprints']['stack_version'])
        if not ret: raise Exception('stack not found')
        return ret

class Service(object):
    def __init__(self,stack,name):
        self.stack=stack
        self.name=name
        self.url=self.stack.url+"/services/"+self.name
        self._info=None
        self._components=None
        self._categories=None
        self._dependencies=None
        self._quicklinks=None
    @property
    def info(self):
        if not self._info:
            self._info=self.stack.client.get(self.url)
        return self._info
    @property
    def description(self):
        return self.info['StackServices']['comments']
    @property
    def components(self):
        if self._components is None:
            self._components=[]
            for cpn in self.info['components']:
                self._components.append(Component(service=self,name=cpn['StackServiceComponents']['component_name']))
        return self._components
    def get_component(self,name):
        for c in self.components:
            if c.name==name:
                return c
        return None
    @property
    def quicklinks(self):
        if self._quicklinks is None:
            self._quicklinks=[]
            quick_info=self.stack.client._request(self.info['quicklinks'][0]['href'])['QuickLinkInfo']['quicklink_data']['QuickLinksConfiguration']['configuration']['links']
            for link in quick_info:
                self._quicklinks.append(Quicklink(
                    component=self.get_component(link['component_name']),
                    url="http"+link['url'].split("%@")[1]+"%@"+link['url'].split("%@")[2]+link['port']['http_default_port']+link['url'].split("%@")[3],
                    name=link['name'],
                    http_default_port=link['port']['http_default_port']
                ))
        return self._quicklinks
    @property
    def categories(self):
        if self._categories is None:
            self._categories={}
            for c in self.components:
                if not c.category in self._categories:
                    self._categories[c.category]=[]
                self._categories[c.category].append(c)
        return self._categories
    @property
    def masters(self):
        if 'MASTER' in self.categories:
            return self.categories['MASTER']
        return []
    @property
    def slaves(self):
        if 'SLAVE' in self.categories:
            return self.categories['SLAVE']
        return []
    @property
    def clients(self):
        if 'CLIENT' in self.categories:
            return self.categories['CLIENT']
        return []
    @property
    def dependencies(self):
        if self._dependencies is None:
            self._dependencies=[]
            for c in self.components:
                for d in c.dependencies:
                    if d.service not in self._dependencies:
                        self._dependencies.append(d.service)
        return self._dependencies

class Component(object):
    def __init__(self,service,name):
        self.service=service
        self.name=name
        self.url=self.service.url+'/components/'+self.name
        self._info=None
        self._dependencies=None
    @property
    def info(self):
        if not self._info:
            self._info=self.service.stack.client.get(self.url)
        return self._info
    @property
    def display_name(self):
        return self.info['StackServiceComponents']['display_name']
    @property
    def category(self):
        return self.info['StackServiceComponents']['component_category']
    @property
    def dependencies(self):
        if self._dependencies is None:
            self._dependencies=[]
            for d in self.info['dependencies']:
                self._dependencies.append(self.service.stack.get_component(d['Dependencies']['component_name']))
        return self._dependencies
        
class Quicklink(object):
    def __init__(self,component,name,url,http_default_port):
        self.component=component
        self.name=name
        self.url=url
        self.http_default_port=http_default_port
        
    


