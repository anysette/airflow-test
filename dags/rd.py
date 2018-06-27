"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


sources = [{
    "table" : "input101",
    "type":"sensor",
},{
    "table" : "input102",
    "type":"sensor",
},{
    "table" : "input103",
    "type":"sensor",
},]

jobs = [{
    "table" : "bim1",
    "type":"spark",
    "spark_class": "",
    "dependencies": ["input101", "input102", "input103"]
}, {
    "table" : "bim2",
    "type":"spark",
    "spark_class": "",
    "dependencies": ["input101", "input102", "input103"]
},{
    "table" : "bim3",
    "type":"spark",
    "spark_class": "",
    "dependencies": ["input101", "input102", "input103"]
},{
    "table" : "bimJoin",
    "type":"spark",
    "spark_class": "",
    "dependencies": ["bim1", "bim2", "bim3"]
},
]

class DAGFactory(object):
    def __init__(self, dag):
        self.available_operators = {}
        self.dag = dag

    def create_operator(self, job):
        factory = {
            'spark': self.create_spark_operator,
            'sensor': self.create_sensor_operator
            # operators
        }[job['type']]
        return factory(job)

    def build(self, job):
        op = self.create_operator(job)
        dependencies = [self.available_operators[dep] for dep in job.get('dependencies', [])]
        op.set_upstream(dependencies)
        self.available_operators[job['table']] = op


    def create_spark_operator(self, job):
        task_id = job['table']
        return BashOperator(
            task_id=task_id,
            bash_command='sleep 5',
            retries=3,
            dag=dag
        )

    def create_sensor_operator(self, job):
        task_id = job['table']
        return MySensor(
            task_id=task_id,
            dag=dag)


    def fill(self):
        for job in sources + jobs:
            self.build(job)



class MySensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs):
        super(MySensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        return True




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 6, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


with DAG('processings',
         catchup=True,
         default_args=default_args,
         schedule_interval=timedelta(1)
         ) as dag:
    DAGFactory(dag).fill()

