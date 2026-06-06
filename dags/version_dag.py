import random
import numpy as np

from airflow.sdk import dag , task, task_group , asset 
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

default_args = {'owner': 'airflow', 'retries': 3, 'retry_delay': 10}
@dag(dag_id = "surgon_dag" , schedule= CronTriggerTimetable("30 4 * * *",  timezone= "Asia/Kathmandu") ,default_args=default_args , start_date=datetime(2024, 6, 1) , is_paused_upon_creation= False, catchup= False)
def surgon_dag():
    @task.python
    def task_a(**kwargs):
        ti = kwargs['ti']
        
        dats = [i for i in range(1, 20)]
        ti.xcom_push(key = "dataa", value = dats)
    
    @task.python
    def task_a_par(**kwargs):
        ti = kwargs['ti']
        dats  = [i for i in range(0, 20)]
        ti.xcom_push(key = "datpar" , value = dats)
    
    @task.python
    def task_b(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids = "task_a" , key = "dataa")
        data2 =ti.xcom_pull(task_ids = 'task_a_par',  key = "datpar")
        ba = np.array(data)
        bb = np.array(data2)
        a = np.random.choice(ba, size=5, replace=False)
        b = np.random.choice(bb, size=5, replace=False)
        data_sum = a+b
        data_fin = int(np.sum(data_sum))
        ti.xcom_push(key = "data_fin" , value = data_fin)
        print(f"the final data is {data_fin}")
    
    @task.branch
    def task_brn(**kwargs):
        ti = kwargs['ti']
        sum = ti.xcom_pull(task_ids = "task_b" , key = "data_fin")
        if sum > 100:
            return "task_c"
        else:
            return "task_d"
    
    @task_group( group_id = "task_c" , tooltip = "this is task c group")
    def task_c(**kwargs):
        @task.python
        def task_ca(**kwargs):
            print("this is task ca")
            ti = kwargs['ti']
            data = ti.xcom_pull(task_ids = "task_b" , key = "data_fin")
            data = data*2
            print(f"the data in task ca is {data}")
        @task.python
        def task_cb():
            print("the task is done")
        
        task1 = task_ca()
        task2 = task_cb()
        task1 >> task2
    
    @task_group(group_id="task_d")
    def task_d() :
        @task.python 
        def task_da(**kwargs):
            print("this is task da")
            ti = kwargs['ti']
            data = ti.xcom_pull(task_ids = "task_b" , key = "data_fin")
            data = data/2
            print(f"the data in task da is {data}")
        @task.python
        def task_db():
            print("the task is done")
        
        task1 = task_da()
        task2 = task_db()
        task1 >> task2 

    a = task_a()
    a_par = task_a_par()
    b = task_b()
    c = task_c()
    d = task_d()
    [a , a_par] >> b >> task_brn() >> [c , d]

            
surgon_dag()
        

