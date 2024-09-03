from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import time
import copy

# works
@dag(start_date=datetime(2023, 5, 1), 
     schedule=None, 
     catchup=False, 
     params={
         "marks": "",
         "dates": "",
         })
def final_dag_test():
    # upstream task returning a list of tuples (index, value)
    @task
    def get_123(**kwargs):
        marks_str = kwargs['params']['marks']
        dates_str = kwargs['params']['dates']
        print("marks_str: " + marks_str)
        print("dates_str: " + dates_str)
        # Split the string by commas and convert the elements to integers
        marks_list = [int(x) for x in marks_str.split(',') if x.strip().isdigit()]
        dates_list = [x for x in dates_str.split(',')]
        dates_list.sort()

        param_list = []
        for index, element in enumerate(dates_list):
            new_param = copy.deepcopy(kwargs['params'])
            new_param["dates"] = [element]
            param_list.append(new_param)
        
        # Return a list of tuples (index, value)
        # return [{"marks": "1,2,3", "dates": "20240801"},{"marks":"5,6,7", "dates": "20240808"}]
        return param_list
    
    trigger_task = TriggerDagRunOperator.partial(
        task_id='trigger_dag2',
        trigger_dag_id='triggered',  # ID of the DAG to trigger
        wait_for_completion=True,  # Optional: If you want the triggering task to wait for the triggered DAG to complete
    ).expand(conf=get_123()) # Optional: Pass any configuration as a dictionary 

    trigger_task_rest = TriggerDagRunOperator(
        task_id='trigger_dag2_rest',
        trigger_dag_id='triggered',  # ID of the DAG to trigger
        conf={"marks": "{{ params.marks }}", "dates": "{{ params.dates }}"},   # Optional: Pass any configuration as a dictionary
        wait_for_completion=True,  # Optional: If you want the triggering task to wait for the triggered DAG to complete
    )

    trigger_task >> trigger_task_rest

final_dag_test()
