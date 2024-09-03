"""
### Dynamically map a task over an XCom output and reduce the results

Simple DAG that shows basic dynamic task mapping with a reduce step.
"""

from airflow.decorators import dag, task
from pendulum import datetime
import time


@dag(start_date=datetime(2023, 5, 1), 
     schedule=None, 
     catchup=False, 
     params={
         "marks": "",
         "dates": "",
         })
def triggered():
    # upstream task returning a list or dictionary
    @task
    def get_123(**kwargs):
        marks_str = kwargs['params']['marks']
        print("marks_str: " + marks_str)
        # Split the string by commas and convert the elements to integers
        marks_list = [int(x) for x in marks_str.split(',') if x.strip().isdigit()]
        return marks_list

    # dynamically mapped task iterating over list returned by an upstream task
    @task
    def multiply_by_y(x, y):
        return x * y

    multiplied_vals = multiply_by_y.partial(y=1).expand(x=get_123())

    # optional: having a reducing task after the mapping task
    @task
    def get_sum(vals):
        total = sum(vals)
        return total

    get_sum(multiplied_vals)


triggered()