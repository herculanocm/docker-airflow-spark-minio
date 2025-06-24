from airflow.models import BaseOperator
import time
import urllib3
import logging
from airflow.exceptions import AirflowException
import json

class FetchApiOperator(BaseOperator):

    template_fields = ['url', 'type_request', 'max_retries', 'wait_time', 'xcom_key']

    def __init__(self, task_id: str, url: str, xcom_key: str, max_retries: int = 5, wait_time: int = 5, type_request: str = 'GET', *args, **kwargs):
        super(FetchApiOperator, self).__init__(task_id=task_id, *args, **kwargs)
        self.url = url
        self.xcom_key = xcom_key
        self.max_retries = max_retries
        self.wait_time = wait_time
        self.type_request = type_request
        self.task_id = task_id
        

    def execute(self, context):
        retries = 0
        while retries < self.max_retries:
            logging.info(f"Attempt: {retries + 1}, URL: {self.url}")

            headers_authentication = {
                'Content-Type': 'application/json',
            }

            try:
                http = urllib3.PoolManager()
                resp = http.request(self.type_request, self.url, headers=headers_authentication)
                if 200 <= resp.status < 300:
                    self.xcom_push_result(context, resp)
                    return resp.data.decode('utf-8')  # Return the response if needed
                else:
                    logging.warning(f"Status error: {resp.status}")
                    logging.warning("Retrying...")
            except Exception as e:
                logging.error(f"Error: {e}")
                
            time.sleep(self.wait_time)
            retries += 1

        logging.error("Exceeded maximum retries")
        raise AirflowException("Maximum retries exceeded")

    def xcom_push_result(self, context, result: urllib3.BaseHTTPResponse):
        str_result = result.data.decode('utf-8')
        logging.info(f"Pushing result to XCom with key: {self.xcom_key}, result: {str_result}")
        context['ti'].xcom_push(key=self.xcom_key, value=str_result)