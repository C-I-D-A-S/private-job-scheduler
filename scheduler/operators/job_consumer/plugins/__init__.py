""" Collection of queue plugins
"""

from operators.job_consumer.plugins.queue_selector.main import get_queue_selector
from operators.job_consumer.plugins.job_selector.main import get_job_selector
from operators.job_consumer.plugins.job_operator_trigger.main import get_job_trigger

# For JobConsumer Import
QUEUE_SELECTOR = get_queue_selector()
JOB_SELECTOR = get_job_selector()

send_job = get_job_trigger()
