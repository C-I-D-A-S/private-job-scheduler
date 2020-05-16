""" Collection of queue plugins
"""

from operators.job_consumer.plugins.queue_selector.main import get_queue_selector
from operators.job_consumer.plugins.job_selector.main import get_job_selector

# For JobConsumer Import
QUEUE_SELECTOR = get_queue_selector()
JOB_SELECTOR = get_job_selector()
