"""
Module for utility functions
Author: Po-Chun, Lu
"""

import traceback
from typing import Optional

import requests
from loguru import logger


def send_request(request_func):
    """A Decorator for requests module to prevent some exceptions

    Arguments:
        request_func {callable} -- function with requests.get, requests.post
    """

    def wrapper(url, headers=None, data=None):
        try:
            res = request_func(url, headers, data)
            status = res.status_code

            if status != 200:
                logger.warning(f"REQ UNAVAILABLE: {status} - {url} - data: {data}")
            else:
                logger.info(f"{status} - {url}\n - data: {data}\n - Res: {res.json()}")
            return res

        except requests.exceptions.Timeout as error:
            logger.warning(f"UNAVAILABLE: Connection Timeout {error}")
        except requests.exceptions.ConnectionError as error:
            logger.warning(f"UNAVAILABLE: Connection Error {error}")
        except ConnectionRefusedError as error:
            logger.warning(f"UNAVAILABLE: Connection Refused Error {error}")
        except requests.exceptions.MissingSchema:
            logger.warning(f"UNAVAILABLE: URL Schema Error {traceback.format_exc()}")

    return wrapper


@send_request
def send_post_request(url, headers=None, data=None) -> Optional[requests.Response]:
    """ send post requests with error checking
    """
    return requests.post(url, headers=headers, data=data)


@send_request
def send_get_request(url, headers=None, data=None) -> Optional[requests.Response]:
    """ send get requests with error checking
    """
    return requests.get(url, headers=headers, data=data)
