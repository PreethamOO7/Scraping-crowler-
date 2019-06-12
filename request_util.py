import os
from time import sleep

import requests
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

import scripts.logger_util as Logger
from scripts.session_helper import SessionPool
from scripts.utils import get_configuration

disable_warnings(InsecureRequestWarning)

config = get_configuration()
logger = Logger.get_logger(__name__)


def get_data_from_url(url, headers):
    session_obj = SessionPool().acquire()
    max_retry = config['MAX_RETRY'] if isinstance(config['MAX_RETRY'], str) else int(config['MAX_RETRY'])

    retry = max_retry
    data = None

    while retry:
        pid = os.getpid()
        if retry < max_retry:
            logger.debug(
                "Retrying url {}...Already retired {} times, Max retry {}".format(url, max_retry - retry, max_retry))
        with session_obj.session as session:
            try:
                req = requests.Request(method='GET', url=url, headers=headers)
                prepped = session.prepare_request(req)

                # Merge environment settings into session
                settings = session.merge_environment_settings(prepped.url, {}, None, False, None)

                if config['PROXY']:
                    res = session.send(prepped, timeout=10, **settings)
                else:
                    res = session.send(prepped, timeout=10)

                if res.status_code != 200:
                    logger.error(
                        "Error occurred while getting data from url - {}, PID - {}\nResposne code - {}"
                            .format(url, pid, res.status_code))
                else:
                    if retry < max_retry:
                        logger.debug(("Retry successful... for {} url, PID - {}, after retrying {} times"
                                      ).format(url, pid, max_retry - retry))
                    data = res.json()
                    break
            except requests.ConnectionError as e:
                logger.error(("OOPS!! Connection Error while accessing url - {}, PID - {}." +
                              " Make sure you are connected to Internet." +
                              " Technical Details given below.\n").format(url, pid))
                logger.error(str(e))
            except requests.Timeout as e:
                logger.error(
                    "OOPS!! Timeout Error while accessing url - {}, PID - {}.Technical Details given below.\n".format(
                        url, pid))
                logger.error(str(e))
            except requests.RequestException as e:
                logger.error(
                    ("OOPS!! Request Exception while accessing url - {}, PID - {}.Technical Details given below.\n"
                     ).format(url, pid))
                logger.error(str(e))
            except Exception as e:
                logger.error(
                    ("OOPS!! General Exception while accessing url - {}, PID - {}.Technical Details given below.\n"
                     ).format(url, pid))
                logger.error(str(e))

        sleep(2)  # Sleeping for 2 sec before retrying again
        retry -= 1

    SessionPool().release(session_obj)
    return data
