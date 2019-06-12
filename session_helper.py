import platform
import sys
import uuid
from datetime import datetime
from multiprocessing import Queue, Event
from pprint import pprint
from queue import Empty as QueueEmpty
from queue import Full as QueueFull
from time import sleep

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from requests.adapters import HTTPAdapter
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry

import scripts.logger_util as Logger
from scripts.utils import *

disable_warnings(InsecureRequestWarning)

config = get_configuration()
logger = Logger.get_logger(__name__)


def _singleton(cls):
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


class _RequestsRetrySession:
    def __init__(self, retries=3, backoff_factor=1, status_forcelist=(500, 502, 504)):
        self.session = self.__create_session(retries, backoff_factor, status_forcelist)
        self.update_auth()

    @staticmethod
    def __create_session(retries, backoff_factor, status_forcelist):
        s = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount('http://', adapter)
        s.mount('https://', adapter)
        s.verify = False
        s.keep_alive = True
        s.cookies.update(get_cookies())
        s.headers.update({
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/70.0.3538.110 Safari/537.36',
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://udaan.com',
            'authority': 'api.udaan.com',
            'scheme': 'https',
            'upgrade-insecure-requests': '1',
        })
        return s

    def update_auth(self):
        max_retry = config['MAX_RETRY'] if isinstance(config['MAX_RETRY'], str) else int(config['MAX_RETRY'])
        retry = max_retry

        updated = False
        request_id = uuid.uuid4()

        url = config['AUTH_URL']
        csrf_token = config['CSRF_TOKEN']

        logger.debug("Updating auth token using auth url '{}' for requestId '{}'".format(url, request_id))
        while retry > 0:
            target_url = '/market/v1'

            if retry < max_retry:
                logger.debug(("Retrying auth update, url {}, requestId {}...Already retired {} times, Max retry {}"
                              ).format(url, request_id, max_retry - retry, max_retry))

            headers = get_referer_headers(target_url)
            headers.update({
                'content-type': 'text/plain;charset=UTF-8',
                'x-csrf-token': csrf_token
            })

            req = requests.Request(method='POST', url=url, data='u=-1', headers=headers)
            prepped = self.session.prepare_request(req)
            # Merge environment settings into session
            settings = self.session.merge_environment_settings(prepped.url, {}, None, False, None)

            # pprint(prepped.__dict__)
            try:
                if config['PROXY']:
                    res = self.session.send(prepped, **settings)
                else:
                    res = self.session.send(prepped)
                if res.status_code != 200:
                    error_message = ("OOPS!! Error occurred while getting auth data from url - {}" +
                                     ", request Id - {}.\nResponse code - {}\nResponse  - {}").format(url,
                                                                                                      request_id,
                                                                                                      res.status_code,
                                                                                                      res.text)
                    logger.error(error_message)
                else:
                    try:
                        auth_res = res.json()
                        token_type = auth_res.get('token_type', "Bearer")
                        auth_token = token_type + " " + auth_res['accessToken']
                        self.session.headers.update({
                            'Authorization': auth_token,
                        })
                        updated = True
                        break
                    except Exception as e:
                        error_message = (
                                "OOPS!! Error occurred while getting json from auth response, url - {}" +
                                ", request Id - {}. Response code - {}\n" +
                                "Response  - {}\nError - {}").format(
                            url,
                            request_id,
                            res.status_code,
                            res.text,
                            str(e))
                        logger.error(error_message)
            except requests.ConnectionError as e:
                logger.error(
                    (
                            "OOPS!! Connection Error while getting auth data from url - {}, request Id - {}. " +
                            "Make sure you are connected to Internet." +
                            " Technical Details given below.\n").format(url, request_id))
                logger.error(str(e))
            except requests.Timeout as e:
                logger.error(
                    "OOPS!! Timeout Error while getting auth data from url - {}, request Id - {}." +
                    "Technical Details given below.\n".format(url, request_id))
                logger.error(str(e))
            except requests.RequestException as e:
                logger.error(
                    "OOPS!! Request Exception while getting auth data from  url - {}, request Id - {}." +
                    "Technical Details given below.\n".format(url, request_id))
                logger.error(str(e))
            except Exception as e:
                logger.error(
                    "OOPS!! General Exception while getting auth data from  url - {}, request Id - {}." +
                    "Technical Details given below.\n".format(url, request_id))
                logger.error(str(e))

            sleep(2)  # Sleeping for 2 sec before retrying again
            retry -= 1

        if not updated:
            logger.error(
                "Auth could not be updated for request Id - {}, hence terminating the service".format(request_id))
            sys.exit(status="Auth could not be updated")


@_singleton
class SessionPool:
    """
    Manage Reusable objects for use by Client objects.
    """

    def __init__(self):
        self.__event = Event()
        self.__event.set()

        self.__session_queue_size = config['NUM_OF_WORKER_PROCESS']
        self.__session_queue = Queue(self.__session_queue_size)
        session_list = [_RequestsRetrySession() for _ in range(self.__session_queue_size)]
        self.__insert_list_items_in_queue(session_list)

    def acquire(self):
        if not self.__event.is_set():
            self.__event.wait()

        return self.__session_queue.get()

    def release(self, session_obj):
        if isinstance(session_obj, _RequestsRetrySession):
            try:
                self.__session_queue.put(session_obj)

            except QueueFull:
                logger.warn('Unable to push into queue at {}, as Queue is Full'.format(datetime.now()))
                pass
        else:
            logger.warn(
                'Receive object is not session object, receive object is of {},' +
                ' Creating a new session and storing in queue'.format(type(session_obj)))
            self.__session_queue.put(_RequestsRetrySession())

    def trigger_update_auth(self):
        logger.debug('Received event for refresh auth token at {}'.format(datetime.now()))
        self.__lock_on_acquire()
        if not self.__session_queue.full():
            if platform.system() == "Darwin":
                __queue_size = self.__session_queue_size
            else:
                __queue_size = self.__session_queue.qsize()
            logger.info('Session pool is not full on {}, there are only {} session out of {}'
                        .format(datetime.now(), self.__get_current_pool_size(), __queue_size))
            set_timeout(5.0, self.trigger_update_auth)
        else:
            session_list = []
            logger.info('All session are there in Session pool on {}. Hence triggering update for all session'
                        .format(datetime.now()))
            for sessionObj in self.__get_all():
                sessionObj.update_auth()
                session_list.append(sessionObj)

            self.__insert_list_items_in_queue(session_list)
            self.__release_lock_on_acquire()
            logger.info('Successfully refreshed auth token for {} sessions at {}'
                        .format(len(session_list), datetime.now()))

    def __insert_list_items_in_queue(self, l):
        for s in l:
            try:
                self.__session_queue.put(s)
            except QueueFull:
                break

    def __lock_on_acquire(self):
        self.__event.clear()  # set internal flag to false

    def __release_lock_on_acquire(self):
        self.__event.set()  # set internal flag to true

    def __get_current_pool_size(self):
        session_list = []
        for session in self.__get_all():
            session_list.append(session)
        current_pool_size = len(session_list)
        self.__insert_list_items_in_queue(session_list)
        return current_pool_size

    def __get_all(self):
        while True:
            try:
                result = self.__session_queue.get(block=False)
                yield result
            except QueueEmpty:
                break


@_singleton
class _AuthUpdateScheduler:

    def __init__(self):
        self.__stop = False
        self.__job_scheduler = BackgroundScheduler()
        self.__job_scheduler.add_job(SessionPool().trigger_update_auth, 'interval', seconds=500, misfire_grace_time=50)

        self.__monitoring_scheduler = BackgroundScheduler()
        self.__monitoring_scheduler.add_job(self.check_progress, 'interval', seconds=20, misfire_grace_time=20)

    def start(self):
        logger.info('Starting Scheduler')
        self.__job_scheduler.start()
        self.__monitoring_scheduler.start()

    def check_progress(self):
        if self.__stop:
            self.__job_scheduler.shutdown()
            self.__monitoring_scheduler.shutdown()

    def stop(self):
        logger.info('Stopping Scheduler')
        self.__stop = True


def start_auth_updater():
    _AuthUpdateScheduler().start()


def stop_auth_updater():
    _AuthUpdateScheduler().stop()
