import json
import ssl
import threading
from http import cookiejar
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

CONFIG_FILE_LOC = 'config/config.json'


def get_configuration():
    with open(CONFIG_FILE_LOC, 'r') as f:
        return json.load(f)


def update_configuration(c):
    with open(CONFIG_FILE_LOC, 'w') as f:
        f.write(json.dumps(c, indent=2))


def get_cookies():
    cookie = cookiejar.MozillaCookieJar(config['COOKIE_FILE'])
    cookie.load()
    return cookie


config = get_configuration()


def get_referer_url(target_url):
    return config['BASE_URL'] + target_url


def get_referer_headers(target_url):
    return {
        'referer': get_referer_url(target_url),
    }


def set_query_field(url, field, value, replace=False):
    components = urlparse(url)
    query_pairs = parse_qsl(urlparse(url).query)

    if replace:
        query_pairs = [(f, v) for (f, v) in query_pairs if f != field]
    query_pairs.append((field, value))

    new_query_str = urlencode(query_pairs)

    new_components = (
        components.scheme,
        components.netloc,
        components.path,
        components.params,
        new_query_str,
        components.fragment
    )
    return urlunparse(new_components)


def get_api_url(target_url, start_value):
    search_url = target_url if target_url.startswith("/") else "/" + target_url
    if search_url[7:10] != '/v1':
        search_url = search_url[:7] + '/v1' + search_url[7:]
    api_url = config['API_BASE_URL'] + search_url
    return set_query_field(api_url, 'start_value', start_value, True)


def set_timeout(interval, func, args=None, kwargs=None):
    threading.Timer(interval, func, args, kwargs).start()
