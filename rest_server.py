import os
from functools import wraps
from xml.sax.saxutils import quoteattr

import time

from flask import Flask, make_response as _make_response
from flask import request, redirect, url_for, render_template
from flask_lambda import FlaskLambda

import borders.borders
import threading
from converters import teryt, tools

import logging

PRG_GMINY_CACHE_V_ = 'osm_prg_gminy_cache_v1'

logger = logging.getLogger(__name__)

if os.environ.get('USE_AWS'):
    app = FlaskLambda(__name__)
    import boto3
    import lz4


    def make_response(ret, code):
        resp = _make_response(ret, code)
        resp.mimetype = 'text/xml; charset=utf-8'
        return resp


    def dynamo_cache(*, table_name: str, cache_key: str, cache_bucket: str, ttl: int = 86400):
        cache_table = boto3.resource('dynamodb').Table(table_name)

        def decorating_function(user_function):
            @wraps(user_function)
            def wrapper(*args, **kwargs):
                logger.error("Start dynamo cache")
                key = kwargs[cache_key]
                ret = cache_table.get_item(
                    Key={
                        'key': tools.join([key, cache_bucket])
                    }
                )
                if ret and 'Item' in ret and ret['Item']['ttl'] > time.time() and ret['Item'].get('value'):
                    logger.error("Return from table")
                    return lz4.decompress(ret['Item']['value'].value)

                if ret.get('Item') and not ret['Item'].get('value'):
                    logger.error("Sleeping waiting for data")
                    time_slept = 0
                    while True:
                        time.sleep(5)
                        time_slept += 5
                        ret = cache_table.get_item(
                            Key={
                                'key': tools.join([key, cache_bucket])
                            }
                        )
                        if ret and 'Item' in ret \
                                and ret['Item']['ttl'] > time.time() \
                                and ret['Item'].get('value'):
                            logger.error("Found data in table")
                            return lz4.decompress(ret['Item']['value'].value)
                        if time_slept > 240:
                            cache_table.delete_item(
                                Key={
                                    'key': tools.join([key, cache_bucket]),
                                }
                            )
                            logger.error("Giving up on waiting for data - removing sentinel object")
                            raise TimeoutError()
                # create sentinel item in cache
                logger.error("Puting sentinel object")
                cache_table.put_item(
                    Item={
                        'key': tools.join([key, cache_bucket]),
                        'ttl': int(time.time()) + 300
                    }
                )
                logger.error("Calling function")
                ret = user_function(*args, **kwargs)
                logger.error("Function returned")
                cache_table.put_item(
                    Item={
                        'key': tools.join([key, cache_bucket]),
                        'value': lz4.block.compress(ret, mode='high_compression'),
                        'ttl': int(time.time()) + ttl
                    }

                )
                logger.error("Putting response to cache")
                return ret

            return wrapper

        return decorating_function

else:
    logger.info("Working in standard rest-server mode")
    app = Flask(__name__)


    def make_response(ret, code):
        resp = _make_response(ret, code)
        resp.mimetype = 'text/xml; charset=utf-8'
        return resp


    def dynamo_cache(*, table_name: str, cache_key: str, cache_bucket: str, ttl: int = 86400):
        def decorating_function(user_function):
            @wraps(user_function)
            def wrapper(*args, **kwargs):
                return user_function(*args, **kwargs)

            return wrapper

        return decorating_function


def async_call(*, timeout: int):

    def decorating_function(user_function):
        class TimeoutedThread(threading.Thread):
            def __init__(self, args, kwargs):
                self.args = args
                self.kwargs = kwargs
                self.ret = None
                super(TimeoutedThread, self).__init__()

            def run(self):
                self.ret = user_function(*self.args, **self.kwargs)

        @wraps(user_function)
        def wrapper(*args, **kwargs):
            t = TimeoutedThread(args=args, kwargs=kwargs)
            t.start()
            t.join(timeout)
            ret = t.ret
            if ret:
                return ret
            else:
                logger.error("Timeout waiting for response")
                raise TimeoutError()
        return wrapper
    return decorating_function

@app.route("/osm-borders/all/<terc>.osm", methods=["GET", ])
@async_call(timeout=45)
@dynamo_cache(table_name=PRG_GMINY_CACHE_V_, cache_key='terc', cache_bucket='all_borders')
def get_all_borders(*, terc):
    resp = make_response(borders.borders.get_borders(terc), 200)
    resp.headers['Content-Disposition'] = 'attachment; filename={0}.osm'.format(terc)
    return resp


@app.route("/osm-borders/nosplit/<terc>.osm", methods=["GET", ])
@async_call(timeout=45)
@dynamo_cache(table_name=PRG_GMINY_CACHE_V_, cache_key='terc', cache_bucket='nosplit_borders')
def get_nosplit_borders(*, terc):
    resp = make_response(borders.borders.get_borders(terc, borders_mapping=lambda x: x, do_clean_borders=False), 200)
    resp.headers['Content-Disposition'] = 'attachment; filename={0}.osm'.format(terc)
    return resp


@app.route("/osm-borders/error<stuff>", methods=["GET", ])
def error(stuff):
    raise ValueError("Sample error")


@app.route("/osm-borders/<terc>.osm", methods=["GET", ])
@async_call(timeout=45)
@dynamo_cache(table_name=PRG_GMINY_CACHE_V_, cache_key='terc', cache_bucket='lvl8_borders')
def get_lvl8_borders(*, terc):
    brd = borders.borders.get_borders(terc, lambda x: x.tags.get('admin_level') == "8")
    resp = make_response(brd, 200)
    resp.headers['Content-Disposition'] = 'attachment; filename={0}.osm'.format(terc)
    return resp


@app.route("/osm-borders/prg/gminy/<terc>.osm", methods=["GET", ])
@async_call(timeout=45)
@dynamo_cache(table_name=PRG_GMINY_CACHE_V_, cache_key='terc', cache_bucket='gminy')
def get_gminy(*, terc):
    resp = make_response(borders.borders.gminy_prg_as_osm(terc), 200)
    resp.headers['Content-Disposition'] = 'attachment; filename={0}-gminy.osm'.format(terc)
    return resp


@app.errorhandler(404)
def page_not_found(e):
    logger.info("Redirecting to: %s", url_for("list_all"))
    # resp = redirect(url_for("list_all"))
    resp = redirect(url_for("list_all"))
    resp.mimetype = 'text/html'
    return resp


@app.route("/osm-borders/list/")
def list_all():
    return render_list(None)


@app.route("/osm-borders/list/<terc>")
def render_list(terc):
    teryt_cache = teryt.teryt()
    if terc:
        items = [(k, teryt_cache[k]) for k in teryt_cache.keys() if k.startswith(terc) and len(k) > len(terc)]
        # items = [(k, v) for k, v in teryt.teryt().keys() if k.startswith(terc) and len(k) > len(terc)]
    else:
        items = [(k, teryt_cache[k]) for k in teryt_cache.keys() if len(k) < 7]
        # items = [(k, v) for k, v in teryt.teryt().keys() if len(k) < 7]
    resp = make_response(render_template('list.html', items=items, teryt=teryt.teryt()), 200)
    resp.mimetype = 'text/html'
    return resp


def report_exception(e):
    app.logger.error('{0}: {1}'.format(request.path, e), exc_info=(type(e), e, e.__traceback__))
    resp = make_response(
        """<?xml version='1.0' encoding='UTF-8'?>
        <osm version="0.6" generator="import adresy merger.py">
            <node id="-1" lon="19" lat="52">
                <tag k="fixme" v=%s />
            </node>
        </osm>""" % quoteattr(repr(e)), 200)
    resp.headers['Content-Disposition'] = 'attachment; filename=error.osm'
    return resp


def redirect_to_self(e):
    resp = redirect(request.url)
    resp.mimetype = 'text/html'
    return resp


def start_rest_server():
    ADMINS = ['logi-osm@vink.pl']
    DEBUG = bool(os.environ.get('DEBUG', False))
    os.sys.stderr.write("Debug mode: {0}\n".format(DEBUG))
    MAILLOG = bool(os.environ.get('MAILLOG', False))
    MAILHOST = os.environ.get('MAILHOST', '127.0.0.1')
    os.sys.stderr.write("Mail logging mode: {0}. SMTP host: {1}\n".format(MAILLOG, MAILHOST))
    if MAILLOG:
        from logging.handlers import SMTPHandler

        mail_handler = SMTPHandler(MAILHOST,
                                   'server-error@vink.pl',
                                   ADMINS, 'OSM Rest-Server Failed')
        mail_handler.setLevel(logging.INFO)
        app.logger.addHandler(mail_handler)

    if not DEBUG:
        app.errorhandler(TimeoutError)(redirect_to_self)
        app.errorhandler(Exception)(report_exception)

    app.run(host='0.0.0.0', port=5002, debug=DEBUG)


if __name__ == '__main__':
    start_rest_server()
