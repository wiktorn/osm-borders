import logging
import os
import time
from xml.sax.saxutils import quoteattr

import boto3
import lz4
from flask import make_response as _make_response
from flask import request, redirect, url_for, render_template
from flask_lambda import FlaskLambda

from converters import teryt, tools

PRG_GMINY_CACHE_V_ = os.environ.get('OSM_BORDERS_CACHE_TABLE')
SNS_TOPIC = os.environ.get('OSM_BORDERS_SNS_TOPIC_ARN')

app = FlaskLambda(__name__)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def make_response(ret, code):
    resp = _make_response(ret, code)
    resp.mimetype = 'text/xml; charset=utf-8'
    return resp


def redirect_to_self():
    resp = redirect(request.url)
    resp.mimetype = 'text/html'
    return resp


@app.route("/osm-borders/get/<typ>/<terc>.osm", methods=["GET", ])
def get_borders(*, typ, terc):
    logger.info("Processing typ: %s, terc: %s", typ, terc)
    if typ not in ('all', 'nosplit', 'split', 'gminy'):
        raise ValueError("Invalid type: {}".format(typ))

    if typ == 'gminy' and len(terc) != 4 or len(terc) != 7:
        raise ValueError("Invalid terc length {} for type {}".format(len(terc), typ))

    request_time = time.time()

    def response(body):
        resp = make_response(body, 200)
        resp.headers['Content-Disposition'] = 'attachment; filename={0}.osm'.format(terc)
        return resp

    logger.info("Using cache table: %s", PRG_GMINY_CACHE_V_)
    cache_table = boto3.resource('dynamodb').Table(PRG_GMINY_CACHE_V_)

    def fetch_from_cache():
        return cache_table.get_item(
            Key={
                'key': tools.join([terc, typ])
            }
        )

    def response_if_cache_valid(entry):
        if entry and \
                'Item' in entry and \
                entry['Item']['ttl'] > time.time() and \
                entry['Item'].get('value'):
            logger.error("cache_return from table")
            return response(lz4.decompress(entry['Item']['value'].value))
        return None

    cache_entry = fetch_from_cache()
    ret = response_if_cache_valid(cache_entry)
    if ret:
        return ret

    if not (cache_entry.get('Item') and not cache_entry['Item'].get('value')):
        # sentinel element not found, create it and send request
        logger.info("Creating sentinel item in cache: %s", tools.join([terc, typ]))
        cache_table.put_item(
            Item={
                'key': tools.join([terc, typ]),
                'ttl': int(time.time()) + 600,
                'request_time': request_time
            }
        )

        logger.info("Sending SNS message initiating request")
        sns = boto3.client('sns')
        sns.publish(
            TopicArn=SNS_TOPIC,
            Message={
                "terc": terc,
                "type": typ,
                "request_time": request_time
            },
            MessageStructure='json'
        )

    logger.error("Sleeping waiting for data")
    time_slept = 0
    while True:
        time.sleep(5)
        time_slept += 5
        ret = response_if_cache_valid(fetch_from_cache())
        if ret:
            return ret

        if time_slept > 50:
            return redirect_to_self()


@app.errorhandler(404)
def page_not_found(e):
    logger.info("Redirecting to: %s", url_for("list_all"))
    resp = redirect("/api" + url_for("list_all"))
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
    else:
        items = [(k, teryt_cache[k]) for k in teryt_cache.keys() if len(k) < 7]
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


app.errorhandler(Exception)(report_exception)
