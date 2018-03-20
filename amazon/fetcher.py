import calendar
import datetime
import json
import logging
import time

import boto3
import os

import lz4.block

import borders.borders
from converters import tools

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PRG_GMINY_CACHE_V_ = os.environ.get('OSM_BORDERS_CACHE_TABLE')


def next_midnight():
    ret = datetime.datetime.now().replace(hour=0, minute=0, second=0)
    return calendar.timegm((ret + datetime.timedelta(days=1)).timetuple())


def cache_ttl():
    return max(next_midnight(), time.time() + 15*60)


def app(event, context):
    logger.info(event)
    logger.info(context)

    cache_table = boto3.resource('dynamodb').Table(name=PRG_GMINY_CACHE_V_)

    for record in event['Records']:
        msg = json.loads(record['Sns']['Message'])
        terc = msg['terc']
        typ = msg['type']
        tstamp = msg['request_time']
        item = cache_table.get_item(
            Key={
                'key': tools.join([terc, typ])
            }
        )

        if item and 'Item' in item and tstamp != item['Item']['request_time']:
            logger.warning("Skipping message. Another item should be processed. tstamp %s != %s", tstamp, item['Item']['request_time]'])
            continue
        ret = None
        if typ == "all":
            ret = borders.borders.get_borders(terc)
        elif typ == 'nosplit':
            ret = borders.borders.get_borders(terc, borders_mapping=lambda x: x, do_clean_borders=False)
        elif typ == 'lvl8':
            ret = borders.borders.get_borders(terc, lambda x: x.tags.get('admin_level') == "8")
        elif typ == 'gminy':
            ret = borders.borders.gminy_prg_as_osm(terc)
        else:
            logger.critical("Unknown type: %s. event: %s", typ, event)

        logger.info("Properly processed terc: %s", terc)
        cache_table.put_item(
            Item={
                'key': tools.join([terc, typ]),
                'ttl': cache_ttl(),
                'value': lz4.block.compress(ret)
            }
        )
