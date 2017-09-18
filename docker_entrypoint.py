#!/usr/bin/env python3

import argparse
import logging

import borders.borders
import rest_server
from converters import teryt

__log = logging.getLogger(__name__)

def get_all_borders(terc):
    return borders.borders.get_borders(terc)


def get_nosplit_borders(terc):
    return borders.borders.get_borders(terc, borders_mapping=lambda x: x, do_clean_borders=False)


def get_lvl8_borders(terc):
    return borders.borders.get_borders(terc, lambda x: x.tags.get('admin_level') == "8")


def get_gminy(terc):
    return borders.borders.gminy_prg_as_osm(terc)


def main():
    parser = argparse.ArgumentParser(description="Export admin_level=8 and admin_level=9 borders from EMUiA")
    parser.add_argument('terc',
                        nargs='?',
                        help='TERC code of municipiality',
                        type=str
                        )

    parser.add_argument('--log-level',
                        help='Set logging level, defaults to INFO',
                        dest='log_level',
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                        default='INFO'
                        )

    parser.add_argument('--output', type=argparse.FileType('w+b'),
                        help="Output file, defaults to <terc>.osm")

    parser.add_argument('--mode',
                        help="Mode to run:\n"
                             "all_borders - returns borders splited by common paths\n"
                             "nosplit_borders - returns one line per border\n"
                             "only_lvl8 - returns only admin_level=8 borders with splitting\n"
                             "prg - returns admin_level=7 borders from PRG (use 2 or 4 digits code for prg)",
                        choices=['all_borders', 'nosplit_borders', 'only_lvl8', 'prg'],
                        default='all_borders'
                        )

    parser.add_argument('--server', help="run REST server. Overrides all other options", action='store_true')
    args = parser.parse_args()

    if args.server:
        rest_server.start_rest_server()
        return

    logging.basicConfig(level=logging.getLevelName(args.log_level))

    terc = args.terc

    if not terc:
        print("TERC code is required when not starting REST server.")
        parser.print_usage()
        return

    teryt_entry = teryt.teryt[terc]
    __log.info("Working with {0} {1}".format(teryt_entry.rodz_nazwa, teryt_entry.nazwa))

    if args.mode == 'all_borders':
        data = get_all_borders(terc)
    elif args.mode == 'nosplit_borders':
        data = get_nosplit_borders(terc)
    elif args.mode == 'only_lvl8':
        data = get_lvl8_borders(terc)
    elif args.mode == 'prg':
        data = get_gminy(terc)
    else:
        raise ValueError("Unknown mode: {0}".format(args.mode))

    with args.output if args.output else open("{0}.osm".format(terc), "w+b") as output:
        output.write(data)
        __log.info("Wrote {0} bytes to {1}".format(len(data), output.name))

if __name__ == '__main__':
    main()
