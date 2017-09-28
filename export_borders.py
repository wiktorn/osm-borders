import argparse
import logging

from borders.borders import get_borders


def fetch(args):
    args.output.write(get_borders(args.terc[0], filter=lambda x: x.tags.get('admin_level') == "8"))


def init(args):
    import converters.prg
    converters.prg.init()


def main():
    root_parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description="""Fetch administrative borders from EMUiA"""
                                     )
    root_parser.add_argument('--log-level',
                        help='Set logging level (debug=10, info=20, warning=30, error=40, critical=50), default: 20',
                        dest='log_level', default=20, type=int)
    root_parser.set_defaults(func=lambda x: root_parser.print_help())

    subparsers = root_parser.add_subparsers(title='subcommands',
                                            description='valid subcommands',
                                            help='additional help')

    fetch_parser = subparsers.add_parser('fetch', help='Fetch data from EMUiA')
    fetch_parser.add_argument('--output', type=argparse.FileType('w+b'), default='result.osm',
                        help='output file with merged data (default: result.osm)')

    fetch_parser.add_argument('terc', nargs=1, help='county terc code')
    fetch_parser.set_defaults(func=fetch)

    init_parser = subparsers.add_parser('init', help='Initialize dictionary data')
    init_parser.set_defaults(func=init)

    args = root_parser.parse_args()
    logging.basicConfig(level=args.log_level)
    args.func(args)



if __name__ == '__main__':
    main()
