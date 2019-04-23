import logging

import converters.teryt
import converters.prg

logging.basicConfig(level=logging.INFO)


def main():
    converters.teryt.init()
    converters.prg.init()


if __name__ == '__main__':
    main()
