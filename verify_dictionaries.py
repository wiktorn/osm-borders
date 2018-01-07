
import logging

import converters.teryt
import converters.prg


logging.basicConfig(level=logging.ERROR)

converters.teryt.verify()
