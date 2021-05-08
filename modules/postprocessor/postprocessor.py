from .utils.dbhandler import DbHandler
from .utils.defaults import DEFAULT_LANGS, DEFAULT_DBNAME, DEFAULT_THRESHOLD, DEFAULT_FORCE, DEFAULT_SKIP


def langs(dbname = DEFAULT_DBNAME) -> list[str]:
    dbhandler = DbHandler(dbname)
    langs = dbhandler.retrieve_langs()
    dbhandler.destroy()
    return langs