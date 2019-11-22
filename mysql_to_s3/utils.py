from mo_logs import Log

from jx_python import jx
from pyLibrary.sql.mysql import MySQL


def check_database(database, debug):
    # VERIFY WE DO NOT HAVE TOO MANY OTHER PROCESSES WORKING ON STUFF
    with MySQL(**database) as db:
        processes = None
        try:
            processes = jx.filter(
                db.query("show processlist"),
                {
                    "and": [
                        {"neq": {"Command": "Sleep"}},
                        {"neq": {"Info": "show processlist"}},
                    ]
                },
            )
        except Exception as e:
            Log.warning("no database", cause=e)

        if processes:
            if debug:
                Log.warning("Processes are running\n{{list|json}}", list=processes)
            else:
                Log.error("Processes are running\n{{list|json}}", list=processes)
