#!/usr/bin/python3
import sys, argparse, logging, sys, os, hashlib, sqlite3
from pathlib import Path

(SCRIPTNAME, undef) = os.path.splitext(os.path.basename(__file__))
DATABASE='/'.join((os.environ['HOME'], f'{SCRIPTNAME}.sqlite3'))

TABLENAME='fileinfo'
# https://www.sqlite.org/autoinc.html
# rowid is automatic, AUTOINCREMENT is slow and not needed.
sql_create_table=f'''CREATE TABLE IF NOT EXISTS {TABLENAME} (
                        filename TEXT, 
                        sha1sum TEXT
                    )'''
sql_insert_row=f'''INSERT INTO {TABLENAME} (sha1sum, filename) VALUES (?, ?)'''
sql_select_all=f'''SELECT rowid, sha1sum, filename FROM {TABLENAME}'''
sql_drop_table=f'''DROP TABLE IF EXISTS {TABLENAME}'''

# --relative_parent FOO # if <filepath> is relative then /FOO/filepath
# probably also a good idea to be able to store that as config in DB
parser = argparse.ArgumentParser(
    description=f"Exploring SQLite with file data stored in '{DATABASE}'",
    epilog="The database is created as needed."
)
parser.add_argument('--eval', '-e', metavar='SQL', 
    help='run SQL against database')
parser.add_argument('--debug', '-d', metavar='LEVEL', 
    choices=['debug', 'info', 'warning', 'error', 'critical'])

db_maint = parser.add_argument_group('SQLite3 Database')
db_maint.add_argument('--create', action='store_true', 
    help=f"Creates table '{TABLENAME}' in '{DATABASE}'")
db_maint.add_argument('--from_stdin', action='store_true', 
    help=f"Reads '<sha1sum> <filepath>' on STDIN for insert")
db_maint.add_argument('--from_path', metavar='PATH',
    help=f"Finds filepaths and generates sha1sum for insert")
db_maint.add_argument('--select_all', action='store_true', help=sql_select_all)
db_maint.add_argument('--drop', action='store_true', help=sql_drop_table)
db_maint.add_argument('--rm_db', action='store_true', 
    help=f"Removes '{DATABASE}' if found")
args = parser.parse_args()

if args.from_stdin or args.from_path or args.select_all or args.eval:
    args.create = True # make sure DB is available

if args.debug:
    numeric_level = getattr(logging, args.debug.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.debug())
    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=numeric_level)
    logging.info(f"Debug level: {args.debug}")

if __name__ == '__main__':
    conn = sqlite3.connect(DATABASE)
    curs = conn.cursor()

    try: # and except BrokenPipeError: below
        if args.create:
            curs.execute(sql_create_table)
            conn.commit()

        if args.from_stdin or args.from_path:
            curs.execute('''BEGIN''')  # exponentially faster to commit once!
            if args.from_stdin:
                for line in sys.stdin:
                    line = line.strip()
                    if line == 'q': break  # allow interactive quitting
                    sha1sum, filename = line.split(maxsplit=1)
                    curs.execute(sql_insert_row, (sha1sum, filename))
            if args.from_path:
                for path, subdirs, files in os.walk(args.from_path):
                    for filename in files:
                        filename='/'.join((path, filename))
                        sha1sum = hashlib.sha1()
                        sha1sum.update(Path(filename).read_bytes())
                        curs.execute(sql_insert_row, 
                            (sha1sum.hexdigest(), filename))
            curs.execute('''COMMIT''')

        if args.eval:
            logging.debug(f"SQL: {args.eval}")
            for row in curs.execute(args.eval):
                print(row)

        if args.select_all:
            for row in curs.execute(sql_select_all): print(row)

        if args.drop:
            curs.execute(sql_drop_table)
            conn.commit()

        conn.close()

        if args.rm_db and Path(DATABASE).exists(): Path(DATABASE).unlink()

        # https://docs.python.org/3/library/signal.html#note-on-sigpipe
        sys.stdout.flush() # force SIGPIPE inside try:

    except BrokenPipeError:
        # Python flushes standard streams on exit; redirect remaining output
        # to devnull to avoid another BrokenPipeError at shutdown
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1)  # Python exits with error code 1 on EPIPE

