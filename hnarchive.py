import argparse
import logging
import requests
import sqlite3
import sys
import time

from voussoirkit import backoff
from voussoirkit import betterhelp
from voussoirkit import ratelimiter
from voussoirkit import sqlhelpers
from voussoirkit import threadpool

log = logging.getLogger('hnarchive')

VERSION = 1

HEADERS = {
    'User-Agent': f'voussoir/hnarchive v{VERSION}.',
}

DB_INIT = '''
PRAGMA user_version = 1;
CREATE TABLE IF NOT EXISTS items(
    id INT PRIMARY KEY NOT NULL,
    deleted INT,
    type TEXT,
    author TEXT,
    time INT,
    text TEXT,
    dead INT,
    parent TEXT,
    poll TEXT,
    url TEXT,
    score INT,
    title TEXT,
    descendants INT,
    retrieved INT
);
CREATE INDEX IF NOT EXISTS index_items_id on items(id);
CREATE INDEX IF NOT EXISTS index_items_parent on items(parent);
CREATE INDEX IF NOT EXISTS index_items_time on items(time);
CREATE INDEX IF NOT EXISTS index_items_type_time on items(type, time);
CREATE INDEX IF NOT EXISTS index_items_age_at_retrieval on items(retrieved - time);
'''
COLUMNS = sqlhelpers.extract_table_column_map(DB_INIT)
ITEMS_COLUMNS = COLUMNS['items']

sql = sqlite3.connect('hnarchive.db')
sql.executescript(DB_INIT)

LOG_LOUD = 1
logging.addLevelName(LOG_LOUD, 'LOUD')
log.loud = lambda *args, **kwargs: log.log(LOG_LOUD, *args, **kwargs)

# HELPERS ##########################################################################################

def int_or_none(x):
    if x is None:
        return x
    return int(x)

# API ##############################################################################################

def get(url, retries=1):
    start_time = time.time()

    bo = backoff.Quadratic(a=0.2, b=0, c=1, max=10)
    while retries > 0:
        log.loud(url)
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            break
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 429:
                pass
            elif 400 <= exc.response.status_code <= 499:
                raise
            retries -= 1
            log.loud('Request failed, %d tries remain.', retries)
            time.sleep(bo.next())
        except requests.exceptions.ConnectionError:
            time.sleep(bo.next())

    end_time = time.time()
    log.loud('%s took %s.', url, end_time - start_time)
    return response

def get_item(id):
    url = f'https://hacker-news.firebaseio.com/v0/item/{id}.json'
    response = get(url, retries=8)
    item = response.json()
    if item is None:
        return None
    if 'time' not in item:
        # For example, 78692 from the api shows {"id": 78692, "type": "story"},
        # but the web says "No such item."
        # https://hacker-news.firebaseio.com/v0/item/78692.json
        # https://news.ycombinator.com/item?id=78692
        return None
    return item

def get_items(ids, threads=None):
    if threads:
        return get_items_multithreaded(ids, threads)
    else:
        return get_items_singlethreaded(ids)

def get_items_multithreaded(ids, threads):
    pool = threadpool.ThreadPool(threads, paused=True)
    job_gen = ({'function': get_item, 'kwargs': {'id': id}} for id in ids)
    pool.add_generator(job_gen)

    for job in pool.result_generator(buffer_size=250):
        if job.exception:
            raise job.exception
        if job.value is not None:
            yield job.value

def get_items_singlethreaded(ids):
    for id in ids:
        item = get_item(id)
        if item is not None:
            yield item

def get_latest_id():
    url = 'https://hacker-news.firebaseio.com/v0/maxitem.json'
    response = get(url)
    latest_id = int(response.text)
    return latest_id

def livestream():
    bo = backoff.Linear(m=2, b=5, max=60)
    id = select_latest_id() or 1
    while True:
        item = get_item(id)
        if item is None:
            time.sleep(bo.next())
            continue
        id += 1
        bo.rewind(2)
        yield item

# DATABASE #########################################################################################

def commit():
    log.info('Committing.')
    sql.commit()

def insert_item(data):
    id = data['id']
    retrieved = int(time.time())

    existing = select_item(id)
    if existing is None:
        row = {
            'id': id,
            'deleted': bool(data.get('deleted', False)),
            'type': data['type'],
            'author': data.get('by', None),
            'time': int(data['time']),
            'text': data.get('text', None),
            'dead': bool(data.get('dead', False)),
            'parent': data.get('parent', None),
            'poll': data.get('poll', None),
            'url': data.get('url', None),
            'score': int_or_none(data.get('score', None)),
            'title': data.get('title', None),
            'descendants': int_or_none(data.get('descendants', None)),
            'retrieved': retrieved,
        }
        log.info('Inserting item %s.', id)
        (qmarks, bindings) = sqlhelpers.insert_filler(ITEMS_COLUMNS, row, require_all=True)
        query = f'INSERT INTO items VALUES({qmarks})'
        sql.execute(query, bindings)
        log.loud('Inserted item %s.', id)
    else:
        row = {
            'id': id,
            'deleted': bool(data.get('deleted', False)),
            'type': data['type'],
            'author': data.get('by', existing.get('author', None)),
            'time': int(data['time']),
            'text': data.get('text', existing.get('text', None)),
            'dead': bool(data.get('dead', False)),
            'parent': data.get('parent', None),
            'poll': data.get('poll', existing.get('poll', None)),
            'url': data.get('url', existing.get('url', None)),
            'score': int_or_none(data.get('score', existing.get('score', None))),
            'title': data.get('title', existing.get('title', None)),
            'descendants': int_or_none(data.get('descendants', None)),
            'retrieved': retrieved,
        }
        log.info('Updating item %s.', id)
        (qmarks, bindings) = sqlhelpers.update_filler(row, where_key='id')
        query = f'UPDATE items {qmarks}'
        sql.execute(query, bindings)
        log.loud('Updated item %s.', id)

    return {'row': row, 'is_new': existing is None}

def insert_items(items, commit_period=200):
    ticker = 0
    for item in items:
        insert_item(item)
        ticker = (ticker + 1) % commit_period
        if ticker == 0:
            commit()
    commit()

def select_item(id):
    cur = sql.execute('SELECT * FROM items WHERE id == ?', [id])
    row = cur.fetchone()

    if row is None:
        return None

    item = dict(zip(ITEMS_COLUMNS, row))
    return item

def select_latest_id():
    cur = sql.execute('SELECT id FROM items ORDER BY time DESC, id ASC LIMIT 1')
    row = cur.fetchone()
    if row is None:
        return None
    return row[0]

# COMMAND LINE #####################################################################################

DOCSTRING = '''
hnarchive.py
============

{get}

{update}

{livestream}

{update_items}
'''.lstrip()

SUB_DOCSTRINGS = dict(
get='''
get:
    Get items between two IDs, inclusive.

    flags:
    --lower:
        Lower bound item ID.

    --upper:
        Upper bound item ID.

    --threads X:
        Use X threads to download items. Default = 1 thread.

    --commit_period X:
        Commit the database after every X insertions. Default = 1000.
'''.strip(),

update='''
update:
    Get new items, from the highest ID in the database to the present.

    flags:
    --threads X:
        Use X threads to download items. Default = 1 thread.
'''.strip(),

update_items='''
update_items:
    Redownload items to update their scores, descendant counts, etc.

    flags:
    --days X:
        Update items where the retrieval date is less than X days ahead of the
        submission date.
        Stories are only open for comments for 14 days.
        It seems the upvote button does not disappear at any age, though I
        don't know whether votes on old submissions will actually count.
        Regardless, votes tend to solidify within a day or two after
        submission so a small number should be sufficient.

    --threads X:
        Use X threads to download items. Default = 1 thread.

    --only_mature:
        If True, only update items where the submission date is more than 14
        days ago. Without this, you will be updating items which are very close
        to the present time, an effort which you may find wasteful.
'''.strip(),

livestream='''
livestream:
    Watch for new items in an infinite loop.
'''.strip(),
)

DOCSTRING = betterhelp.add_previews(DOCSTRING, SUB_DOCSTRINGS)

def get_argparse(args):
    lower = args.lower or 1
    upper = args.upper or get_latest_id()

    ids = range(lower, upper+1)
    items = get_items(ids, threads=args.threads)

    try:
        insert_items(items, commit_period=args.commit_period)
    except KeyboardInterrupt:
        commit()

def livestream_argparse(args):
    try:
        insert_items(livestream())
    except KeyboardInterrupt:
        commit()

def update_argparse(args):
    try:
        while True:
            lower = select_latest_id() or 1
            upper = get_latest_id()
            if lower == upper:
                break

            ids = range(lower, upper+1)
            items = get_items(ids, threads=args.threads)

            insert_items(items)
    except KeyboardInterrupt:
        commit()

def update_items_argparse(args):
    seconds = args.days * 86400
    if args.only_mature:
        then = time.time() - (86400 * 14)
        query = 'SELECT id FROM items WHERE retrieved - time <= ? AND time < ?'
        bindings = [seconds, then]
    else:
        query = 'SELECT id FROM items WHERE retrieved - time <= ?'
        bindings = [seconds]
    cur = sql.execute(query, bindings)
    ids = cur.fetchall()

    ids = [id for (id,) in ids]
    items = get_items(ids, threads=args.threads)

    try:
        insert_items(items)
    except KeyboardInterrupt:
        commit()

def main(argv):
    logging.basicConfig()
    if '--loud' in argv:
        log.setLevel(LOG_LOUD)
        argv.remove('--loud')
    elif '--debug' in argv:
        log.setLevel(logging.DEBUG)
        argv.remove('--debug')
    elif '--quiet' in argv:
        log.setLevel(logging.ERROR)
        argv.remove('--quiet')
    else:
        log.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers()

    p_get = subparsers.add_parser('get')
    p_get.add_argument('--lower', type=int, default=None)
    p_get.add_argument('--upper', type=int, default=None)
    p_get.add_argument('--threads', type=int, default=None)
    p_get.add_argument('--commit_period', '--commit-period', type=int, default=1000)
    p_get.set_defaults(func=get_argparse)

    p_livestream = subparsers.add_parser('livestream')
    p_livestream.set_defaults(func=livestream_argparse)

    p_update = subparsers.add_parser('update')
    p_update.add_argument('--threads', type=int, default=None)
    p_update.set_defaults(func=update_argparse)

    p_update_items = subparsers.add_parser('update_items', aliases=['update-items'])
    p_update_items.add_argument('--days', type=float, required=True)
    p_update_items.add_argument('--threads', type=int, default=None)
    p_update_items.add_argument('--only_mature', '--only-mature', action='store_true')
    p_update_items.set_defaults(func=update_items_argparse)

    return betterhelp.subparser_main(
        argv,
        parser,
        main_docstring=DOCSTRING,
        sub_docstrings=SUB_DOCSTRINGS,
    )

if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
