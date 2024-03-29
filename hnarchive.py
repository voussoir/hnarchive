import argparse
import bs4
import datetime
import html
import logging
import requests
import sqlite3
import sys
import time

from voussoirkit import backoff
from voussoirkit import betterhelp
from voussoirkit import httperrors
from voussoirkit import mutables
from voussoirkit import operatornotify
from voussoirkit import pathclass
from voussoirkit import ratelimiter
from voussoirkit import sqlhelpers
from voussoirkit import threadpool
from voussoirkit import treeclass
from voussoirkit import vlogging

log = vlogging.getLogger(__name__, 'hnarchive')

VERSION = '1.0.0'

HEADERS = {
    'User-Agent': f'voussoir/hnarchive v{VERSION}.',
}

session = requests.Session()
session.headers.update(HEADERS)

DB_INIT = '''
BEGIN;
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
CREATE INDEX IF NOT EXISTS index_items_poll on items(poll) WHERE poll IS NOT NULL;
CREATE INDEX IF NOT EXISTS index_items_time on items(time);
CREATE INDEX IF NOT EXISTS index_items_type_time on items(type, time);
CREATE INDEX IF NOT EXISTS index_items_age_at_retrieval on items(retrieved - time);
COMMIT;
'''

def init_db():
    global sql
    global cur

    log.debug('Initializing database.')
    db_path = pathclass.Path('hnarchive.db')
    if db_path.is_link and not db_path.is_file:
        raise RuntimeError(f'{db_path.absolute_path} is a broken link.')

    db_exists = db_path.is_file
    sql = sqlite3.connect(db_path.absolute_path)
    sql.row_factory = sqlite3.Row
    cur = sql.cursor()

    if not db_exists:
        log.debug('Running first-time database setup.')
        sqlhelpers.executescript(conn=sql, script=DB_INIT)

# HELPERS ##########################################################################################

def ctrlc_commit(function):
    def wrapped(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except KeyboardInterrupt:
            commit()
            return 1
    return wrapped

def int_or_none(x):
    if x is None:
        return x
    return int(x)

# API ##############################################################################################

def get(url, retries=1):
    bo = backoff.Quadratic(a=0.2, b=0, c=1, max=10)
    while retries > 0:
        try:
            log.loud(url)
            response = session.get(url, timeout=2)
            httperrors.raise_for_status(response)
            return response
        except (
                httperrors.HTTP429,
                httperrors.HTTP5XX,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
            ):
            # Any other 4XX should raise.
            retries -= 1
            log.loud('Request failed, %d tries remain.', retries)
            time.sleep(bo.next())

    raise RuntimeError(f'Ran out of retries on {url}.')

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
    if threads and threads > 1:
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
    # missed_loops:
    # Usually, livestream assumes that `item is None` means the requested item
    # id hasn't been published yet. But, if that item is actually just deleted,
    # we would be stuck waiting for it forever. missed_loops is used to
    # ocassionally check get_latest_id to see if new items are available, so we
    # know that the current id is really just deleted.
    # Items are released in small batches of < ~10 at a time. It is important
    # that the number in `latest > id+XXX` is big enough that we are sure the
    # requested item is really dead and not just part of a fresh batch that
    # beat our check in a race condition (consider that between the last
    # iteration which triggered the check and the call to get_latest_id, the
    # item we were waiting for is published in a new batch). I chose 50 because
    # catching up with 50 items is not a big deal.
    missed_loops = 0
    while True:
        item = get_item(id)
        if item is None:
            log.debug('%s does not exist yet.', id)
            missed_loops += 1
            if missed_loops % 5 == 0:
                latest = get_latest_id()
                if latest > (id+50):
                    log.debug('Skipping %s because future ids exist.', id)
                    id += 1
                    continue
            time.sleep(bo.next())
            continue
        id += 1
        missed_loops = 0
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
        (qmarks, bindings) = sqlhelpers.insert_filler(row)
        query = f'INSERT INTO items {qmarks}'
        cur.execute(query, bindings)
        log.loud('Inserted item %s.', id)
    else:
        row = {
            'id': id,
            'deleted': bool(data.get('deleted', False)),
            'type': data['type'],
            'author': data.get('by', existing['author']),
            'time': int(data['time']),
            'text': data.get('text', existing['text']),
            'dead': bool(data.get('dead', False)),
            'parent': data.get('parent', None),
            'poll': data.get('poll', existing['poll']),
            'url': data.get('url', existing['url']),
            'score': int_or_none(data.get('score', existing['score'])),
            'title': data.get('title', existing['title']),
            'descendants': int_or_none(data.get('descendants', None)),
            'retrieved': retrieved,
        }
        log.info('Updating item %s.', id)
        (qmarks, bindings) = sqlhelpers.update_filler(row, where_key='id')
        query = f'UPDATE items {qmarks}'
        cur.execute(query, bindings)
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

def select_child_items(id):
    '''
    Return items whose parent is this id.
    '''
    cur.execute('SELECT * FROM items WHERE parent == ?', [id])
    rows = cur.fetchall()
    return rows

def select_poll_options(id):
    '''
    Return items that are pollopts under this given poll id.
    '''
    cur.execute('SELECT * FROM items WHERE poll == ?', [id])
    rows = cur.fetchall()
    return rows

def select_item(id):
    cur.execute('SELECT * FROM items WHERE id == ?', [id])
    row = cur.fetchone()
    return row

def select_latest_id():
    cur.execute('SELECT id FROM items ORDER BY id DESC LIMIT 1')
    row = cur.fetchone()
    if row is None:
        return None
    return row['id']

# RENDERING ########################################################################################

def _fix_ptags(text):
    '''
    The text returned by HN only puts <p> in between paragraphs, they do
    not add closing tags or put an opening <p> on the first paragraph.

    If the user typed a literal <p> then it will have been stored with &lt; and
    &gt; so it won't get messed up here.
    '''
    text = text.replace('<p>', '</p><p>')
    text = '<p>' + text + '</p>'
    return text

def build_item_tree(*, id=None, item=None):
    if id is not None and item is None:
        item = select_item(id)
        if item is None:
            raise ValueError('We dont have that item in the database.')
    elif item is not None and id is None:
        id = item['id']
    else:
        raise TypeError('Please pass only one of id, item.')

    tree = treeclass.Tree(str(id), data=item)
    for child in select_child_items(id):
        tree.add_child(build_item_tree(item=child))
    return tree

def html_render_comment(*, soup, item):
    div = soup.new_tag('div')
    div['class'] = item['type']
    div['id'] = item['id']

    userinfo = soup.new_tag('p')
    div.append(userinfo)

    author = item['author'] or '[deleted]'
    username = soup.new_tag('a', href=f'https://news.ycombinator.com/user?id={author}')
    username.append(author)
    userinfo.append(username)

    userinfo.append(' | ')

    date = datetime.datetime.utcfromtimestamp(item['time'])
    date = date.strftime('%Y %b %d %H:%M:%S')
    timestamp = soup.new_tag('a', href=f'https://news.ycombinator.com/item?id={item["id"]}')
    timestamp.append(date)
    userinfo.append(timestamp)

    text = item['text'] or '[deleted]'
    text = bs4.BeautifulSoup(_fix_ptags(text), 'html.parser')
    div.append(text)
    return div

def html_render_comment_tree(*, soup, tree):
    div = html_render_comment(soup=soup, item=tree.data)

    for child in tree.list_children(sort=lambda node: node.data['time']):
        div.append(html_render_comment_tree(soup=soup, tree=child))

    return div

def html_render_job(*, soup, item):
    div = soup.new_tag('div')
    div['class'] = item['type']
    div['id'] = item['id']

    h = soup.new_tag('h1')
    div.append(h)
    h.append(item['title'])

    if item['text']:
        text = bs4.BeautifulSoup(_fix_ptags(item['text']), 'html.parser')
        div.append(text)

    return div

def html_render_poll(*, soup, item):
    options = select_poll_options(item['id'])
    div = html_render_story(soup=soup, item=item)
    for option in options:
        div.append(html_render_pollopt(soup=soup, item=option))
    return div

def html_render_pollopt(*, soup, item):
    div = soup.new_tag('div')
    div['class'] = item['type']

    text = bs4.BeautifulSoup(_fix_ptags(item['text']), 'html.parser')
    div.append(text)

    points = soup.new_tag('p')
    points.append(f'{item["score"]} points')
    div.append(points)

    return div

def html_render_story(*, soup, item):
    div = soup.new_tag('div')
    div['class'] = item['type']
    div['id'] = item['id']

    h = soup.new_tag('h1')
    div.append(h)
    if item['url']:
        a = soup.new_tag('a', href=item['url'])
        a.append(item['title'])
        h.append(a)
    else:
        h.append(item['title'])
    if item['text']:
        text = bs4.BeautifulSoup(_fix_ptags(item['text']), 'html.parser')
        div.append(text)

    userinfo = soup.new_tag('p')
    div.append(userinfo)

    author = item['author']
    username = soup.new_tag('a', href=f'https://news.ycombinator.com/user?id={author}')
    username.append(author)
    userinfo.append(username)

    userinfo.append(' | ')

    date = datetime.datetime.utcfromtimestamp(item['time'])
    date = date.strftime('%Y %b %d %H:%M:%S')
    timestamp = soup.new_tag('a', href=f'https://news.ycombinator.com/item?id={item["id"]}')
    timestamp.append(date)
    userinfo.append(timestamp)

    userinfo.append(' | ')

    points = soup.new_tag('span')
    points.append(f'{item["score"]} points')
    userinfo.append(points)
    return div

def html_render_page(tree):
    soup = bs4.BeautifulSoup()
    html = soup.new_tag('html')
    soup.append(html)

    head = soup.new_tag('head')
    html.append(head)

    style = soup.new_tag('style')
    style.append('''
    .comment,
    .job,
    .poll,
    .pollopt,
    .story
    {
        padding-left: 20px;
        margin-top: 4px;
        margin-right: 4px;
        margin-bottom: 4px;
    }
    .job, .poll, .story
    {
        border: 2px solid blue;
    }
    body > .story + .comment,
    body > .comment + .comment
    {
        margin-top: 10px;
    }
    .comment, .pollopt
    {
        border: 1px solid black;
    }
    ''')
    head.append(style)

    body = soup.new_tag('body')
    html.append(body)

    item = tree.data

    if item['type'] == 'comment':
        body.append(html_render_comment_tree(soup=soup, tree=tree))

    elif item['type'] == 'job':
        body.append(html_render_job(soup=soup, item=item))

    elif item['type'] == 'poll':
        body.append(html_render_poll(soup=soup, item=item))
        for child in tree.list_children(sort=lambda node: node.data['time']):
            body.append(html_render_comment_tree(soup=soup, tree=child))

    elif item['type'] == 'story':
        body.append(html_render_story(soup=soup, item=item))
        for child in tree.list_children(sort=lambda node: node.data['time']):
            body.append(html_render_comment_tree(soup=soup, tree=child))

    return soup

# COMMAND LINE #####################################################################################

@ctrlc_commit
def get_argparse(args):
    init_db()
    lower = args.lower
    upper = args.upper or get_latest_id()

    ids = range(lower, upper+1)
    items = get_items(ids, threads=args.threads)

    insert_items(items, commit_period=args.commit_period)
    return 0

def html_render_argparse(args):
    init_db()
    for id in args.ids:
        tree = build_item_tree(id=id)
        soup = html_render_page(tree)
        html = str(soup)
        if args.output:
            filename = args.output.format(id=id)
            with open(filename, 'w', encoding='utf-8') as handle:
                handle.write(html)
        else:
            print(html)

@ctrlc_commit
def livestream_argparse(args):
    init_db()
    NOTIFY_EVERY_LINE.set(True)
    insert_items(livestream(), commit_period=args.commit_period)
    return 0

@ctrlc_commit
def update_argparse(args):
    init_db()
    while True:
        lower = select_latest_id() or 1
        upper = get_latest_id()
        if lower == upper:
            break

        ids = range(lower, upper+1)
        items = get_items(ids, threads=args.threads)

        insert_items(items, commit_period=args.commit_period)
    return 0

@ctrlc_commit
def update_items_argparse(args):
    init_db()
    seconds = args.days * 86400
    if args.only_mature:
        then = time.time() - (86400 * 14)
        query = 'SELECT id FROM items WHERE retrieved - time <= ? AND time < ?'
        bindings = [seconds, then]
    else:
        query = 'SELECT id FROM items WHERE retrieved - time <= ?'
        bindings = [seconds]
    cur.execute(query, bindings)
    ids = cur.fetchall()

    log.info('Updating %d items.', len(ids))

    if not ids:
        return 0

    ids = [id for (id,) in ids]
    items = get_items(ids, threads=args.threads)

    insert_items(items, commit_period=args.commit_period)
    return 0

NOTIFY_EVERY_LINE = mutables.Boolean(False)

@operatornotify.main_decorator(subject='hnarchive.py', notify_every_line=NOTIFY_EVERY_LINE)
@vlogging.main_decorator
def main(argv):
    parser = argparse.ArgumentParser(description='Hacker News downloader.')
    subparsers = parser.add_subparsers()

    ################################################################################################

    p_get = subparsers.add_parser(
        'get',
        description='''
        Get items between two IDs, inclusive.
        ''',
    )
    p_get.add_argument(
        '--lower',
        type=int,
        default=1,
        help='''
        Lower bound item ID.
        ''',
    )
    p_get.add_argument(
        '--upper',
        type=int,
        default=None,
        help='''
        Upper bound item ID.
        Default: most recent post.
        ''',
    )
    p_get.add_argument(
        '--threads',
        type=int,
        default=1,
        help='''
        Use this many threads to download items.
        ''',
    )
    p_get.add_argument(
        '--commit_period', '--commit-period',
        type=int,
        default=200,
        help='''
        Commit the database after every this many insertions.
        '''
    )
    p_get.set_defaults(func=get_argparse)

    ################################################################################################

    p_html_render = subparsers.add_parser(
        'html_render',
        aliases=['html-render'],
        description='''
        Render items to HTML -- stories, comment trees, etc.
        ''',
    )
    p_html_render.add_argument(
        'ids',
        nargs='+',
        type=int,
        help='''
        One or more ids to render.
        ''',
    )
    p_html_render.add_argument(
        '--output',
        type=str,
        default=None,
        help='''
        Save the html to this file. Your filename may include "{id}" and
        the item's ID will be formatted into the string. This will be necessary
        if you are rendering multiple IDs in a single invocation.
        ''',
    )
    p_html_render.set_defaults(func=html_render_argparse)

    ################################################################################################

    p_livestream = subparsers.add_parser(
        'livestream',
        description='''
        Watch for new items in an infinite loop.

        Starts from the most recent id in the database.
        ''',
    )
    p_livestream.add_argument(
        '--commit_period', '--commit-period',
        type=int,
        default=200,
        help='''
        Commit the database after every this many insertions.
        ''',
    )
    p_livestream.set_defaults(func=livestream_argparse)

    ################################################################################################

    p_update = subparsers.add_parser(
        'update',
        description='''
        Get new items, from the highest ID in the database to the present.
        ''',
    )
    p_update.add_argument(
        '--threads',
        type=int,
        default=1,
        help='''
        Use this many threads to download items.
        ''',
    )
    p_update.add_argument(
        '--commit_period', '--commit-period',
        type=int,
        default=200,
        help='''
        Commit the database after every this many insertions.
        '''
    )
    p_update.set_defaults(func=update_argparse)

    ################################################################################################

    p_update_items = subparsers.add_parser(
        'update_items',
        aliases=['update-items'],
        description='''
        Redownload items to update their scores, descendant counts, etc.
        ''',
    )
    p_update_items.add_argument(
        '--days',
        type=float,
        required=True,
        help='''
        Update items where the retrieval date is less than X days ahead of the
        submission date.
        Stories are only open for comments for 14 days, so the `descendants`
        count of any story younger than 14 days should be considered volatile.
        It seems the upvote button does not disappear at any age, though I
        don't know whether votes on old submissions will actually count.
        Regardless, votes and comments tend to solidify within a day or two
        after submission so a small number should be sufficient.
        ''',
    )
    p_update_items.add_argument(
        '--threads',
        type=int,
        default=1,
        help='''
        Use this many threads to download items.
        ''',
    )
    p_update_items.add_argument(
        '--only_mature', '--only-mature',
        action='store_true',
        help='''
        If True, only update items where the submission date is more than 14
        days ago. Without this, you will be updating items which are very close
        to the present time, an effort which you may find wasteful.
        ''',
    )
    p_update_items.add_argument(
        '--commit_period', '--commit-period',
        type=int,
        default=200,
        help='''
        Commit the database after every this many insertions.
        '''
    )
    p_update_items.set_defaults(func=update_items_argparse)

    return betterhelp.go(parser, argv)

if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
