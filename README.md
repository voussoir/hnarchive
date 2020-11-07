hnarchive
=========

hnarchive downloads all HN items (threads and comments) into an SQLite database. At this time, my database is 23.18 GiB with just over 25,000,000 items. I'd be happy to share it.

Please `pip install requests` and `pip install voussoirkit`.

According to the [HN API docs](https://github.com/HackerNews/API) there is no enforced ratelimit, so just use a `threads` count that seems polite.

To get started, just run `python hnarchive.py update` and it will start from 1. In the future, you can run `update` on a cronjob or use `livestream` to get new items forever.

Notes:

- `update` always starts from the highest ID in the database. If you use `get` to get a range of IDs that is ahead of your update schedule, your next `update` will miss the skipped IDs.

- `update_items` will overwrite previously fetched data with the new properties. Please know that HN moderators occasionally migrate comments between threads, adjust thread titles, etc. HN has a tight window in which authors can edit their own posts so you can expect actual item texts to remain pretty static outside of moderator action.

  The exception is if an item is deleted and comes back as `None` from the server, then hnarchive keeps the old data.

Here are all of the subcommands:

    get:
        Get items between two IDs, inclusive.

        flags:
        --lower id:
            Lower bound item ID.

        --upper id:
            Upper bound item ID.

        --threads X:
            Use X threads to download items. Default = 1 thread.

        --commit_period X:
            Commit the database after every X insertions. Default = 200.

    livestream:
        Watch for new items in an infinite loop.

        flags:
        --commit_period X:
            Commit the database after every X insertions. Default = 200.

    update:
        Get new items, from the highest ID in the database to the present.

        flags:
        --threads X:
            Use X threads to download items. Default = 1 thread.

        --commit_period X:
            Commit the database after every X insertions. Default = 200.

    update_items:
        Redownload items to update their scores, descendant counts, etc.

        flags:
        --days X:
            Update items where the retrieval date is less than X days ahead of the
            submission date.
            Stories are only open for comments for 14 days, so the `descendants`
            count of any story younger than 14 days should be considered volatile.
            It seems the upvote button does not disappear at any age, though I
            don't know whether votes on old submissions will actually count.
            Regardless, votes and comments tend to solidify within a day or two
            after submission so a small number should be sufficient.

        --threads X:
            Use X threads to download items. Default = 1 thread.

        --only_mature:
            If True, only update items where the submission date is more than 14
            days ago. Without this, you will be updating items which are very close
            to the present time, an effort which you may find wasteful.

        --commit_period X:
            Commit the database after every X insertions. Default = 200.

https://github.com/voussoir/hnarchive

https://gitlab.com/voussoir/hnarchive
