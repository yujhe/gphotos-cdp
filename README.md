gphotos-cdp
========

gphotos-cdp is a program that downloads your photos stored in Google Photos.

Usage
--------
```
# Opens browser for authentication, downloads all photos to ./photos
gphotos-cdp -dldir photos

# To run headless, you must first run:
gphotos-cdp -dev -dldir photos

# After successful authentication, you can stop the process and run this instead:
gphotos-cdp -dev -headless -dldir photos

# To sync a single album, you can use the -album flag:
gphotos-cdp -dev -dldir photos -album 1234567890ABCDEF

# For more options, check the help output:
gphotos-cdp -h
```

What?
--------

This program uses the Chrome DevTools Protocol to drive a Chrome session that
downloads your photos stored in Google Photos.
For each downloaded photo, an external program can be run on it (with the -run
flag) right after it is downloaded to e.g. upload it somewhere else. See the
upload/perkeep program, which uploads to a Perkeep server, for an example.


Why?
--------

We want to incrementally download our own photos out of Google Photos. Google offers
no APIs to do this, so we have to scrape the website.

We can get our original photos out with [Google Takeout](https://takeout.google.com/),
but only manually, and slowly. We don't want to have to remember to do
it (or remember to renew the time-limited scheduled takeouts) and we'd
like our photos mirrored in seconds or minutes, not weeks.


What if Google Photos breaks this tool on purpose or accident?
--------

I guess we'll have to continually update it.

But that's no different than using people's APIs, because companies all seem to
be deprecating and changing their APIs regularly too.

