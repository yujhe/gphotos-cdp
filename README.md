# gphotos-cdp

gphotos-cdp is a program that downloads your photos stored in Google Photos.

Forked from [spraot/gphotos-cdp](https://github.com/spraot/gphotos-cdp).

## Installation

```sh
go build -o bin/gphotos-cdp
```

## Quickstart

```sh
# Opens browser for authentication, downloads all photos to ./photos
./bin/gphotos-cdp -download-dir photos -log-level info

# To run headless, you must first run:
./bin/gphotos-cdp -dev -download-dir photos -log-level info

# After successful authentication, you can stop the process and run this instead:
./bin/gphotos-cdp -dev -headless -download-dir photos -log-level info

# To sync a single album, you can use the -album flag:
./bin/gphotos-cdp -dev -download-dir photos -album 1234567890ABCDEF -log-level info
```

## Usage

```sh
./bin/gphotos-cdp -h

Usage of ./bin/gphotos-cdp:
  -album string
        ID of album to download, has no effect if lastdone file is found or if -start contains full URL
  -album-type string
        type of album to download (as seen in URL), has no effect if lastdone file is found or if -start contains full URL (default "album")
  -batch-size int
        number of photos to download in one batch
  -chrome-exec-path string
        path to Chrome/Chromium binary to use
  -db-file string
        path to the SQLite database file (default "gphotos.db")
  -dev
        dev mode. we reuse the same session dir (/tmp/gphotos-cdp), so we don't have to auth at every run.
  -download-dir string
        where to write the downloads (default "/Users/yujhe.li/Downloads/gphotos-cdp")
  -from string
        earliest date to sync (YYYY-MM-DD)
  -headless
        Start chrome browser in headless mode (must use -dev and have already authenticated).
  -json
        output logs in JSON format
  -log-level string
        log level: debug, info, warn, error, fatal, panic (default "info")
  -profile string
        like -dev, but with a user-provided profile dir
  -run string
        the program to run on each downloaded item, right after it is dowloaded. It is also the responsibility of that program to remove the downloaded item, if desired.
  -skip-download
        skip download of photos, useful for dry runs or metadata collection
  -until string
        stop syncing at this photo
  -workers int
        number of concurrent downloads allowed (default 1)
```

## What?

This program uses the Chrome DevTools Protocol to drive a Chrome session that downloads your photos stored in Google Photos. For each downloaded photo, an external program can be run on it (with the -run flag) right after it is downloaded to e.g. upload it somewhere else.

## Why?

We want to incrementally download our own photos out of Google Photos. Google offers no APIs to do this, so we have to scrape the website.

We can get our original photos out with [Google Takeout](https://takeout.google.com/), but only manually, and slowly. We don't want to have to remember to do it (or remember to renew the time-limited scheduled takeouts) and we'd like our photos mirrored in seconds or minutes, not weeks.

### What if Google Photos breaks this tool on purpose or accident?

I guess we'll have to continually update it.

But that's no different than using people's APIs, because companies all seem to be deprecating and changing their APIs regularly too.
