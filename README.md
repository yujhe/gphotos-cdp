# gphotos-cdp

gphotos-cdp is a program that downloads your photos stored in Google Photos to local storage.

Worked based on [spraot/gphotos-cdp](https://github.com/spraot/gphotos-cdp) (Thanks to @spraot) with some changes:

- Indexed downloaded photos/videos in database: No need to scan the files/dirs in the download directory. Even if you move the downloads to other directory, they will not be re-download.
- Add `-skip-download` flag: Skip download of photos, useful for dry runs or index creation.
- Remove `-n`, `-to`, `-removed`, `-until` flags: Not necessary for my requirements.

## Installation

```sh
go build -o bin/gphotos-cdp
```

## Usage

```txt
./bin/gphotos-cdp -h

Usage of ./bin/gphotos-cdp:
  -album string
        ID of album to download
  -album-type string
        type of album to download (as seen in URL)
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
  -workers int
        number of concurrent downloads allowed (default 1)
```

## Quickstart

### Download All Photos

It will open browser for authentication and downloads all photos to `./photos`

```sh
./bin/gphotos-cdp -download-dir photos -log-level info
# or use `-from` flag to download photos after that date
./bin/gphotos-cdp -download-dir photos -from 2025-01-01 -log-level info
```

### Download Photos in A Album

You can find the album id from URL, it should look like: `album/id123`, `share/id123`.

```sh
# To sync a single album, you can use the -album flag:
./bin/gphotos-cdp -download-dir photos -album album/1234567890ABCDEF -log-level info
# or use `-from` flag to download photos after that date
./bin/gphotos-cdp -download-dir photos -album album/1234567890ABCDEF -from 2025-01-01 -log-level info
```

### Download Photos in Headless Mode

In `headless` mode, no browser is opened for downloads. To run headless mode, you must first complete authentication by `-dev` or `-profile`.

```sh
# To run headless, you must first run with `-dev`:
./bin/gphotos-cdp -dev -download-dir photos -log-level info
# or `-profile` to store authetication result in `profile` dir
./bin/gphotos-cdp -profile profile -download-dir photos -log-level info

# After successful authentication, you can stop the process and run this instead:
./bin/gphotos-cdp -dev -headless -download-dir photos -log-level info
# or you can pass the profile
./bin/gphotos-cdp -profile profile -headless -download-dir photos -log-level info
```
