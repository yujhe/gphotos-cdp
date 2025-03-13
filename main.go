/*
Copyright 2019 The Perkeep Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// The gphotos-cdp program uses the Chrome DevTools Protocol to drive a Chrome session
// that downloads your photos stored in Google Photos.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/evilsocket/islazy/zip"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/css"
	"github.com/chromedp/cdproto/dom"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/cdproto/page"
	cdpruntime "github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/kb"

	"slices"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	nItemsFlag      = flag.Int("n", -1, "number of items to download. If negative, get them all.")
	devFlag         = flag.Bool("dev", false, "dev mode. we reuse the same session dir (/tmp/gphotos-cdp), so we don't have to auth at every run.")
	downloadDirFlag = flag.String("dldir", "", "where to write the downloads. defaults to $HOME/Downloads/gphotos-cdp.")
	profileFlag     = flag.String("profile", "", "like -dev, but with a user-provided profile dir")
	fromFlag        = flag.String("from", "", "earliest date to sync (YYYY-MM-DD)")
	toFlag          = flag.String("to", "", "latest date to sync (YYYY-MM-DD)")
	runFlag         = flag.String("run", "", "the program to run on each downloaded item, right after it is dowloaded. It is also the responsibility of that program to remove the downloaded item, if desired.")
	verboseFlag     = flag.Bool("v", false, "be verbose")
	headlessFlag    = flag.Bool("headless", false, "Start chrome browser in headless mode (must use -dev and have already authenticated).")
	jsonLogFlag     = flag.Bool("json", false, "output logs in JSON format")
	logLevelFlag    = flag.String("loglevel", "", "log level: debug, info, warn, error, fatal, panic")
	removedFlag     = flag.Bool("removed", false, "save list of files found locally that appear to be deleted from Google Photos")
	workersFlag     = flag.Int("workers", 4, "number of concurrent downloads allowed")
	albumIdFlag     = flag.String("album", "", "ID of album to download, has no effect if lastdone file is found or if -start contains full URL")
	albumTypeFlag   = flag.String("albumtype", "album", "type of album to download (as seen in URL), has no effect if lastdone file is found or if -start contains full URL")
	batchSizeFlag   = flag.Int("batchsize", 20, "number of photos to download in one batch")
)

const gphotosUrl = "https://photos.google.com"
const tick = 500 * time.Millisecond
const originalSuffix = "_original"

var errStillProcessing = errors.New("video is still processing & can be downloaded later")
var errCouldNotPressDownloadButton = errors.New("could not press download button")
var errPhotoTakenBeforeFromDate = errors.New("photo taken before from date")
var errPhotoTakenAfterToDate = errors.New("photo taken after to date")
var fromDate time.Time
var toDate time.Time
var loc GPhotosLocale

func main() {
	zerolog.TimestampFieldName = "dt"
	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.999Z07:00"
	flag.Parse()
	if *nItemsFlag == 0 {
		return
	}
	if *verboseFlag && *logLevelFlag == "" {
		*logLevelFlag = "debug"
	}
	level, err := zerolog.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Fatal().Err(err).Msgf("-loglevel argument not valid")
	}
	zerolog.SetGlobalLevel(level)
	if !*jsonLogFlag {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}
	if (!*devFlag && *profileFlag == "") && *headlessFlag {
		log.Fatal().Msg("-headless only allowed in dev mode or if -profile dir is set")
	}
	if *devFlag && *profileFlag != "" {
		log.Fatal().Msg("only one of -dev and -profile can be used")
	}
	if *albumIdFlag != "" && (*fromFlag != "" || *toFlag != "") {
		log.Fatal().Msg("-from and -to cannot be used with -album")
	}

	// Set XDG_CONFIG_HOME and XDG_CACHE_HOME to a temp dir to solve issue in newer versions of Chromium
	if os.Getenv("XDG_CONFIG_HOME") == "" {
		if err := os.Setenv("XDG_CONFIG_HOME", filepath.Join(os.TempDir(), ".chromium")); err != nil {
			log.Fatal().Msgf("err %v", err)
		}
	}
	if os.Getenv("XDG_CACHE_HOME") == "" {
		if err := os.Setenv("XDG_CACHE_HOME", filepath.Join(os.TempDir(), ".chromium")); err != nil {
			log.Fatal().Msgf("err %v", err)
		}
	}

	if *fromFlag != "" {
		var err error
		fromDate, err = time.Parse(time.DateOnly, *fromFlag)
		if err != nil {
			log.Fatal().Msgf("could not parse -from argument %s, must be YYYY-MM-DD", *fromFlag)
		}
	}
	if *toFlag != "" {
		var err error
		toDate, err = time.Parse(time.DateOnly, *toFlag)
		toDate = toDate.Add(time.Hour * 24)
		if err != nil {
			log.Fatal().Msgf("could not parse -to argument %s, must be YYYY-MM-DD", *toFlag)
		}
	}

	s, err := NewSession()
	if err != nil {
		log.Err(err).Msgf("failed to create session")
		return
	}
	defer s.Shutdown()

	log.Info().Msgf("session dir: %v", s.profileDir)

	if err := s.cleanDownloadDir(); err != nil {
		log.Err(err).Msgf("failed to clean download directory %v", s.downloadDir)
		return
	}

	ctx, cancel := s.NewWindow()
	defer cancel()

	if err := s.login(ctx); err != nil {
		log.Err(err).Msg("login failed")
		return
	}

	locale, err := s.getLocale(ctx)
	if err != nil {
		log.Err(err).Msg("checking the locale failed")
		return
	}

	initLocales()
	_loc, exists := locales[locale]
	if !exists {
		log.Warn().Msgf("your Google account is using unsupported locale %s, this is likely to cause issues. Please change account language to English (en) or another supported locale", locale)
	} else {
		log.Info().Msgf("using locale %s", locale)
		loc = _loc
	}

	if err := chromedp.Run(ctx,
		chromedp.ActionFunc(s.firstNav),
	); err != nil {
		log.Fatal().Msgf("failed to run first nav: %v", err)
	}

	if err := chromedp.Run(ctx,
		chromedp.ActionFunc(s.resync),
	); err != nil {
		log.Fatal().Msgf("failure during sync: %v", err)
	}

	log.Info().Msg("Done")
}

type PhotoData struct {
	date     time.Time
	filename string
}

type Job struct {
	imageIds []string
	errChan  chan error
}

type NewDownload struct {
	GUID              string
	suggestedFilename string
}

type DownloadChannels struct {
	downloadStarted  chan NewDownload
	downloadProgress chan bool
}

var lastMessageForKey map[string]time.Time = make(map[string]time.Time)
var muLog sync.Mutex = sync.Mutex{}

func timedLogReady(key string, duration time.Duration) bool {
	if lastMessageForKey != nil {
		muLog.Lock()
		defer muLog.Unlock()
		t := lastMessageForKey[key]
		if t.IsZero() || time.Since(t) >= duration {
			lastMessageForKey[key] = time.Now()
			return true
		}
	}
	return false
}

type Session struct {
	parentContext    context.Context
	chromeExecCancel context.CancelFunc
	downloadDir      string // dir where the photos get stored
	downloadDirTmp   string // dir where the photos get stored temporarily
	profileDir       string // user data session dir. automatically created on chrome startup.
	startNodeParent  *cdp.Node
	nextDownload     chan DownloadChannels
	globalErrChan    chan error
	photoRelPath     string
	existingItems    map[string]struct{}
	foundItems       map[string]struct{}
	downloadedItems  map[string]struct{}
}

func NewSession() (*Session, error) {
	photoRelPath := ""
	if *albumIdFlag != "" {
		i := strings.LastIndex(*albumIdFlag, "/")
		if i != -1 {
			if *albumTypeFlag != "album" {
				log.Warn().Msgf("-albumtype argument is ignored because it looks like given album ID already contains a type: %v", *albumIdFlag)
			}
			photoRelPath = "/" + *albumIdFlag
			*albumIdFlag = photoRelPath[i+1:]
		} else {
			photoRelPath = "/" + *albumTypeFlag + "/" + *albumIdFlag
		}
	}
	log.Info().Msgf("syncing files at root dir %s%s", gphotosUrl, photoRelPath)
	var dir string
	if *devFlag {
		dir = filepath.Join(os.TempDir(), "gphotos-cdp")
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	} else if *profileFlag != "" {
		dir = *profileFlag
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
	} else {
		var err error
		dir, err = os.MkdirTemp("", "gphotos-cdp")
		if err != nil {
			return nil, err
		}
	}
	downloadDir := *downloadDirFlag
	if downloadDir == "" {
		downloadDir = filepath.Join(os.Getenv("HOME"), "Downloads", "gphotos-cdp")
	}
	if err := os.MkdirAll(downloadDir, 0700); err != nil {
		return nil, err
	}

	downloadDirEntries, err := os.ReadDir(downloadDir)
	if err != nil {
		return nil, err
	}
	existingDownloadFoldersMap := make(map[string]struct{}, len(downloadDirEntries))
	for _, e := range downloadDirEntries {
		if e.IsDir() && e.Name() != "tmp" {
			existingDownloadFoldersMap[e.Name()] = struct{}{}
		}
	}

	downloadDirTmp := filepath.Join(downloadDir, "tmp")
	if err := os.MkdirAll(downloadDirTmp, 0700); err != nil {
		return nil, err
	}

	s := &Session{
		profileDir:      dir,
		downloadDir:     downloadDir,
		downloadDirTmp:  downloadDirTmp,
		nextDownload:    make(chan DownloadChannels, 1),
		globalErrChan:   make(chan error, 1),
		photoRelPath:    photoRelPath,
		existingItems:   existingDownloadFoldersMap,
		foundItems:      make(map[string]struct{}),
		downloadedItems: make(map[string]struct{}),
	}
	return s, nil
}

func (s *Session) NewWindow() (context.Context, context.CancelFunc) {
	log.Info().Msgf("starting Chrome browser")

	// Let's use as a base for allocator options (It implies Headless)
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.DisableGPU,
		chromedp.UserDataDir(s.profileDir),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("lang", "en-US,en"),
		chromedp.Flag("accept-lang", "en-US,en"),
		chromedp.Flag("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36"),
	)

	if !*headlessFlag {
		// undo the three opts in chromedp.Headless() which is included in DefaultExecAllocatorOptions
		opts = append(opts, chromedp.Flag("headless", false))
		opts = append(opts, chromedp.Flag("hide-scrollbars", false))
		opts = append(opts, chromedp.Flag("mute-audio", false))
		// undo DisableGPU from above
		opts = append(opts, chromedp.Flag("disable-gpu", false))
	}
	ctx, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
	s.chromeExecCancel = cancel

	ctx, cancel = chromedp.NewContext(ctx,
		chromedp.WithLogf(s.cdpLog),
		chromedp.WithDebugf(s.cdpDebug),
		chromedp.WithErrorf(s.cdpError),
	)
	s.parentContext = ctx
	ctx = SetContextData(ctx)

	if err := chromedp.Run(ctx,
		chromedp.ActionFunc(func(ctx context.Context) error {
			c := chromedp.FromContext(ctx)
			return browser.SetDownloadBehavior(browser.SetDownloadBehaviorBehaviorAllowAndName).WithDownloadPath(s.downloadDirTmp).WithEventsEnabled(true).
				// use the Browser executor so that it does not pass "sessionId" to the command.
				Do(cdp.WithExecutor(ctx, c.Browser))
		}),
	); err != nil {
		panic(err)
	}

	startDownloadListener(ctx, s.nextDownload, s.globalErrChan)

	return ctx, cancel
}

func (s *Session) Shutdown() {
	s.chromeExecCancel()
}

func (s *Session) cdpLog(format string, v ...any) {
	if !strings.Contains(format, "unhandled") && !strings.Contains(format, "event") {
		log.Debug().Msgf(format, v...)
	}
}

func (s *Session) cdpDebug(format string, v ...any) {
	// log.Trace().Msgf(format, v...)
}

func (s *Session) cdpError(format string, v ...any) {
	if !strings.Contains(format, "unhandled") && !strings.Contains(format, "event") {
		log.Error().Msgf(format, v...)
	}
}

// cleanDownloadDir removes all files (but not directories) from s.downloadDir
func (s *Session) cleanDownloadDir() error {
	if s.downloadDir == "" {
		return nil
	}
	entries, err := os.ReadDir(s.downloadDirTmp)
	if err != nil {
		return err
	}
	for _, v := range entries {
		if v.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(s.downloadDirTmp, v.Name())); err != nil {
			return err
		}
	}
	return nil
}

// login navigates to https://photos.google.com/ and waits for the user to have
// authenticated (or for 2 minutes to have elapsed).
func (s *Session) login(ctx context.Context) error {
	log.Info().Msg("starting authentication...")
	return chromedp.Run(ctx,
		chromedp.Navigate(gphotosUrl),
		// when we're not authenticated, the URL is actually
		// https://www.google.com/photos/about/ , so we rely on that to detect when we have
		// authenticated.
		chromedp.ActionFunc(func(ctx context.Context) error {
			tick := time.Second
			timeout := time.Now().Add(2 * time.Minute)
			var location string
			for {
				if time.Now().After(timeout) {
					return errors.New("timeout waiting for authentication")
				}
				if err := chromedp.Location(&location).Do(ctx); err != nil {
					return err
				}
				if strings.HasPrefix(location, gphotosUrl) {
					return nil
				}
				if *headlessFlag {
					captureScreenshot(ctx, filepath.Join(s.downloadDir, "error.png"))
					return errors.New("authentication not possible in -headless mode, see error.png (URL=" + location + ")")
				}
				log.Debug().Msgf("not yet authenticated, at: %v", location)
				time.Sleep(tick)
			}
		}),
		chromedp.ActionFunc(func(ctx context.Context) error {
			log.Info().Msg("successfully authenticated")
			return nil
		}),
	)
}

func (s *Session) getLocale(ctx context.Context) (string, error) {
	var locale string

	err := chromedp.Run(ctx,
		chromedp.EvaluateAsDevTools(`
				(function() {
					// Try to get locale from html lang attribute
					const htmlLang = document.documentElement.lang;
					if (htmlLang) return htmlLang;
					
					// Try to get locale from meta tags
					const metaLang = document.querySelector('meta[property="og:locale"]');
					if (metaLang) return metaLang.content;
					
					// Try to get locale from Google's internal data
					const scripts = document.getElementsByTagName('script');
					for (const script of scripts) {
						if (script.text && script.text.includes('"locale"')) {
							const match = script.text.match(/"locale":\s*"([^"]+)"/);
							if (match) return match[1];
						}
					}
					
					return "unknown";
				})()
			`, &locale),
	)

	if err != nil {
		log.Warn().Err(err).Msg("failed to detect account locale, will assume English (en)")
		return "en", nil
	}

	return locale, nil
}

func captureScreenshot(ctx context.Context, filePath string) {
	var buf []byte

	log.Trace().Msgf("saving screenshot to %v", filePath+".png")
	if err := chromedp.Run(ctx, chromedp.CaptureScreenshot(&buf)); err != nil {
		log.Err(err).Msgf("failed to capture screenshot: %v", err)
	} else if err := os.WriteFile(filePath+".png", buf, os.FileMode(0666)); err != nil {
		log.Err(err).Msgf("failed to write screenshot: %v", err)
	}

	// Dump the HTML to a file
	var html string
	if err := chromedp.Run(ctx, chromedp.OuterHTML("html", &html, chromedp.ByQuery)); err != nil {
		log.Err(err).Msgf("failed to get HTML: %v", err)
	} else if err := os.WriteFile(filePath+".html", []byte(html), 0640); err != nil {
		log.Err(err).Msgf("failed to write HTML: %v", err)
	}
}

// firstNav does either of:
// 1) if a specific photo URL was specified with *startFlag, it navigates to it
// 2) if the last session marked what was the most recent downloaded photo, it navigates to it
// 3) otherwise it jumps to the end of the timeline (i.e. the oldest photo)
func (s *Session) firstNav(ctx context.Context) (err error) {
	// This is only used to ensure page is loaded
	if err := s.setFirstItem(ctx); err != nil {
		return err
	}

	if s.photoRelPath != "" {
		resp, err := chromedp.RunResponse(ctx, chromedp.Navigate(gphotosUrl+s.photoRelPath))
		if err != nil {
			return err
		}
		code := resp.Status
		if code != http.StatusOK {
			return fmt.Errorf("unexpected %d code when restarting to %s%s", code, gphotosUrl, s.photoRelPath)
		}
		chromedp.WaitReady("body", chromedp.ByQuery).Do(ctx)
	}

	if *toFlag != "" {
		t, err := time.Parse("2006-01-02", *toFlag)
		if err != nil {
			log.Err(err).Msgf("error parsing -to argument '%s': %s", *toFlag, err.Error())
			return errors.New("-to argument must be of format 'YYYY-MM-DD'")
		}
		startDate := t

		time.Sleep(500 * time.Millisecond)
		log.Info().Msgf("attempting to scroll to %v", startDate)

		if err := s.navToEnd(ctx); err != nil {
			return err
		}

		// Find class name for date nodes
		dateNodesClassName := ""
		for range 20 {
			chromedp.Evaluate(`
				document.querySelector('`+getAriaLabelSelector(loc.SelectAllPhotosLabel)+`').parentNode.childNodes[1].childNodes[0].childNodes[0].childNodes[0].className
				`, &dateNodesClassName).Do(ctx)
			if dateNodesClassName != "" {
				break
			}
			chromedp.KeyEvent(kb.PageUp).Do(ctx)
			time.Sleep(100 * time.Millisecond)
		}
		if dateNodesClassName == "" {
			return errors.New("failed to find date nodes class name")
		}

		bisectBounds := []float64{0.0, 1.0}
		scrollPos := 0.0
		var foundDateNode, matchedNode *cdp.Node
		for range 100 {
			scrollTarget := (bisectBounds[0] + bisectBounds[1]) / 2
			log.Debug().Msgf("scrolling to %.2f%%", scrollTarget*100)
			for range 20 {
				if err := chromedp.Evaluate(fmt.Sprintf(`
						(function() {
							const main = document.querySelector('[role="main"]');
							const scrollTarget = %f;
							main.scrollTo(0, main.scrollHeight*scrollTarget);
						})();
					`, scrollTarget), nil).Do(ctx); err != nil {
					return err
				}
				time.Sleep(100 * time.Millisecond)
				if err := chromedp.Evaluate(`
						(function() {
							const main = document.querySelector('[role="main"]');
							return (main.scrollTop+0.0001)/(main.scrollHeight-main.clientHeight+0.0001);
						})();
					`, &scrollPos).Do(ctx); err != nil {
					return err
				}
				if math.Abs(scrollPos-scrollTarget) < 0.002 {
					break
				}
			}
			log.Trace().Msgf("scroll position: %.4f%%", scrollPos*100)

			var dateNodes []*cdp.Node
			for range 20 {
				if err := chromedp.Nodes(`div.`+dateNodesClassName, &dateNodes, chromedp.ByQueryAll, chromedp.AtLeast(0)).Do(ctx); err != nil {
					return errors.New("failed to get visible date nodes, " + err.Error())
				}
				if len(dateNodes) > 0 {
					break
				}
				chromedp.KeyEvent(kb.PageUp).Do(ctx)
				time.Sleep(500 * time.Millisecond)
			}
			if len(dateNodes) == 0 {
				return errors.New("no date nodes found")
			}

			var closestDateNode *cdp.Node
			var closestDateDiff int
			var knownFirstOccurance bool
			for i, n := range dateNodes {
				if n.NodeName != "DIV" || n.ChildNodeCount == 0 {
					continue
				}
				dateStr := n.Children[0].NodeValue
				var dt time.Time
				// Handle special days like "Yesterday" and "Today"
				today := time.Now()
				today = time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, today.Location())
				for i := range 6 {
					dtTmp := today.AddDate(0, 0, -i)
					dayStr := loc.LongDayNames[dtTmp.Weekday()]
					if i == 0 {
						dayStr = loc.Today
					} else if i == 1 {
						dayStr = loc.Yesterday
					}
					if strings.EqualFold(dayStr, dateStr) {
						dt = dtTmp
						break
					}
				}

				if dt == (time.Time{}) {
					var err error
					dt, err = parseDate(dateStr, "", "")
					if err != nil {
						return fmt.Errorf("could not parse date %s: %w", dateStr, err)
					}
				}
				diff := int(dt.Sub(startDate).Hours())
				log.Trace().Msgf("parsed date element %v with distance %d days", dt, diff/24)
				if closestDateNode == nil || absInt(diff) < absInt(closestDateDiff) {
					closestDateNode = n
					closestDateDiff = diff
					knownFirstOccurance = i > 0 || scrollPos <= 0.001
					if knownFirstOccurance {
						break
					}
				}
			}

			if int(closestDateDiff/24) != 0 && matchedNode != nil {
				foundDateNode = matchedNode
				break
			} else if int(closestDateDiff/24) == 0 && closestDateNode != nil {
				if knownFirstOccurance {
					foundDateNode = closestDateNode
					break
				} else {
					matchedNode = closestDateNode
					bisectBounds[1] = (scrollPos + bisectBounds[1]*3) / 4
				}
			} else if closestDateDiff > 0 {
				bisectBounds[0] = scrollPos
			} else if closestDateDiff < 0 {
				bisectBounds[1] = scrollPos
			}

			time.Sleep(50 * time.Millisecond)
		}

		log.Debug().Msgf("final scroll position: %.4f%%", scrollPos*100)

		time.Sleep(1000 * time.Millisecond)

		if foundDateNode == nil {
			return errors.New("could not find -start date")
		}

		for foundDateNode.Parent != nil {
			foundDateNode = foundDateNode.Parent
			if foundDateNode.AttributeValue("style") != "" {
				s.startNodeParent = foundDateNode
				break
			}
		}
	}

	var location string
	if err := chromedp.Location(&location).Do(ctx); err != nil {
		return err
	}
	log.Debug().Msgf("location: %v", location)

	return nil
}

// setFirstItem looks for the first item, and sets it as s.firstItem.
// We always run it first even for code paths that might not need s.firstItem,
// because we also run it for the side-effect of waiting for the first page load to
// be done, and to be ready to receive scroll key events.
func (s *Session) setFirstItem(ctx context.Context) error {
	// wait for page to be loaded, i.e. that we can make an element active by using
	// the right arrow key.
	var firstItem string
	for {
		log.Trace().Msg("attempting to find first item")
		attributes := make(map[string]string)
		if err := chromedp.Run(ctx,
			chromedp.KeyEvent(kb.ArrowRight),
			chromedp.Sleep(tick),
			chromedp.Attributes(`document.activeElement`, &attributes, chromedp.ByJSPath)); err != nil {
			return err
		}
		if len(attributes) == 0 {
			time.Sleep(tick)
			continue
		}

		photoHref, ok := attributes["href"]
		if ok {
			res, err := imageIdFromUrl(photoHref)
			if err == nil {
				firstItem = res
				break
			}
		}
	}
	log.Debug().Msgf("page loaded, most recent item in the feed is: %s", firstItem)
	return nil
}

// navToEnd scrolls down to the end of the page, i.e. to the oldest items.
func (s *Session) navToEnd(ctx context.Context) error {
	// try jumping to the end of the page. detect we are there and have stopped
	// moving when two consecutive screenshots are identical.
	var previousScr, scr []byte
	for {
		if err := chromedp.Run(ctx,
			chromedp.KeyEvent(kb.PageDown),
			chromedp.KeyEvent(kb.End),
			chromedp.Sleep(tick*time.Duration(5)),
			chromedp.CaptureScreenshot(&scr),
		); err != nil {
			return err
		}
		if previousScr == nil {
			previousScr = scr
			continue
		}
		if bytes.Equal(previousScr, scr) {
			break
		}
		previousScr = scr
	}

	log.Debug().Msg("successfully jumped to the end")

	return nil
}

// doRun runs *runFlag as a command on the given filePath.
func doRun(filePath string) error {
	if *runFlag == "" {
		return nil
	}
	log.Debug().Msgf("running %v on %v", *runFlag, filePath)
	cmd := exec.Command(*runFlag, filePath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// navLeft navigates to the next item to the left
func navWithAction(ctx context.Context, action chromedp.Action) error {
	cl := GetContextData(ctx)
	st := time.Now()
	cl.muNavWaiting.Lock()
	cl.listenEvents = true
	cl.muNavWaiting.Unlock()
	action.Do(ctx)
	cl.muNavWaiting.Lock()
	cl.navWaiting = true
	cl.muNavWaiting.Unlock()
	t := time.NewTimer(time.Minute)
	select {
	case <-cl.navDone:
		if !t.Stop() {
			<-t.C
		}
	case <-t.C:
		return errors.New("timeout waiting for navigation")
	}
	cl.muNavWaiting.Lock()
	cl.navWaiting = false
	cl.muNavWaiting.Unlock()
	log.Debug().Msgf("navigation took %dms", time.Since(st).Milliseconds())
	return nil
}

// requestDownload1 sends the Shift+D event, to start the download of the currently
// viewed item.
func requestDownload1(ctx context.Context, log zerolog.Logger) error {
	muTabActivity.Lock()
	defer muTabActivity.Unlock()

	log.Debug().Msgf("requesting download (method 1)")
	target.ActivateTarget(chromedp.FromContext(ctx).Target.TargetID).Do(ctx)
	if err := pressButton(ctx, "D", input.ModifierShift); err != nil {
		return err
	}
	time.Sleep(250 * time.Millisecond)
	return nil
}

func pressButton(ctx context.Context, key string, modifier input.Modifier) error {
	keyD, ok := kb.Keys[rune(key[0])]
	if !ok {
		return fmt.Errorf("no %s key", key)
	}

	down := input.DispatchKeyEventParams{
		Key:                   keyD.Key,
		Code:                  keyD.Code,
		NativeVirtualKeyCode:  keyD.Native,
		WindowsVirtualKeyCode: keyD.Windows,
		Type:                  input.KeyDown,
		Modifiers:             modifier,
	}
	if runtime.GOOS == "darwin" {
		down.NativeVirtualKeyCode = 0
	}
	up := down
	up.Type = input.KeyUp

	for _, ev := range []*input.DispatchKeyEventParams{&down, &up} {
		log.Trace().Msgf("triggering button press event: %v, %v, %v", ev.Key, ev.Type, ev.Modifiers)

		if err := chromedp.Run(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// requestDownload2 clicks the icons to start the download of the currently
// viewed item.
func requestDownload2(ctx context.Context, log zerolog.Logger, imageId string, original bool, hasOriginal *bool) error {
	log.Debug().Msgf("requesting download (method 2)")
	originalSelector := getAriaLabelSelector(loc.DownloadOriginalLabel)
	var downloadSelector string
	if original {
		downloadSelector = originalSelector
	} else {
		downloadSelector = getAriaLabelSelector(loc.DownloadLabel)
	}

	moreOptionsSelector := getAriaLabelSelector(loc.MoreOptionsLabel)

	foundDownloadButton := false
	i := 0
	for {
		i++
		err := func() error {
			muTabActivity.Lock()
			defer muTabActivity.Unlock()
			// context timeout just in case
			ctxTimeout, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			return chromedp.Run(ctxTimeout,
				target.ActivateTarget(chromedp.FromContext(ctxTimeout).Target.TargetID),
				chromedp.ActionFunc(func(ctx context.Context) error {
					// Wait for more options menu to appear
					if !foundDownloadButton {
						// Open more options dialog
						if err := chromedp.EvaluateAsDevTools(`[...document.querySelectorAll('`+moreOptionsSelector+`')].pop()?.click()`, nil).Do(ctx); err != nil {
							return fmt.Errorf("could not open 'more options' dialog due to %w", err)
						}
					}
					return nil
				}),

				// Press down arrow until the right menu option is selected
				chromedp.ActionFunc(func(ctx context.Context) error {
					var nodes []*cdp.Node
					if err := doActionWithTimeout(ctx, chromedp.Nodes(downloadSelector, &nodes, chromedp.ByQuery), 100*time.Millisecond); err != nil {
						return fmt.Errorf("could not find 'download' button due to %w", err)
					}
					foundDownloadButton = true

					// Check if there is an original version of the image that can also be downloaded
					if hasOriginal != nil && !*hasOriginal {
						var downloadOriginalNodes []*cdp.Node
						if err := chromedp.Nodes(originalSelector, &downloadOriginalNodes, chromedp.AtLeast(0)).Do(ctx); err != nil {
							return fmt.Errorf("checking for original version failed due to %w", err)
						}
						*hasOriginal = len(downloadOriginalNodes) > 0
					}

					n := 0
					for {
						n++
						if n > 30 {
							return errCouldNotPressDownloadButton
						}
						var style []*css.ComputedStyleProperty
						if err := chromedp.ComputedStyle(downloadSelector, &style).Do(ctx); err != nil {
							return err
						}

						for _, s := range style {
							if s.Name == "background-color" && !strings.Contains(s.Value, "0, 0, 0") {
								return nil
							}
						}
						chromedp.KeyEventNode(nodes[0], kb.ArrowDown).Do(ctx)
						time.Sleep(2 * time.Millisecond)
					}
				}),

				// Activate the selected action and wait a bit before continuing
				chromedp.KeyEvent(kb.Enter),
			)
		}()

		if err == nil {
			log.Debug().Msgf("download request succeeded in %d tries", i)
			break
		} else if ctx.Err() != nil {
			return ctx.Err()
		} else if i >= 5 {
			log.Debug().Msgf("trying to request download with method 2 %d times, giving up now", i)
			break
		} else if err == errCouldNotPressDownloadButton || err.Error() == "Could not find node with given id (-32000)" || errors.Is(err, context.DeadlineExceeded) {
			log.Debug().Msgf("trying to request download with method 2 again after error: %v", err)
		} else {
			return fmt.Errorf("encountered error '%s' when requesting download with method 2", err.Error())
		}

		time.Sleep(1 * time.Millisecond)
	}

	return nil
}

// navigateToPhoto navigates to the photo page for the given image ID.
func (s *Session) navigateToPhoto(ctx context.Context, imageId string) error {
	url := s.getPhotoUrl(imageId)
	log.Trace().Msgf("navigating to %v", url)
	resp, err := chromedp.RunResponse(ctx, chromedp.Navigate(url))
	if err != nil {
		return fmt.Errorf("error navigating to %v: %w", url, err)
	}
	if resp.Status == http.StatusOK {
		if err := chromedp.Run(ctx, chromedp.WaitReady("body", chromedp.ByQuery)); err != nil {
			return fmt.Errorf("error waiting for body: %w", err)
		}
	} else {
		return fmt.Errorf("unexpected response: %v", resp.Status)
	}
	return nil
}

// getPhotoData gets the date from the currently viewed item.
// First we open the info panel by clicking on the "i" icon (aria-label="Open info")
// if it is not already open. Then we read the date from the
// aria-label="Date taken: ?????" field.
func (s *Session) getPhotoData(ctx context.Context, log zerolog.Logger, imageId string) (PhotoData, error) {
	var filename string
	var dateStr string
	var timeStr string
	var tzStr string
	timeout1 := time.NewTimer(3 * time.Second)
	timeout2 := time.NewTimer(90 * time.Second)
	log.Debug().Msg("extracting photo date text and original file name")

	var n = 0
	muTabActivity.Lock()
	defer muTabActivity.Unlock()
	for {
		n++
		if err := func() error {
			target.ActivateTarget(chromedp.FromContext(ctx).Target.TargetID).Do(ctx)

			select {
			case <-timeout1.C:
				log.Debug().Msgf("getPhotoData: reloading page to force photo info to load (n=%d)", n)
				if err := s.navigateToPhoto(ctx, imageId); err != nil {
					log.Error().Msgf("getPhotoData: %s", err.Error())
					timeout1 = time.NewTimer(2 * time.Second)
				} else {
					timeout1 = time.NewTimer(10 * time.Second)
				}
			default:
			}

			wait := 1 * time.Millisecond
			if n > 1 {
				wait = time.Duration(300+n*50) * time.Millisecond
			}

			if err := chromedp.Run(ctx,
				chromedp.Sleep(wait),
				chromedp.Evaluate(getContentOfFirstVisibleNodeScript(getAriaLabelSelector(loc.FileNameLabel), imageId), &filename),
				chromedp.Evaluate(getContentOfFirstVisibleNodeScript(getAriaLabelSelector(loc.DateLabel), imageId), &dateStr),
				chromedp.Evaluate(getContentOfFirstVisibleNodeScript(getAriaLabelSelector(loc.DateLabel)+" + div "+getAriaLabelSelector(loc.TimeLabel), imageId), &timeStr),
				chromedp.Evaluate(getContentOfFirstVisibleNodeScript(getAriaLabelSelector(loc.DateLabel)+" + div "+getAriaLabelSelector(loc.TzLabel), imageId), &tzStr),
			); err != nil {
				return fmt.Errorf("could not extract photo data due to %w", err)
			}

			if len(dateStr) == 0 && n > 2 {
				log.Trace().Msgf("incomplete data - Date: %v, Time: %v, Timezone: %v, File name: %v", dateStr, timeStr, tzStr, filename)

				// Click on info button
				log.Debug().Msgf("date not visible (n=%d), clicking on i button", n)
				if err := chromedp.Run(ctx, chromedp.EvaluateAsDevTools(`document.querySelector('[data-p*="`+imageId+`"] `+getAriaLabelSelector(loc.OpenInfoMatch)+` button')?.click()`, nil)); err != nil {
					return fmt.Errorf("could not click on info button due to %w", err)
				}
			}

			return nil
		}(); err != nil {
			return PhotoData{}, err
		} else if len(filename) > 0 && len(dateStr) > 0 && len(timeStr) > 0 {
			break
		}

		select {
		case <-timeout2.C:
			return PhotoData{}, fmt.Errorf("timeout waiting for photo info")
		case <-time.After(10 * time.Millisecond):
		}
	}

	log.Trace().Msgf("parsing date: %v and time: %v", dateStr, timeStr)
	log.Trace().Msgf("parsing filename: %v", filename)
	dt, err := parseDate(dateStr, timeStr, tzStr)
	if err != nil {
		return PhotoData{}, fmt.Errorf("parsing date, %w", err)
	}

	log.Debug().Msgf("found date: %v and original filename: %v", dt, filename)

	return PhotoData{dt, filename}, nil
}

var startDownloadLock sync.Mutex = sync.Mutex{}

// startDownload starts the download of the currently viewed item. It returns
// with an error if the download does not start within a minute.
func (s *Session) startDownload(ctx context.Context, log zerolog.Logger, imageId string, isOriginal bool, hasOriginal *bool) (newDownload NewDownload, progressChan chan bool, err error) {
	log.Trace().Msgf("entering startDownload()")

	startDownloadLock.Lock()
	defer startDownloadLock.Unlock()
	timeoutTimer := time.NewTimer(90 * time.Second)
	refreshTimer := time.NewTimer(90 * time.Second)
	requestTimer := time.NewTimer(0 * time.Second)

	if len(s.nextDownload) != 0 {
		return NewDownload{}, nil, errors.New("unexpected: nextDownload channel is not empty")
	}

	downloadStartedChan := make(chan NewDownload, 1)
	downloadProgressChan := make(chan bool, 1)
	s.nextDownload <- DownloadChannels{downloadStartedChan, downloadProgressChan}

	defer func() {
		if err != nil {
			select {
			case <-s.nextDownload:
			default:
			}
		}
	}()

	for {
		select {
		case <-requestTimer.C:
			if err := requestDownload2(ctx, log, imageId, isOriginal, hasOriginal); err != nil {
				if isOriginal || err != errCouldNotPressDownloadButton {
					return NewDownload{}, nil, err
				} else if !isOriginal {
					requestDownload1(ctx, log)
				}
				refreshTimer = time.NewTimer(100 * time.Millisecond)
			} else {
				refreshTimer = time.NewTimer(5 * time.Second)
			}
		case <-refreshTimer.C:
			log.Debug().Msgf("reloading page because download failed to start")
			if err := s.navigateToPhoto(ctx, imageId); err != nil {
				log.Error().Msgf("startDownload: %s", err.Error())
				refreshTimer = time.NewTimer(1 * time.Second)
			} else {
				requestTimer = time.NewTimer(100 * time.Millisecond)
			}
		case <-timeoutTimer.C:
			return NewDownload{}, nil, fmt.Errorf("timeout waiting for download to start for %v", imageId)
		case newDownload := <-downloadStartedChan:
			log.Trace().Msgf("downloadStartedChan: %v", newDownload)
			return newDownload, downloadProgressChan, nil
		default:
			time.Sleep(50 * time.Millisecond)
		}

		if timedLogReady("downloadStatus"+imageId, 15*time.Second) {
			log.Debug().Msgf("checking download start status")
		}

		// Checking for gphotos warning that this video can't be downloaded (no known solution)
		// This check only works for requestDownload2 method (not requestDownload1)
		if err := s.checkForStillProcessing(ctx); err != nil {
			return NewDownload{}, nil, err
		}
	}
}

func (*Session) checkForStillProcessing(ctx context.Context) error {
	log.Trace().Msgf("checking for still processing dialog")
	var nodes []*cdp.Node
	if err := chromedp.Nodes(getAriaLabelSelector(loc.VideoStillProcessingDialogLabel)+` button`, &nodes, chromedp.ByQuery, chromedp.AtLeast(0)).Do(ctx); err != nil {
		return err
	}
	isStillProcessing := len(nodes) > 0
	if isStillProcessing {
		log.Debug().Msgf("found still processing dialog, need to press button to remove")
		// Click the button to close the warning, otherwise it will block navigating to the next photo
		cl := GetContextData(ctx)
		cl.muKbEvents.Lock()
		err := chromedp.MouseClickNode(nodes[0]).Do(ctx)
		cl.muKbEvents.Unlock()
		if err != nil {
			return err
		}
	} else {
		// This check only works for requestDownload1 method (not requestDownload2)
		if err := chromedp.Evaluate("document.body?.textContent.indexOf('"+loc.VideoStillProcessingStatusText+"') >= 0", &isStillProcessing).Do(ctx); err != nil {
			return err
		}
		if isStillProcessing {
			log.Debug().Msg("found still processing status at bottom of screen, waiting for it to disappear before continuing")
			time.Sleep(5 * time.Second) // Wait for error message to disappear before continuing, otherwise we will also skip next files
		}
		if !isStillProcessing {
			// Sometimes Google returns a different error, check for that too
			if err := chromedp.Evaluate("document.body?.textContent.indexOf('"+loc.NoWebpageFoundText+"') >= 0", &isStillProcessing).Do(ctx); err != nil {
				return err
			}
			if isStillProcessing {
				log.Info().Msgf("this is an error page, we will navigate back to the photo to be able to continue")
				if err := navWithAction(ctx, chromedp.NavigateBack()); err != nil {
					return err
				}
				time.Sleep(400 * time.Millisecond)
			}
		}
	}
	if isStillProcessing {
		log.Warn().Msg("received 'Video is still processing' error")
		return errStillProcessing
	}
	return nil
}

func imageIdFromUrl(location string) (string, error) {
	// Parse the URL
	u, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("invalid URL %v: %w", location, err)
	}

	// Split the path into segments
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")

	// Look for "photo" segment and ensure there's a following segment
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "photo" {
			return parts[i+1], nil
		}
	}
	return "", fmt.Errorf("could not find /photo/{imageId} pattern in URL: %v", location)
}

// makeOutDir creates a directory in s.downloadDir named of the item ID
func (s *Session) makeOutDir(imageId string) (string, error) {
	newDir := filepath.Join(s.downloadDir, imageId)
	if err := os.MkdirAll(newDir, 0700); err != nil {
		return "", err
	}
	return newDir, nil
}

// processDownload creates a directory in s.downloadDir with name = imageId and moves the downloaded files into that directory
func (s *Session) processDownload(log zerolog.Logger, downloadInfo NewDownload, downloadProgressChan chan bool, isOriginal, hasOriginal bool, imageId string, photoDataChan chan PhotoData) error {
	log = log.With().Str("GUID", downloadInfo.GUID).Logger()

	log.Trace().Msgf("entering processDownload")
	downloadTimeout := time.NewTimer(time.Minute)
progressLoop:
	for {
		select {
		case p := <-downloadProgressChan:
			if p {
				// download done
				log.Trace().Msgf("processDownload: received download completed message")
				break progressLoop
			} else {
				// still downloading
				log.Trace().Msgf("processDownload: received download still in progress message")
				downloadTimeout.Reset(time.Minute)
			}
		case <-downloadTimeout.C:
			return fmt.Errorf("timeout waiting for download to complete for %v", imageId)
		}
	}

	data := <-photoDataChan

	outDir, err := s.makeOutDir(imageId)
	if err != nil {
		return err
	}

	var filePaths []string
	if strings.HasSuffix(downloadInfo.suggestedFilename, ".zip") {
		var err error
		filePaths, err = s.handleZip(log, filepath.Join(s.downloadDirTmp, downloadInfo.GUID), outDir)
		if err != nil {
			return err
		}
		foundExpectedFile := false
		for _, f := range filePaths {
			if strings.Contains(strings.ToLower(f), strings.ToLower(data.filename)) {
				foundExpectedFile = true
				break
			}
		}
		if !foundExpectedFile {
			log.Warn().Msgf("expected file %v not found in downloaded zip", data.filename)
		}
	} else {
		var filename string
		if downloadInfo.suggestedFilename != "download" && downloadInfo.suggestedFilename != "" {
			filename = downloadInfo.suggestedFilename
		} else {
			filename = data.filename
		}

		if isOriginal || !hasOriginal {
			if !strings.Contains(filename, data.filename) {
				log.Warn().Msgf("expected file %v but downloaded file %v", data.filename, filename)
			}
		}

		if isOriginal {
			// to ensure the filename is not the same as the other download, change e.g. image_1.jpg to image_1_original.jpg
			ext := filepath.Ext(filename)
			filename = strings.TrimSuffix(filename, ext) + originalSuffix + ext
		}

		newFile := filepath.Join(outDir, filename)
		log.Debug().Msgf("moving %v to %v", downloadInfo.GUID, newFile)
		if err := os.Rename(filepath.Join(s.downloadDirTmp, downloadInfo.GUID), newFile); err != nil {
			return err
		}
		filePaths = []string{newFile}
	}

	if err := doFileDateUpdate(data.date, filePaths); err != nil {
		return err
	}

	for _, f := range filePaths {
		if err := doRun(f); err != nil {
			return err
		}
	}

	for _, f := range filePaths {
		log.Info().Msgf("downloaded %v with date %v", filepath.Base(f), data.date.Format(time.DateOnly))
	}

	return nil
}

// downloadAndProcessItem starts a download then sends it to processDownload for processing
func (s *Session) downloadAndProcessItem(ctx context.Context, log zerolog.Logger, imageId string) error {
	log.Trace().Msgf("entering downloadAndProcessItem")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	photoDataChan := make(chan PhotoData, 2)
	hasOriginalChan := make(chan bool, 1)
	errChan := make(chan error)
	jobsRemaining := 3

	go func() {
		log.Trace().Msgf("getting photo data")
		data, err := s.getPhotoData(ctx, log, imageId)
		if err != nil {
			errChan <- err
		} else if fromDate != (time.Time{}) && data.date.Before(fromDate) {
			errChan <- errPhotoTakenBeforeFromDate
		} else if toDate != (time.Time{}) && data.date.After(toDate) {
			errChan <- errPhotoTakenAfterToDate
		} else {
			errChan <- nil

			// we need two of these in case we are downloading an original
			photoDataChan <- data
			photoDataChan <- data
		}
	}()

	go func() {
		hasOriginal := false
		downloadInfo, downloadProgressChan, err := s.startDownload(ctx, log, imageId, false, &hasOriginal)
		if err != nil {
			log.Trace().Msgf("download failed: %v", err)
			errChan <- err
			hasOriginalChan <- false
		} else {
			hasOriginalChan <- hasOriginal
			errChan <- s.processDownload(log, downloadInfo, downloadProgressChan, false, hasOriginal, imageId, photoDataChan)
		}
	}()

	go func() {
		if <-hasOriginalChan {
			log := log.With().Bool("isOriginal", true).Logger()
			downloadInfo, downloadProgressChan, err := s.startDownload(ctx, log, imageId, true, nil)
			if err != nil {
				log.Trace().Msgf("download of original failed: %v", err)
				errChan <- err
			} else {
				errChan <- s.processDownload(log, downloadInfo, downloadProgressChan, true, true, imageId, photoDataChan)
			}
		} else {
			errChan <- nil
		}
	}()

	go func() {
		deadline := time.NewTimer(30 * time.Minute)
		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				close(errChan)
				return
			case <-time.After(60 * time.Second):
				log.Trace().Msgf("downloadAndProcessItem: waiting for %d jobs to finish", jobsRemaining)
			case <-deadline.C:
				errChan <- fmt.Errorf("downloadAndProcessItem: timeout waiting for %d jobs to finish, canceling", jobsRemaining)
				close(errChan)
				return
			}
		}
	}()

	for err := range errChan {
		jobsRemaining--
		if err != nil {
			log.Trace().Msgf("downloadAndProcessItem: job result %s, %d jobs remaining", err.Error(), jobsRemaining)
			if !errors.Is(err, context.Canceled) {
				captureScreenshot(ctx, filepath.Join(s.downloadDir, "error"))
			}

			// Error downloading original or generated image, remove files already downloaded
			if err := os.RemoveAll(filepath.Join(s.downloadDir, imageId)); err != nil {
				log.Err(err).Msgf("error removing files already downloaded: %v", err)
			}

			return err
		} else {
			log.Trace().Msgf("downloadAndProcessItem: job result done, %d jobs remaining", jobsRemaining)
		}
		if jobsRemaining == 0 {
			return nil
		}
	}

	// If we get here, the channel was closed but jobsRemaining > 0
	return ctx.Err()
}

// handleZip handles the case where the currently item is a zip file. It extracts
// each file in the zip file to the same folder, and then deletes the zip file.
func (s *Session) handleZip(log zerolog.Logger, zipfile, outFolder string) ([]string, error) {
	st := time.Now()
	log.Debug().Msgf("unzipping %v to %v", zipfile, outFolder)
	// unzip the file
	files, err := zip.Unzip(zipfile, outFolder)
	if err != nil {
		return []string{""}, err
	}

	// delete the zip file
	if err := os.Remove(zipfile); err != nil {
		return []string{""}, err
	}

	log.Debug().Msgf("unzipped %v in %v", zipfile, time.Since(st))
	return files, nil
}

var muTabActivity sync.Mutex = sync.Mutex{}

type ContextLocks = struct {
	muNavWaiting             sync.RWMutex
	muKbEvents               sync.Mutex
	listenEvents, navWaiting bool
	navDone                  chan bool
}
type ContextLocksPointer = *ContextLocks

// contextKey is a custom type for context keys to avoid collisions
type contextKey struct {
	name string
}

// Define the key for context locks
var contextLocksKey = &contextKey{name: "contextLocks"}

func GetContextData(ctx context.Context) ContextLocksPointer {
	return ctx.Value(contextLocksKey).(ContextLocksPointer)
}

func SetContextData(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextLocksKey, &ContextLocks{
		muNavWaiting: sync.RWMutex{},
		muKbEvents:   sync.Mutex{},
		listenEvents: false,
		navWaiting:   false,
		navDone:      make(chan bool, 1),
	})
}

func listenNavEvents(ctx context.Context) {
	cl := GetContextData(ctx)
	chromedp.ListenTarget(ctx, func(ev interface{}) {
		cl.muNavWaiting.RLock()
		listen := cl.listenEvents
		cl.muNavWaiting.RUnlock()
		if !listen {
			return
		}
		switch ev.(type) {
		case *page.EventNavigatedWithinDocument:
			go func() {
				for {
					cl.muNavWaiting.RLock()
					waiting := cl.navWaiting
					cl.muNavWaiting.RUnlock()
					if waiting {
						cl.navDone <- true
						break
					}
					time.Sleep(25 * time.Millisecond)
				}
			}()
		}
	})
}

func getSliderPosAndText(ctx context.Context) (float64, string, error) {
	var sliderPos float64
	var sliderText string

	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`(document.querySelector('div[role="slider"][aria-valuemax="1"][aria-valuetext]')?.ariaValueText || '')`, &sliderText),
	); err != nil {
		return 0, "", fmt.Errorf("couldn't find slider node, %w", err)
	}

	var mainSel string
	if len(*albumIdFlag) > 1 {
		mainSel = `c-wiz c-wiz c-wiz`
	} else {
		mainSel = `[role="main"]`
	}

	if err := chromedp.Run(ctx, chromedp.Evaluate(fmt.Sprintf(`
	(function() {
		var nodes = [...document.querySelectorAll('%s')].filter(x => x.querySelector('a[href*="/photo/"]') && getComputedStyle(x).visibility != 'hidden');
		return nodes.length > 0 ? (nodes[0].scrollTop+0.000001)/(nodes[0].scrollHeight-nodes[0].clientHeight+0.000001) : 0.0;
	})()`, mainSel), &sliderPos)); err != nil {
		return 0, "", fmt.Errorf("couldn't calculate scroll position, %w", err)
	}

	return sliderPos, sliderText, nil
}

// Resync the library/album of photos
// Use [...document.querySelectorAll('a[href^=".{relPath}photo/"]')] to find all visible photos
// Check that each one is already downloaded. Optionally check/update date from the element
// attr, e.g. aria-label="Photo - Landscape - Feb 12, 2025, 6:34:39 PM"
// Then do .pop().focus() on the last a element found to scroll to it and make more photos visible
// Then repeat until we get to the end
// If any photos are missing we can asynchronously create a new chromedp context, then in that
// context navigate to that photo and call downloadAndProcessItem
func (s *Session) resync(ctx context.Context) error {
	listenNavEvents(ctx)

	lastNode := &cdp.Node{}
	var nodes []*cdp.Node
	i := 0             // next node to process in nodes array
	n := 0             // number of nodes processed in all
	newItemsCount := 0 // number of photos downloaded
	retries := 0       // number of subsequent failed attempts to find new items to download
	sliderPos := 0.0
	estimatedRemaining := 1000
	photoNodeSelector := getPhotoNodeSelector(s.photoRelPath)

	log.Trace().Msgf("finding start node")
	opts := []chromedp.QueryOption{chromedp.ByQuery, chromedp.AtLeast(0)}

	if err := chromedp.Nodes(photoNodeSelector, &nodes, opts...).Do(ctx); err != nil {
		return fmt.Errorf("error finding photo nodes, %w", err)
	}
	if len(nodes) == 0 {
		log.Info().Msg("no photos to resync")
		return nil
	}

	jobChan := make(chan Job, *workersFlag)
	resultChan := make(chan string, *workersFlag)
	errChan := make(chan error, *workersFlag)
	runningWorkers := *workersFlag
	for i := range *workersFlag {
		s.downloadWorker(i+1, jobChan, resultChan, errChan)
	}

syncAllLoop:
	for {
		if retries%5 == 0 {
			target.ActivateTarget(chromedp.FromContext(ctx).Target.TargetID).Do(ctx)
			if retries != 0 {
				log.Debug().Msgf("we seem to be stuck, manually scrolling might help")
				if err := doActionWithTimeout(ctx, chromedp.KeyEvent(kb.ArrowDown), 2000*time.Millisecond); err != nil {
					log.Err(err).Msgf("error scrolling page down manually, %v", err)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}

		if retries > 0 && retries%25 == 0 {
			// loading slow, let's give it some extra time
			time.Sleep(1 * time.Second)
		}

		// New new nodes found, does it look like we are done?
		if retries > 5000 || (retries > 100 && estimatedRemaining < 50) {
			break
		}

		var err error
		sliderPos, _, err = getSliderPosAndText(ctx)
		if err != nil {
			return fmt.Errorf("error getting slider position and text, %w", err)
		}
		log.Trace().Msgf("slider position: %.2f%%", sliderPos*100)

		if err := s.processJobs(&runningWorkers, resultChan, errChan, false); err != nil {
			if err == errPhotoTakenBeforeFromDate {
				log.Info().Msg("found photo taken before -from date, stopping sync here")
				break syncAllLoop
			}
			return err
		}

		if n < 5 || sliderPos < 0.001 {
			estimatedRemaining = 50
		} else {
			estimatedRemaining = int(math.Floor((1/sliderPos - 1) * float64(n+30)))
		}

		if timedLogReady("resyncLoop", 60*time.Second) {
			log.Info().Msgf("so far: synced %d items, downloaded %d, %d in queue, progress: %.2f%%, estimated remaining: %d", n, len(s.downloadedItems), newItemsCount-len(s.downloadedItems), sliderPos*100, estimatedRemaining)
		}

		if n != 0 && i >= len(nodes) {
			if retries == 0 {
				// start by scrolling to the next batch by focusing the last processed node
				log.Trace().Msgf("scrolling to last processed node: %v", lastNode.NodeID)
				if err := doActionWithTimeout(ctx, dom.Focus().WithNodeID(lastNode.NodeID), 1000*time.Millisecond); err != nil {
					log.Debug().Msgf("error scrolling to next batch of items: %v", err)
				}
			}

			if err := chromedp.Nodes(photoNodeSelector, &nodes, chromedp.ByQueryAll, chromedp.AtLeast(0)).Do(ctx); err != nil {
				return fmt.Errorf("error finding photo nodes, %w", err)
			}
			log.Trace().Msgf("found %d items, checking if any are new", len(nodes))

			// remove already processed nodes
			foundNodes := len(nodes)
			for i, node := range nodes {
				if node == lastNode {
					nodes = nodes[i+1:]
					break
				}
			}
			if len(nodes) == 0 {
				retries++
				continue
			}
			log.Debug().Msgf("%d nodes on page, processing %d that haven't been processed yet", foundNodes, len(nodes))
			if foundNodes == len(nodes) {
				log.Warn().Msg("only new nodes found, expected an overlap")
			}

			retries = 0
			i = 0
		}

		imageIds := []string{}

		for i < len(nodes) && (*batchSizeFlag <= 0 || len(imageIds) < *batchSizeFlag) {
			lastNode = nodes[i]
			i++
			n++

			imageId, err := imageIdFromUrl(lastNode.AttributeValue("href"))
			if err != nil {
				return fmt.Errorf("error getting image id from url, %w", err)
			}
			log := log.With().Str("itemId", imageId).Logger()

			shouldDownload, err := s.isNewItem(log, imageId)
			if err != nil {
				return err
			} else if !shouldDownload {
				if len(imageIds) > 0 {
					break
				} else {
					continue
				}
			}

			ariaLabel, err := s.getAriaLabel(ctx, log, lastNode)
			if err != nil {
				return err
			}

			if strings.Contains(ariaLabel, "Highlight video") {
				log.Info().Msgf("skipping highlight video (%s)", ariaLabel)
				if len(imageIds) > 0 {
					break
				} else {
					continue
				}
			}

			log.Info().Msgf(`item "%s" is missing. Downloading it.`, ariaLabel)
			imageIds = append(imageIds, imageId)
		}

		if len(imageIds) > 0 {
			log.Debug().Msgf("adding %d photos to queue", len(imageIds))
			job := Job{imageIds, make(chan error, 1)}

			log.Trace().Msgf("queuing job with itemIds: %s", strings.Join(job.imageIds, ", "))
		sendJobLoop:
			for {
				select {
				case jobChan <- job:
					break sendJobLoop
				default:
					time.Sleep(100 * time.Millisecond)
					if err := s.processJobs(&runningWorkers, resultChan, errChan, false); err != nil {
						if err == errPhotoTakenBeforeFromDate {
							log.Info().Msg("found photo taken before -from date, stopping sync here")
							break syncAllLoop
						}
						return err
					}
				}
			}
			log.Trace().Msgf("queued job with itemIds: %s", strings.Join(job.imageIds, ", "))
		}

		newItemsCount += len(imageIds)
	}
	close(jobChan)
	log.Info().Msgf("in total: synced %v items, downloaded %v, progress: %.2f%%", n, len(s.downloadedItems), sliderPos*100)

	if err := s.checkForRemovedFiles(ctx); err != nil {
		return err
	}
	return s.processJobs(&runningWorkers, resultChan, errChan, true)
}

func (s *Session) getAriaLabel(ctx context.Context, log zerolog.Logger, node *cdp.Node) (string, error) {
	ariaLabel := ""
	if jsNode, err := dom.ResolveNode().WithNodeID(node.NodeID).Do(ctx); err != nil {
		log.Err(err).Msgf("error resolving object id of node, %s", err.Error())
	} else {
		if err := chromedp.Run(ctx, chromedp.CallFunctionOn(`function() { return this.ariaLabel }`, &ariaLabel,
			func(p *cdpruntime.CallFunctionOnParams) *cdpruntime.CallFunctionOnParams {
				return p.WithObjectID(jsNode.ObjectID)
			},
		)); err != nil {
			log.Err(err).Msgf("error reading node ariaLabel, %s", err.Error())
		}
	}
	return ariaLabel, nil
}

func (s *Session) isNewItem(log zerolog.Logger, imageId string) (bool, error) {
	if _, exists := s.foundItems[imageId]; exists {
		log.Warn().Msgf("looks like we've already seen this item, this shouldn't happen")
		return false, nil
	}

	s.foundItems[imageId] = struct{}{}
	log.Trace().Msgf("processing %v", imageId)

	hasFiles, err := s.dirHasFiles(imageId)
	if err != nil {
		return false, err
	} else if hasFiles {
		log.Trace().Msgf("skipping item, already downloaded")
		return false, nil
	}

	return true, nil
}

func (s *Session) getPhotoUrl(imageId string) string {
	return gphotosUrl + s.photoRelPath + "/photo/" + imageId
}

func (s *Session) downloadWorker(workerId int, jobs <-chan Job, resultChan chan<- string, errChan chan<- error) {
	ctx, cancel := chromedp.NewContext(s.parentContext)
	log := log.With().Int("workerId", workerId).Logger()
	go func() {
		defer cancel()
		for job := range jobs {
			log.Debug().Msgf("worker received batch of %d items", len(job.imageIds))
			log.Trace().Msgf("starting job with itemIds: %s", strings.Join(job.imageIds, ", "))
			for i, imageId := range job.imageIds {
				log := log.With().Str("itemId", imageId).Logger()
				log.Trace().Msgf("processing batch item %d", i)
				expectedLocation := s.getPhotoUrl(imageId)

				ctx = SetContextData(ctx)
				listenNavEvents(ctx)

				atExpectedUrl := false
				if i > 0 {
					// pressing right arrow to navigate to the next item (batch jobs should be sequential)
					log.Trace().Msgf("navigating to %v by right arrow press", expectedLocation)
					var location string
					muTabActivity.Lock()
					err := chromedp.Run(ctx,
						target.ActivateTarget(chromedp.FromContext(ctx).Target.TargetID),
						chromedp.ActionFunc(navRight),
						chromedp.Sleep(50*time.Millisecond),
						chromedp.Location(&location),
						chromedp.ActionFunc(
							func(ctx context.Context) error {
								if location != expectedLocation {
									log.Error().Msgf("after nav to right, expected location %s, got %s", expectedLocation, location)
								} else {
									atExpectedUrl = true
								}
								return nil
							},
						),
					)
					muTabActivity.Unlock()
					if err != nil {
						log.Error().Msgf("error navigating to next batch item: %s", err.Error())
					}
				}

				if !atExpectedUrl {
					log.Trace().Msgf("navigating to %v", expectedLocation)
					resp, err := chromedp.RunResponse(ctx, chromedp.Navigate(expectedLocation))
					if err != nil {
						errChan <- fmt.Errorf("error navigating to %v: %w", expectedLocation, err)
						return
					}
					if resp.Status == http.StatusOK {
						if err := chromedp.Run(ctx, chromedp.WaitReady("body", chromedp.ByQuery)); err != nil {
							errChan <- fmt.Errorf("error waiting for body: %w", err)
							return
						}
					} else {
						errChan <- fmt.Errorf("unexpected response: %v", resp.Status)
						return
					}
				}

				time.Sleep(10 * time.Millisecond)

				err := chromedp.Run(ctx, chromedp.ActionFunc(
					func(ctx context.Context) error {
						return s.downloadAndProcessItem(ctx, log, imageId)
					},
				))
				if err == errStillProcessing {
					// Old highlight videos are no longer available
					log.Info().Msg("skipping generated highlight video that Google seems to have lost")
				} else if err == errPhotoTakenAfterToDate {
					log.Warn().Msg("skipping photo taken after -to date. If you see many of these messages, something has gone wrong.")
					// } else if err == errPhotoTakenBeforeFromDate {
					// 	log.Info().Msgf("skipping photo taken before -from date. If you see more than %d of these messages, something has gone wrong.", *workersFlag)
				} else if err != nil {
					log.Trace().Msgf("downloadWorker: encountered error while processing batch item %d: %s", i, err.Error())
					errChan <- err
					return
				} else {
					resultChan <- imageId
				}
			}
			log.Info().Msgf("worker finished processing batch of %d items", len(job.imageIds))
		}
		errChan <- nil
	}()
}

// navRight navigates to the next item to the right
func navRight(ctx context.Context) error {
	log.Debug().Msg("Navigating right")
	return navWithAction(ctx, chromedp.KeyEvent(kb.ArrowRight))
}

// Check if there are folders in the download dir that were not seen in gphotos
func (s *Session) checkForRemovedFiles(ctx context.Context) error {
	if *removedFlag {
		log.Info().Msg("checking for removed files")
		deleted := []string{}
		for itemId := range s.existingItems {
			if itemId != "tmp" {
				// Check if the folder name is in the map of photo IDs
				if _, exists := s.foundItems[itemId]; !exists {
					deleted = append(deleted, itemId)
				}
			}
		}
		if len(deleted) > 0 {
			log.Info().Msgf("folders found for %d local photos that were not found in this sync. Checking google photos to confirm they are not there", len(deleted))
		}
		i := 0
		for i < len(deleted) {
			imageId := deleted[i]
			var resp int
			if err := chromedp.Run(ctx,
				chromedp.Evaluate(`new Promise((res) => fetch('`+s.getPhotoUrl(imageId)+`').then(x => res(x.status)));`, &resp,
					func(p *cdpruntime.EvaluateParams) *cdpruntime.EvaluateParams {
						return p.WithAwaitPromise(true)
					}),
			); err != nil {
				log.Err(err).Msgf("error checking for removed file %s: %s, will not continue checking for removed files", imageId, err.Error())
				return nil
			}
			if resp == http.StatusOK {
				log.Debug().Msgf("photo %s was not in original sync, but is still present on google photos, it might be in the trash", imageId)
				deleted = slices.Delete(deleted, i, i+1)
				continue
			} else if resp == http.StatusNotFound {
				log.Trace().Msgf("photo %s not found on google photos, but is in local folder, it was probably deleted or removed from album", imageId)
			} else {
				return fmt.Errorf("unexpected response for %s: %v", imageId, resp)
			}
			i++
		}
		if len(deleted) > 0 {
			log.Info().Msgf("folders found for %d local photos that don't exist on google photos (in album if using -album), list saved to .removed", len(deleted))
			if err := os.WriteFile(path.Join(s.downloadDir, ".removed"), []byte(strings.Join(deleted, "\n")), 0644); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Session) processJobs(runningWorkers *int, resultChan chan string, errChan chan error, waitForAll bool) error {
	for range 50000 {
	emptyChannelsLoop:
		for {
			select {
			case res := <-resultChan:
				if _, exists := s.foundItems[res]; !exists {
					s.foundItems[res] = struct{}{}
				}
				if _, exists := s.downloadedItems[res]; exists {
					log.Warn().Msgf("we've downloaded the same item twice, this shouldn't happen")
				} else {
					s.downloadedItems[res] = struct{}{}
				}
			case err := <-errChan:
				*runningWorkers--
				if err == errPhotoTakenBeforeFromDate && waitForAll {
					log.Info().Msgf("skipping photo taken before -from date. If you see more than %d of these messages, something has gone wrong.", *workersFlag)
				} else if err != nil {
					log.Trace().Msgf("processJobs: received error from worker: %s", err.Error())
					return err
				}
			case err := <-s.globalErrChan:
				return err
			default:
				break emptyChannelsLoop
			}
		}

		if *runningWorkers == 0 || !waitForAll {
			return nil
		}

		time.Sleep(50 * time.Millisecond)
	}

	return errors.New("waited too long for jobs to exit, exiting")
}

// doFileDateUpdate updates the file date of the downloaded files to the photo date
func doFileDateUpdate(date time.Time, filePaths []string) error {
	log.Debug().Msgf("setting file date for %v", filePaths)

	for _, f := range filePaths {
		if err := setFileDate(f, date); err != nil {
			return err
		}
	}

	return nil
}

func doActionWithTimeout(ctx context.Context, action chromedp.Action, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := action.Do(ctx); err != nil {
		return err
	}
	return nil
}

// Sets modified date of file to given date
func setFileDate(filepath string, date time.Time) error {
	if err := os.Chtimes(filepath, date, date); err != nil {
		return err
	}
	return nil
}

func startDownloadListener(ctx context.Context, newDownloadChan chan DownloadChannels, globalErrChan chan error) {
	currentDownloads := make(map[string]DownloadChannels)
	chromedp.ListenBrowser(ctx, func(v interface{}) {
		if ev, ok := v.(*browser.EventDownloadWillBegin); ok {
			log.Debug().Str("GUID", ev.GUID).Msgf("download of %s started", ev.SuggestedFilename)
			if ev.SuggestedFilename == "downloads.html" {
				return
			}
			select {
			case currentDownloads[ev.GUID] = <-newDownloadChan:
			default:
				globalErrChan <- fmt.Errorf("unexpected download of %s", ev.SuggestedFilename)
			}
			go func() {
				select {
				case currentDownloads[ev.GUID].downloadStarted <- NewDownload{ev.GUID, ev.SuggestedFilename}:
				default:
					// It looks like these events sometimes get duplicated, let's just ignore it
				}
			}()
		}
	})

	chromedp.ListenBrowser(ctx, func(v interface{}) {
		if ev, ok := v.(*browser.EventDownloadProgress); ok {
			if ev.State == browser.DownloadProgressStateInProgress {
				select {
				case currentDownloads[ev.GUID].downloadProgress <- false:
				default:
				}
			}
			if ev.State == browser.DownloadProgressStateCompleted {
				log.Trace().Str("GUID", ev.GUID).Msgf("received download completed event")
				progressChan := currentDownloads[ev.GUID].downloadProgress
				delete(currentDownloads, ev.GUID)
				go func() {
					time.Sleep(1 * time.Millisecond)
					progressChan <- true
				}()
			}
		}
	})
}

func getContentOfFirstVisibleNodeScript(sel string, imageId string) string {
	return fmt.Sprintf(`[...document.querySelectorAll('[data-p*="%s"] %s')].filter(x => x.checkVisibility()).map(x => x.textContent)[0] || ''`, imageId, sel)
}

func (s *Session) dirHasFiles(imageId string) (bool, error) {
	if _, ok := s.existingItems[imageId]; !ok {
		return false, nil
	}
	entries, err := os.ReadDir(filepath.Join(s.downloadDir, imageId))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, v := range entries {
		if !v.IsDir() {
			f, err := os.Stat(filepath.Join(s.downloadDir, imageId, v.Name()))
			if err != nil {
				return false, err
			}
			if f.Size() > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

func getPhotoNodeSelector(nodeHrefPrefix string) string {
	if strings.HasPrefix(nodeHrefPrefix, "/u/") {
		nodeHrefPrefix = nodeHrefPrefix[3:]
		a := strings.Index(nodeHrefPrefix, "/")
		if a == -1 {
			nodeHrefPrefix = ""
		} else {
			nodeHrefPrefix = nodeHrefPrefix[a:]
		}
	}
	return fmt.Sprintf(`a[href^=".%s/photo/"]`, nodeHrefPrefix)
}

// compiled year regex
var yearRegex = regexp.MustCompile(`\d{4}`)
var dayRegex = regexp.MustCompile(`\d{1,2}`)
var timeRegex = regexp.MustCompile(`(\d{1,2}):(\d\d)(?::\d\d)?\s?([aApP][Mm])?`)
var timeZoneRegex = regexp.MustCompile(`GMT([-+])?(\d{1,2})(?::(\d\d))?`)

func parseDate(dateStr, timeStr, tzStr string) (time.Time, error) {
	var year, month, day, hour, minute int
	yearStr := yearRegex.FindString(dateStr)
	if yearStr != "" {
		year, _ = strconv.Atoi(yearStr)
		dateStr = strings.Replace(dateStr, yearStr, "", 1)
	} else {
		year = time.Now().Year()
	}
	log.Trace().Msgf("parsed year: %d, dateStr: %s", year, dateStr)

	dayStr := dayRegex.FindString(dateStr)
	if dayStr != "" {
		day, _ = strconv.Atoi(dayStr)
	}
	dateStr = strings.Replace(dateStr, dayStr, "", 1)

	for i, v := range loc.ShortMonthNames {
		if strings.Contains(strings.ToUpper(dateStr), strings.ToUpper(v)) {
			month = i + 1
			break
		}
	}
	if month == 0 {
		return time.Time{}, fmt.Errorf("could not find month in string %s", dateStr)
	}
	log.Trace().Msgf("parsed month: %d, dateStr: %s", month, dateStr)

	if timeStr != "" {
		timeMatch := timeRegex.FindStringSubmatch(timeStr)
		if timeMatch == nil {
			return time.Time{}, fmt.Errorf("could not find time in string %s", timeStr)
		}
		hour, _ = strconv.Atoi(timeMatch[1])
		minute, _ = strconv.Atoi(timeMatch[2])
		if strings.EqualFold(timeMatch[3], "pm") && hour < 12 {
			hour += 12
		}
		if strings.EqualFold(timeMatch[3], "am") && hour == 12 {
			hour = 0
		}
	}

	// read time zone from timezoneStr in format GMT-05:00
	var timeZone *time.Location
	timeZoneStr := strings.Trim(tzStr, " ")
	if timeZoneStr != "" {
		timeZoneMatch := timeZoneRegex.FindStringSubmatch(timeZoneStr)
		if timeZoneMatch != nil {
			tzHour, _ := strconv.Atoi(timeZoneMatch[2])
			tzMinute, _ := strconv.Atoi(timeZoneMatch[3])
			offset := tzHour*60*60 + tzMinute*60
			if timeZoneMatch[1] == "-" {
				offset = -offset
			}
			timeZone = time.FixedZone("", offset)
		} else {
			return time.Time{}, fmt.Errorf("could not parse time zone in string %s", timeZoneStr)
		}
	} else {
		timeZone = time.Local
	}
	return time.Date(year, time.Month(month), day, hour, minute, 0, 0, timeZone), nil
}

func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
