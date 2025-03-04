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
	"runtime"
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

	"github.com/spraot/go-dateparser"
)

var (
	nItemsFlag    = flag.Int("n", -1, "number of items to download. If negative, get them all.")
	devFlag       = flag.Bool("dev", false, "dev mode. we reuse the same session dir (/tmp/gphotos-cdp), so we don't have to auth at every run.")
	dlDirFlag     = flag.String("dldir", "", "where to write the downloads. defaults to $HOME/Downloads/gphotos-cdp.")
	fromFlag      = flag.String("from", "", "earliest date to sync (YYYY-MM-DD)")
	toFlag        = flag.String("to", "", "latest date to sync (YYYY-MM-DD)")
	runFlag       = flag.String("run", "", "the program to run on each downloaded item, right after it is dowloaded. It is also the responsibility of that program to remove the downloaded item, if desired.")
	verboseFlag   = flag.Bool("v", false, "be verbose")
	fileDateFlag  = flag.Bool("date", false, "set the file date to the photo date from the Google Photos UI")
	headlessFlag  = flag.Bool("headless", false, "Start chrome browser in headless mode (must use -dev and have already authenticated).")
	jsonLogFlag   = flag.Bool("json", false, "output logs in JSON format")
	logLevelFlag  = flag.String("loglevel", "", "log level: debug, info, warn, error, fatal, panic")
	removedFlag   = flag.Bool("removed", false, "save list of files found locally that appear to be deleted from Google Photos")
	workersFlag   = flag.Int("workers", 6, "number of concurrent downloads allowed")
	albumIdFlag   = flag.String("album", "", "ID of album to download, has no effect if lastdone file is found or if -start contains full URL")
	albumTypeFlag = flag.String("albumtype", "album", "type of album to download (as seen in URL), has no effect if lastdone file is found or if -start contains full URL")
)

var tick = 500 * time.Millisecond
var errStillProcessing = errors.New("video is still processing & can be downloaded later")
var errCouldNotPressDownloadButton = errors.New("could not press download button")
var originalSuffix = "_original"
var errPhotoTakenBeforeFromDate = errors.New("photo taken before from date")
var errPhotoTakenAfterToDate = errors.New("photo taken after to date")
var fromDate time.Time
var toDate time.Time
var dateParserCfg dateparser.Configuration = dateparser.Configuration{
	Languages:       []string{"en"},
	DefaultTimezone: time.Local,
}
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
	if !*devFlag {
		log.Fatal().Msg("-start only allowed in dev mode")
	}
	if !*devFlag && *headlessFlag {
		log.Fatal().Msg("-headless only allowed in dev mode")
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
			return
		}
	}
	if *toFlag != "" {
		var err error
		toDate, err = time.Parse(time.DateOnly, *toFlag)
		toDate = toDate.Add(time.Hour * 24)
		if err != nil {
			log.Fatal().Msgf("could not parse -to argument %s, must be YYYY-MM-DD", *toFlag)
			return
		}
	}

	s, err := NewSession()
	if err != nil {
		log.Err(err).Msgf("Failed to create session")
		return
	}
	defer s.Shutdown()

	log.Info().Msgf("Session Dir: %v", s.profileDir)

	if err := s.cleanDlDir(); err != nil {
		log.Err(err).Msgf("Failed to clean download directory %v", s.dlDir)
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
	var exists bool
	loc, exists = locales[locale]
	if !exists {
		log.Warn().Msgf("Your Google account is using unsupported locale %s, this is likely to cause issues. Please change account language to English (en) or another supported locale", locale)
	} else {
		log.Info().Msgf("Using locale %s", locale)
	}
	dateParserCfg.Locales = []string{locale}

	if err := chromedp.Run(ctx,
		chromedp.ActionFunc(s.firstNav),
	); err != nil {
		log.Fatal().Msg(err.Error())
		return
	}

	if err := chromedp.Run(ctx,
		chromedp.ActionFunc(s.resync),
	); err != nil {
		log.Fatal().Msg(err.Error())
		return
	}

	log.Info().Msg("Done")
}

type PhotoData struct {
	date     time.Time
	filename string
}

type Job struct {
	location string
	errChan  chan error
}

type NewDownload struct {
	GUID              string
	suggestedFilename string
}

type DownloadChannels struct {
	newdl    chan NewDownload
	progress chan bool
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
	parentContext   context.Context
	parentCancel    context.CancelFunc
	dlDir           string // dir where the photos get stored
	dlDirTmp        string // dir where the photos get stored temporarily
	profileDir      string // user data session dir. automatically created on chrome startup.
	startNodeParent *cdp.Node
	nextDl          chan DownloadChannels
	err             chan error
	photoRelPath    string
}

func NewSession() (*Session, error) {
	var dir string
	if *devFlag {
		dir = filepath.Join(os.TempDir(), "gphotos-cdp")
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
	dlDir := *dlDirFlag
	if dlDir == "" {
		dlDir = filepath.Join(os.Getenv("HOME"), "Downloads", "gphotos-cdp")
	}
	if err := os.MkdirAll(dlDir, 0700); err != nil {
		return nil, err
	}
	dlDirTmp := filepath.Join(dlDir, "tmp")
	if err := os.MkdirAll(dlDirTmp, 0700); err != nil {
		return nil, err
	}

	s := &Session{
		profileDir: dir,
		dlDir:      dlDir,
		dlDirTmp:   dlDirTmp,
		nextDl:     make(chan DownloadChannels, 1),
		err:        make(chan error, 1),
	}
	return s, nil
}

func (s *Session) NewWindow() (context.Context, context.CancelFunc) {
	log.Info().Msgf("Starting Chrome browser")

	// Let's use as a base for allocator options (It implies Headless)
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.DisableGPU,
		chromedp.UserDataDir(s.profileDir),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("lang", "en-US,en"),
		chromedp.Flag("accept-lang", "en-US,en"),
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
	s.parentContext = ctx
	s.parentCancel = cancel

	ctx, cancel = chromedp.NewContext(s.parentContext)
	ctx = SetContextData(ctx)

	if err := chromedp.Run(ctx,
		chromedp.ActionFunc(func(ctx context.Context) error {
			c := chromedp.FromContext(ctx)
			return browser.SetDownloadBehavior(browser.SetDownloadBehaviorBehaviorAllowAndName).WithDownloadPath(s.dlDirTmp).WithEventsEnabled(true).
				// use the Browser executor so that it does not pass "sessionId" to the command.
				Do(cdp.WithExecutor(ctx, c.Browser))
		}),
	); err != nil {
		panic(err)
	}

	startDlListener(ctx, s.nextDl, s.err)

	return ctx, cancel
}

func (s *Session) Shutdown() {
	s.parentCancel()
}

// cleanDlDir removes all files (but not directories) from s.dlDir
func (s *Session) cleanDlDir() error {
	if s.dlDir == "" {
		return nil
	}
	entries, err := os.ReadDir(s.dlDirTmp)
	if err != nil {
		return err
	}
	for _, v := range entries {
		if v.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(s.dlDirTmp, v.Name())); err != nil {
			return err
		}
	}
	return nil
}

// login navigates to https://photos.google.com/ and waits for the user to have
// authenticated (or for 2 minutes to have elapsed).
func (s *Session) login(ctx context.Context) error {
	log.Info().Msg("Starting authentication...")
	return chromedp.Run(ctx,
		chromedp.Navigate("https://photos.google.com/"),
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
				if strings.HasPrefix(location, "https://photos.google.com") {
					return nil
				}
				if *headlessFlag {
					dlScreenshot(ctx, filepath.Join(s.dlDir, "error.png"))
					return errors.New("authentication not possible in -headless mode, see error.png (at " + location + ")")
				}
				log.Debug().Msgf("Not yet authenticated, at: %v", location)
				time.Sleep(tick)
			}
		}),
		chromedp.ActionFunc(func(ctx context.Context) error {
			log.Info().Msg("Successfully authenticated")
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
		log.Warn().Err(err).Msg("Failed to detect account locale, will assume English (en)")
		return "en", nil
	}

	return locale, nil
}

func dlScreenshot(ctx context.Context, filePath string) {
	var buf []byte

	log.Trace().Msgf("Saving screenshot to %v", filePath+".png")
	if err := chromedp.Run(ctx, chromedp.CaptureScreenshot(&buf)); err != nil {
		log.Err(err).Msg(err.Error())
	} else if err := os.WriteFile(filePath+".png", buf, os.FileMode(0666)); err != nil {
		log.Err(err).Msg(err.Error())
	}

	// Dump the HTML to a file
	var html string
	if err := chromedp.Run(ctx, chromedp.OuterHTML("html", &html, chromedp.ByQuery)); err != nil {
		log.Err(err).Msg(err.Error())
	} else if err := os.WriteFile(filePath+".html", []byte(html), 0640); err != nil {
		log.Err(err).Msg(err.Error())
	}
}

// firstNav does either of:
// 1) if a specific photo URL was specified with *startFlag, it navigates to it
// 2) if the last session marked what was the most recent downloaded photo, it navigates to it
// 3) otherwise it jumps to the end of the timeline (i.e. the oldest photo)
func (s *Session) firstNav(ctx context.Context) (err error) {
	if *albumIdFlag != "" {
		if strings.Contains(*albumIdFlag, "/") {
			s.photoRelPath = "/" + *albumIdFlag
		} else {
			s.photoRelPath = "/" + *albumTypeFlag + "/" + *albumIdFlag
		}
	}

	// This is only used to ensure page is loaded
	if err := s.setFirstItem(ctx); err != nil {
		return err
	}

	if s.photoRelPath != "" {
		resp, err := chromedp.RunResponse(ctx, chromedp.Navigate("https://photos.google.com"+s.photoRelPath))
		if err != nil {
			return err
		}
		code := resp.Status
		if code != http.StatusOK {
			return fmt.Errorf("unexpected %d code when restarting to https://photos.google.com%s", code, s.photoRelPath)
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
		log.Info().Msgf("Attempting to scroll to %v", startDate)

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
		var foundDateNode *cdp.Node
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
							return main.scrollTop/main.scrollHeight;
						})();
					`, &scrollPos).Do(ctx); err != nil {
					return err
				}
				if math.Abs(scrollPos-scrollTarget) < 0.002 {
					break
				}
			}
			log.Trace().Msgf("scroll position: %.2f%%", scrollPos*100)

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
			var closestDateDiff float64
			var knownFirstOccurance bool
			for i, n := range dateNodes {
				if n.NodeName != "DIV" || n.ChildNodeCount == 0 {
					continue
				}
				dateStr := n.Children[0].NodeValue
				dpt, err := dateparser.Parse(&dateParserCfg, dateStr)
				if err != nil {
					return fmt.Errorf("could not parse date %s: %w", dateStr, err)
				}
				diff := dpt.Time.Sub(startDate).Hours()
				log.Trace().Msgf("parsed date element %v with distance %d days", dpt.Time, int(diff/24))
				if closestDateNode == nil || math.Abs(diff) < math.Abs(closestDateDiff) {
					closestDateNode = n
					closestDateDiff = diff
					knownFirstOccurance = i > 0
				}
			}

			if closestDateDiff == 0 && closestDateNode != nil {
				if knownFirstOccurance {
					foundDateNode = closestDateNode
					break
				} else {
					bisectBounds[1] = scrollPos
				}
			} else if closestDateDiff > 0 {
				bisectBounds[0] = scrollPos
			} else if closestDateDiff < 0 {
				bisectBounds[1] = scrollPos
			}

			time.Sleep(50 * time.Millisecond)
		}

		log.Debug().Msgf("final scroll position: %.2f%%", scrollPos*100)

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
	log.Debug().Msgf("Location: %v", location)

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
		log.Trace().Msg("Attempting to find first item")
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
	log.Debug().Msgf("Page loaded, most recent item in the feed is: %s", firstItem)
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

	log.Debug().Msg("Successfully jumped to the end")

	return nil
}

// doRun runs *runFlag as a command on the given filePath.
func doRun(filePath string) error {
	if *runFlag == "" {
		return nil
	}
	log.Debug().Msgf("Running %v on %v", *runFlag, filePath)
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
func requestDownload1(ctx context.Context, location string) error {
	muTabActivity.Lock()
	defer muTabActivity.Unlock()

	log.Debug().Msgf("Requesting download (method 1) for %v", location)
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
		log.Trace().Msgf("Triggering button press event: %v, %v, %v", ev.Key, ev.Type, ev.Modifiers)

		if err := chromedp.Run(ctx, ev); err != nil {
			return err
		}
	}
	return nil
}

// requestDownload2 clicks the icons to start the download of the currently
// viewed item.
func requestDownload2(ctx context.Context, location string, original bool, hasOriginal *bool) error {
	log.Debug().Str("original", fmt.Sprintf("%v", original)).Msgf("Requesting download (method 2) for %s", location)
	originalSelector := getAriaLabelSelector(loc.DownloadOriginalLabel)
	var selector string
	if original {
		selector = originalSelector
	} else {
		selector = getAriaLabelSelector(loc.DownloadLabel)
	}

	i := 0
	for {
		muTabActivity.Lock()
		ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
		c := chromedp.FromContext(ctx)
		target.ActivateTarget(c.Target.TargetID).Do(ctx)

		err := chromedp.Run(ctxTimeout,
			chromedp.ActionFunc(func(ctx context.Context) error {
				// Wait for more options menu to appear
				var nodesTmp []*cdp.Node
				if err := doActionWithTimeout(ctx, chromedp.Nodes(getAriaLabelSelector(loc.MoreOptionsLabel), &nodesTmp, chromedp.ByQuery), 6000*time.Millisecond); err != nil {
					return fmt.Errorf("could not find 'more options' button due to %w", err)
				}
				return nil
			}),

			// Open more options dialog
			chromedp.EvaluateAsDevTools(`[...document.querySelectorAll('`+getAriaLabelSelector(loc.MoreOptionsLabel)+`')].pop().click()`, nil),
			// chromedp.Sleep(50*time.Millisecond),

			// Go to download button
			chromedp.ActionFunc(func(ctx context.Context) error {
				// Wait for download button to appear
				var nodesTmp []*cdp.Node
				if err := doActionWithTimeout(ctx, chromedp.Nodes(selector, &nodesTmp, chromedp.ByQuery), 1000*time.Millisecond); err != nil {
					return fmt.Errorf("waiting for 'download' button failed due to %w", err)
				}

				// Check if there is an original version of the image that can also be downloaded
				if hasOriginal != nil {
					var dlOriginalNodes []*cdp.Node
					if err := chromedp.Nodes(originalSelector, &dlOriginalNodes, chromedp.AtLeast(0)).Do(ctx); err != nil {
						return fmt.Errorf("checking for original version failed due to %w", err)
					}
					*hasOriginal = len(dlOriginalNodes) > 0
				}
				return nil
			}),

			// Press down arrow until the right menu option is selected
			chromedp.ActionFunc(func(ctx context.Context) error {
				var nodes []*cdp.Node
				if err := doActionWithTimeout(ctx, chromedp.Nodes(selector, &nodes, chromedp.ByQuery), 1000*time.Millisecond); err != nil {
					return fmt.Errorf("could not find 'download' button due to %w", err)
				}

				n := 0
				for {
					n++
					if n > 30 {
						return errCouldNotPressDownloadButton
					}
					var style []*css.ComputedStyleProperty
					if err := chromedp.ComputedStyle(selector, &style).Do(ctx); err != nil {
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
		cancel()
		muTabActivity.Unlock()

		if err == nil {
			break
		} else if i > 5 {
			log.Warn().Str("location", location).Msgf("trying to request download with method 2 %d times, giving up now", i)
			break
		} else if err == errCouldNotPressDownloadButton || err.Error() == "Could not find node with given id (-32000)" || errors.Is(err, context.DeadlineExceeded) {
			log.Warn().Str("location", location).Msgf("trying to request download with method 2 again after error: %v", err)
		} else {
			return fmt.Errorf("encountered error '%s' when requesting download with method 2", err.Error())
		}

		time.Sleep(100 * time.Millisecond)
		i++
	}

	return nil
}

// getPhotoData gets the date from the currently viewed item.
// First we open the info panel by clicking on the "i" icon (aria-label="Open info")
// if it is not already open. Then we read the date from the
// aria-label="Date taken: ?????" field.
func (s *Session) getPhotoData(ctx context.Context) (PhotoData, error) {
	var filename string
	var dateStr string
	var timeStr string
	var tzStr string
	timeout1 := time.NewTimer(10 * time.Second)
	timeout2 := time.NewTimer(90 * time.Second)
	log.Debug().Msg("Extracting photo date text and original file name")

	var n = 0
	for {
		if err := func() error {
			muTabActivity.Lock()
			defer muTabActivity.Unlock()

			c := chromedp.FromContext(ctx)
			target.ActivateTarget(c.Target.TargetID).Do(ctx)

			n++

			if err := chromedp.Run(ctx,
				chromedp.Evaluate(getContentOfFirstVisibleNodeScript(getAriaLabelSelector(loc.FileNameLabel)), &filename),
				chromedp.Evaluate(getContentOfFirstVisibleNodeScript(getAriaLabelSelector(loc.DateLabel)), &dateStr),
				chromedp.Evaluate(getContentOfFirstVisibleNodeScript(getAriaLabelSelector(loc.DateLabel)+" + div "+getAriaLabelSelector(loc.TimeLabel)), &timeStr),
				chromedp.Evaluate(getContentOfFirstVisibleNodeScript(getAriaLabelSelector(loc.DateLabel)+" + div "+getAriaLabelSelector(loc.TzLabel)), &tzStr),
			); err != nil {
				return err
			}

			if len(filename) == 0 && len(dateStr) == 0 && len(timeStr) == 0 {
				log.Trace().Msgf("Incomplete data - Date: %v, Time: %v, Timezone: %v, File name: %v", dateStr, timeStr, tzStr, filename)

				// Click on info button
				log.Debug().Msg("Date not visible, clicking on i button")
				if err := chromedp.Run(ctx, chromedp.EvaluateAsDevTools(`[...document.querySelectorAll('`+getAriaLabelSelector(loc.OpenInfoMatch)+`')].pop()?.click()`, nil)); err != nil {
					return err
				}

				select {
				case <-timeout1.C:
					if _, err := chromedp.RunResponse(ctx, chromedp.Reload()); err != nil {
						return err
					}
					timeout1 = time.NewTimer(30 * time.Second)
				case <-timeout2.C:
					return fmt.Errorf("timeout waiting for photo info")
				case <-time.After(time.Duration(250+n*75) * time.Millisecond):
				}
			}
			return nil
		}(); err != nil {
			return PhotoData{}, err
		} else if len(filename) > 0 && len(dateStr) > 0 && len(timeStr) > 0 {
			break
		}
	}

	log.Trace().Msgf("Parsing date: %v and time: %v", dateStr, timeStr)
	log.Trace().Msgf("Parsing filename: %v", filename)

	// Handle special days like "Yesterday" and "Today"
	timeStr = strings.Replace(timeStr, loc.Yesterday, loc.ShortDayNames[time.Now().AddDate(0, 0, -1).Day()], -1)
	timeStr = strings.Replace(timeStr, loc.Today, loc.ShortDayNames[time.Now().Day()], -1)
	fullDateStr := onlyPrintable(dateStr + ", " + timeStr + " " + tzStr)
	date, err := dateparser.Parse(&dateParserCfg, fullDateStr)
	if err != nil && strings.HasSuffix(err.Error(), "unknown format") {
		log.Err(err).Msg("dateparser returned potentially spurious error, trying again with no language in cfg")
		cfg := dateParserCfg
		cfg.Languages = []string{}
		date, err = dateparser.Parse(&cfg, fullDateStr)
	}
	if err != nil {
		return PhotoData{}, fmt.Errorf("getting photo data, %w", err)
	}

	log.Debug().Msgf("Found date: %v and original filename: %v", date, filename)

	return PhotoData{date.Time, filename}, nil
}

var dlLock sync.Mutex = sync.Mutex{}

// startDownload starts the download of the currently viewed item, and on successful
// completion saves its location as the most recent item downloaded. It returns
// with an error if the download stops making any progress for more than a minute.
func (s *Session) startDownload(ctx context.Context, location string, dlOriginal bool, hasOriginal *bool) (newDl NewDownload, progress chan bool, err error) {
	log.Trace().Msgf("entering startDownload() for %v, dlOriginal=%v", location, dlOriginal)

	dlLock.Lock()
	defer dlLock.Unlock()

	if len(s.nextDl) != 0 {
		return NewDownload{}, nil, errors.New("unexpected: nextDl channel is not empty")
	}

	dlStarted := make(chan NewDownload, 1)
	dlProgress := make(chan bool, 1)
	s.nextDl <- DownloadChannels{dlStarted, dlProgress}

	defer func() {
		if err != nil {
			select {
			case <-s.nextDl:
			default:
			}
		}
	}()

	if err := requestDownload2(ctx, location, dlOriginal, hasOriginal); err != nil {
		if dlOriginal || err != errCouldNotPressDownloadButton {
			return NewDownload{}, nil, err
		} else if !dlOriginal {
			requestDownload1(ctx, location)
		}
	}

	timeout1 := time.NewTimer(30 * time.Second)
	timeout2 := time.NewTimer(60 * time.Second)

	for {
		if timedLogReady("downloadStatus"+location, 15*time.Second) {
			log.Debug().Msgf("Checking download status of %v", location)
		}

		// Checking for gphotos warning that this video can't be downloaded (no known solution)
		// This check only works for requestDownload2 method (not requestDownload1)
		if err := s.checkForStillProcessing(ctx); err != nil {
			return NewDownload{}, nil, err
		}

		select {
		case <-timeout1.C:
			if err := requestDownload1(ctx, location); err != nil {
				return NewDownload{}, nil, err
			}
		case <-timeout2.C:
			return NewDownload{}, nil, fmt.Errorf("timeout waiting for download to start for %v", location)
		case newDl := <-dlStarted:
			log.Trace().Msgf("dlStarted: %v", newDl)
			return newDl, dlProgress, nil
		default:
			time.Sleep(25 * time.Millisecond)
		}
	}
}

func (*Session) checkForStillProcessing(ctx context.Context) error {
	log.Trace().Msgf("Checking for still processing dialog")
	var nodes []*cdp.Node
	if err := chromedp.Nodes(getAriaLabelSelector(loc.VideoStillProcessingDialogLabel)+` button`, &nodes, chromedp.ByQuery, chromedp.AtLeast(0)).Do(ctx); err != nil {
		return err
	}
	isStillProcessing := len(nodes) > 0
	if isStillProcessing {
		log.Debug().Msgf("Found still processing dialog, need to press button to remove")
		// Click the button to close the warning, otherwise it will block navigating to the next photo
		cl := GetContextData(ctx)
		cl.muKbEvents.Lock()
		err := chromedp.MouseClickNode(nodes[0]).Do(ctx)
		cl.muKbEvents.Unlock()
		if err != nil {
			return err
		}
	} else {
		log.Trace().Msgf("Checking for still processing status (at bottom of screen)")
		// This check only works for requestDownload1 method (not requestDownload2)
		if err := chromedp.Evaluate("document.body.textContent.indexOf('"+loc.VideoStillProcessingStatusText+"') != -1", &isStillProcessing).Do(ctx); err != nil {
			return err
		}
		if isStillProcessing {
			log.Debug().Msg("Found still processing status at bottom of screen, waiting for it to disappear before continuing")
			time.Sleep(5 * time.Second) // Wait for error message to disappear before continuing, otherwise we will also skip next files
		}
		if !isStillProcessing {
			// Sometimes Google returns a different error, check for that too
			log.Trace().Msg("Checking for loading error page")
			if err := chromedp.Evaluate("document.body.textContent.indexOf('"+loc.NoWebpageFoundText+"') != -1", &isStillProcessing).Do(ctx); err != nil {
				return err
			}
			if isStillProcessing {
				log.Info().Msgf("This is an error page, we will navigate back to the photo to be able to continue")
				if err := navWithAction(ctx, chromedp.NavigateBack()); err != nil {
					return err
				}
				time.Sleep(400 * time.Millisecond)
			}
		}
	}
	if isStillProcessing {
		log.Warn().Msg("Received 'Video is still processing' error")
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

// makeOutDir creates a directory in s.dlDir named of the item ID found in
// location
func (s *Session) makeOutDir(location string) (string, error) {
	imageId, err := imageIdFromUrl(location)
	if err != nil {
		return "", err
	}

	newDir := filepath.Join(s.dlDir, imageId)
	if err := os.MkdirAll(newDir, 0700); err != nil {
		return "", err
	}
	return newDir, nil
}

// processDownload creates a directory in s.dlDir named of the item ID found in
// location. It then moves dlFile in that directory
func (s *Session) processDownload(dl NewDownload, dlProgress chan bool, errChan chan error, isOriginal bool, location string, photoDataChan chan PhotoData) {
	log.Trace().Msgf("entering processDownload for %v", dl.GUID)
	dlTimeout := time.NewTimer(time.Minute)
progressLoop:
	for {
		select {
		case p := <-dlProgress:
			if p {
				// download done
				break progressLoop
			} else {
				// still downloading
				dlTimeout.Reset(time.Minute)
			}
		case <-dlTimeout.C:
			errChan <- fmt.Errorf("timeout waiting for download to complete for %v", location)
			return
		}
	}

	data := <-photoDataChan

	outDir, err := s.makeOutDir(location)
	if err != nil {
		errChan <- err
		return
	}

	var filePaths []string
	if strings.HasSuffix(dl.suggestedFilename, ".zip") {
		var err error
		filePaths, err = s.handleZip(filepath.Join(s.dlDirTmp, dl.GUID), outDir)
		if err != nil {
			errChan <- err
			return
		}
	} else {
		var filename string
		if dl.suggestedFilename != "download" && dl.suggestedFilename != "" {
			filename = dl.suggestedFilename
		} else {
			filename = data.filename
		}

		if isOriginal {
			// to ensure the filename is not the same as the other download, change e.g. image_1.jpg to image_1_original.jpg
			ext := filepath.Ext(filename)
			filename = strings.TrimSuffix(filename, ext) + originalSuffix + ext
		}

		newFile := filepath.Join(outDir, filename)
		log.Debug().Msgf("Moving %v to %v", dl.GUID, newFile)
		if err := os.Rename(filepath.Join(s.dlDirTmp, dl.GUID), newFile); err != nil {
			errChan <- err
			return
		}
		filePaths = []string{newFile}
	}

	if err := doFileDateUpdate(data.date, filePaths); err != nil {
		errChan <- err
		return
	}

	for _, f := range filePaths {
		if err := doRun(f); err != nil {
			errChan <- err
			return
		}
	}

	errChan <- nil

	for _, f := range filePaths {
		log.Info().Msgf("downloaded %v with date %v", filepath.Base(f), data.date.Format(time.DateOnly))
	}
}

// downloadAndProcessItem starts a download then sends it to processDownload for processing
func (s *Session) downloadAndProcessItem(ctx context.Context, outerErrChan chan error, location string) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log.Trace().Msgf("entering downloadAndProcessItem() for %v", location)

	photoDataChan := make(chan PhotoData, 2)
	go func() {
		log.Trace().Msgf("Getting photo data for %v", location)
		data, err := s.getPhotoData(ctx)
		if err != nil {
			outerErrChan <- err
		} else {
			if fromDate != (time.Time{}) && data.date.Before(fromDate) {
				outerErrChan <- errPhotoTakenBeforeFromDate
			}
			if toDate != (time.Time{}) && data.date.After(toDate) {
				outerErrChan <- errPhotoTakenAfterToDate
			}
			// we need two of these if we are downloading an original
			photoDataChan <- data
			photoDataChan <- data
		}
	}()

	errChan1 := make(chan error, 1)
	hasOriginalChan := make(chan bool, 1)

	go func() {
		hasOriginal := false
		dl, dlProgress, err := s.startDownload(ctx, location, false, &hasOriginal)
		if err != nil {
			log.Trace().Msgf("download of %v failed: %v", location, err)
			dlScreenshot(ctx, filepath.Join(s.dlDir, "error"))
			errChan1 <- err
			hasOriginalChan <- false
		} else {
			hasOriginalChan <- hasOriginal
			go s.processDownload(dl, dlProgress, errChan1, false, location, photoDataChan)
		}
	}()

	errChan2 := make(chan error, 1)

	go func() {
		if <-hasOriginalChan {
			dl, dlProgress, err := s.startDownload(ctx, location, true, nil)
			if err != nil {
				log.Trace().Msgf("download of %v failed: %v", location, err)
				dlScreenshot(ctx, filepath.Join(s.dlDir, "error"))
				errChan2 <- err
			} else {
				go s.processDownload(dl, dlProgress, errChan2, true, location, photoDataChan)
			}
		} else {
			errChan2 <- nil
		}
	}()

	jobsRemaining := 2

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(60 * time.Second):
				log.Trace().Msgf("downloadAndProcessItem(): waiting for %d jobs to finish for %s", jobsRemaining, location)
			}
		}
	}()

	for {
		var err error
		select {
		case err = <-errChan1:
		case err = <-errChan2:
		case <-ctx.Done():
			return
		}
		log.Trace().Msgf("downloadAndProcessItem(): job result: %s for %s", err, location)
		jobsRemaining--
		if err != nil {
			// Error downloading original or generated image, remove files already downloaded
			outDir, errTmp := s.makeOutDir(location)
			if errTmp != nil {
				log.Err(errTmp).Msg(errTmp.Error())
			} else if errTmp := os.RemoveAll(outDir); errTmp != nil {
				log.Err(errTmp).Msg(errTmp.Error())
			}
			outerErrChan <- err
		}
		if jobsRemaining == 0 {
			break
		}
	}

	outerErrChan <- nil
}

// handleZip handles the case where the currently item is a zip file. It extracts
// each file in the zip file to the same folder, and then deletes the zip file.
func (s *Session) handleZip(zipfile, outFolder string) ([]string, error) {
	st := time.Now()
	log.Debug().Msgf("Unzipping %v in %v", zipfile, outFolder)
	// unzip the file
	files, err := zip.Unzip(zipfile, outFolder)
	if err != nil {
		return []string{""}, err
	}

	// delete the zip file
	if err := os.Remove(zipfile); err != nil {
		return []string{""}, err
	}

	log.Debug().Msgf("Unzipped %v in %v", zipfile, time.Since(st))
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
		chromedp.Evaluate(`+(document.querySelector('div[role="slider"][aria-valuemax="1"][aria-valuetext]')?.ariaValueNow || 0.0)`, &sliderPos),
		chromedp.Evaluate(`(document.querySelector('div[role="slider"][aria-valuemax="1"][aria-valuetext]')?.ariaValueText || '')`, &sliderText),
	); err != nil {
		return 0, "", fmt.Errorf("error finding slider node, %w", err)
	}

	return sliderPos, sliderText, nil
}

// Resync the library/album of photos
// Use [...document.querySelectorAll('a[href^="./{relPath}photo/"]')] to find all visible photos
// Check that each one is already downloaded. Optionally check/update date from the element
// attr, e.g. aria-label="Photo - Landscape - Feb 12, 2025, 6:34:39 PM"
// Then do .pop().focus() on the last a element found to scroll to it and make more photos visible
// Then repeat until we get to the end
// If any photos are missing we can asynchronously create a new chromedp context, then in that
// context navigate to that photo and call dlAndProcess
func (s *Session) resync(ctx context.Context) error {
	listenNavEvents(ctx)

	asyncJobs := []Job{}
	photoIds := []string{}
	lastNode := &cdp.Node{}
	var nodes []*cdp.Node
	i := 0
	n := 0
	dlCnt := 0
	retries := 0
	sliderPos := 0.0
	estimatedRemaining := 1000
	lastHref := ""
	inProgressCnt := 0
	batchProcessing := false

	dlDirEntries, err := os.ReadDir(s.dlDir)
	if err != nil {
		return err
	}
	existingDlFoldersMap := make(map[string]struct{}, len(dlDirEntries))
	for _, e := range dlDirEntries {
		existingDlFoldersMap[e.Name()] = struct{}{}
	}

	log.Trace().Msgf("Finding start node")
	opts := []chromedp.QueryOption{chromedp.ByQuery, chromedp.AtLeast(0)}
	if s.startNodeParent != nil {
		opts = append(opts, chromedp.FromNode(s.startNodeParent))
	}

	if err := chromedp.Nodes(`a[href^=".`+s.photoRelPath+`/photo/"]`, &nodes, opts...).Do(ctx); err != nil {
		return fmt.Errorf("error finding photo nodes, %w", err)
	}
	if len(nodes) == 0 {
		log.Info().Msg("No photos to resync")
		return nil
	}
	if err := dom.Focus().WithNodeID(nodes[0].NodeID).Do(ctx); err != nil {
		return fmt.Errorf("error focusing first photo node, %w", err)
	}

	for {
		if retries%5 == 0 {
			c := chromedp.FromContext(ctx)
			target.ActivateTarget(c.Target.TargetID).Do(ctx)

			if batchProcessing {
				log.Trace().Msgf("We seem to be stuck, manually scrolling might help")
				if err := doActionWithTimeout(ctx, chromedp.KeyEvent(kb.ArrowDown), 2000*time.Millisecond); err != nil {
					log.Err(err).Msgf("error scrolling page down manually, %v", err)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}

		// New new nodes found, does it look like we are done?
		if retries > 5000 || (retries > 100 && estimatedRemaining < 50) {
			break
		}

		if n%50 == 0 {
			var err error
			sliderPos, _, err = getSliderPosAndText(ctx)
			if err != nil {
				return fmt.Errorf("error getting slider position and text, %w", err)
			}
		}

		if n != 0 {
			if inProgressCnt >= *workersFlag || n%100 == 0 {
				if err := s.processJobs(&asyncJobs, *workersFlag-1, &inProgressCnt); err != nil {
					if err == errPhotoTakenBeforeFromDate {
						log.Info().Msg("Found photo taken before -from date, stopping sync here")
						break
					}
					return err
				}
			}

			if i >= len(nodes) {
				log.Trace().Msgf("finding new nodes to process")

				if retries == 0 {
					newBatchProcessing := n > 100 && (inProgressCnt == 0 || inProgressCnt*2 < *workersFlag)
					if newBatchProcessing != batchProcessing {
						log.Info().Msgf("setting batchProcessing = %v", newBatchProcessing)
					}
					batchProcessing = newBatchProcessing
				}
				if batchProcessing {
					// Constrained by finding new files to download, let's get a whole batch to process
					// start by scrolling to the next batch by focusing the last processed node
					log.Trace().Msgf("Scrolling to %v", lastNode.NodeID)
					if err := doActionWithTimeout(ctx, dom.Focus().WithNodeID(lastNode.NodeID), 1000*time.Millisecond); err != nil {
						log.Debug().Msgf("error scrolling to next batch of items: %v", err)
					}
					time.Sleep(50 * time.Millisecond)

					if err := chromedp.Nodes(`a[href^=".`+s.photoRelPath+`/photo/"]`, &nodes, chromedp.ByQueryAll, chromedp.AtLeast(0)).Do(ctx); err != nil {
						return fmt.Errorf("error finding photo nodes, %w", err)
					}
					log.Trace().Msgf("Checking %d nodes for new nodes", len(nodes))

					// remove already processed nodes
					foundNodes := len(nodes)
					for i, node := range nodes {
						if node == lastNode {
							log.Trace().Msgf("Found %d nodes, %d have already been processed", len(nodes), i+1)
							nodes = nodes[i+1:]
							break
						}
					}
					if len(nodes) == 0 {
						retries++
						continue
					}
					if foundNodes == len(nodes) {
						log.Debug().Msg("only new nodes found, expected an overlap")
					}
				}

				if !batchProcessing {
					// Constrained by downloads, just move one right
					if err := chromedp.KeyEvent(kb.ArrowRight).Do(ctx); err != nil {
						return fmt.Errorf("error sending right arrow key event, %w", err)
					}
					time.Sleep(1 * time.Millisecond)

					log.Trace().Msg("Getting active element (if it's an A element with a photo href)")
					if err := chromedp.Nodes(`document.activeElement`, &nodes, chromedp.ByJSPath).Do(ctx); err != nil {
						return fmt.Errorf("error getting active element, %w", err)
					}

					if nodes[0].NodeName != "A" || nodes[0].AttributeValue("href") == lastHref {
						log.Trace().Msg("didn't find new photo node, will try again")
						time.Sleep(1 * time.Millisecond)
						nodes = []*cdp.Node{}
						retries++
						continue
					}
				}

				retries = 0
				i = 0
			}

			if n < 200 {
				estimatedRemaining = 200
			} else {
				estimatedRemaining = int(math.Floor((1/sliderPos - 1) * float64(n)))
			}

			if timedLogReady("resyncLoop", 60*time.Second) {
				log.Info().Msgf("so far: resynced %v items, found %v new items, progress: %.2f%%, estimated remaining: %d", n, dlCnt, sliderPos*100, estimatedRemaining)
			}
		}

		if len(nodes) == 0 {
			continue
		}

		lastNode = nodes[i]
		href := lastNode.AttributeValue("href")
		retries = 0
		lastHref = href

		n++
		i++

		imageId, err := imageIdFromUrl(href)
		if err != nil {
			return err
		}
		photoIds = append(photoIds, imageId)
		log.Trace().Msgf("processing %v", imageId)

		if _, ok := existingDlFoldersMap[imageId]; ok {
			hasFiles, err := dirHasFiles(filepath.Join(s.dlDir, imageId))
			if err != nil {
				return err
			} else if hasFiles {
				log.Trace().Msgf("skipping %v, already downloaded", imageId)
				continue
			}
		}

		ariaLabel := ""
		if jsNode, err := dom.ResolveNode().WithNodeID(lastNode.NodeID).Do(ctx); err != nil {
			log.Err(err).Msgf("error resolving object id of node, %s", err.Error())
		} else {
			if err := chromedp.Run(ctx, chromedp.CallFunctionOn(`function() { return this.ariaLabel }`, &ariaLabel,
				func(p *cdpruntime.CallFunctionOnParams) *cdpruntime.CallFunctionOnParams {
					return p.WithObjectID(jsNode.ObjectID)
				},
			)); err != nil {
				log.Err(err).Msgf("error reading node ariaLabel, %s", err.Error())
			}
			if ariaLabel != "" {
				ariaLabel = "(" + ariaLabel + ")"
			}
		}

		if strings.Contains(ariaLabel, "Highlight video") {
			log.Info().Msgf("Skipping highlight video %v (%s)", imageId, ariaLabel)
			continue
		}

		log.Info().Msgf("photo %v %s is missing. Downloading it.", imageId, ariaLabel)

		// asynchronously create a new chromedp context, then in that
		// context navigate to that photo and call downloadAndProcessItem
		location := "https://photos.google.com" + s.photoRelPath + "/photo/" + imageId
		dlErrChan := make(chan error, 1)
		go func() {
			ctx, cancel := chromedp.NewContext(ctx)
			defer cancel()

			ctx = SetContextData(ctx)
			listenNavEvents(ctx)

			log.Trace().Msgf("Navigating to %v", location)
			resp, err := chromedp.RunResponse(ctx, chromedp.Navigate(location))
			if err != nil {
				dlErrChan <- fmt.Errorf("error navigating to %v: %w", location, err)
				return
			}
			if resp.Status == http.StatusOK {
				chromedp.WaitReady("body", chromedp.ByQuery).Do(ctx)
			} else {
				dlErrChan <- fmt.Errorf("unexpected response: %v", resp.Status)
				return
			}

			time.Sleep(200 * time.Millisecond)

			s.downloadAndProcessItem(ctx, dlErrChan, location)
		}()
		asyncJobs = append(asyncJobs, Job{location, dlErrChan})
		dlCnt++
		inProgressCnt++
	}
	log.Info().Msgf("in total: resynced %v items, found %v new items, progress: %.2f%%", n, dlCnt, sliderPos*100)

	if *removedFlag {
		// Check if there are folders in the dl dir that were not seen in gphotos

		// Create a map for O(1) lookups instead of using contains() which is O(n)
		photoIdsMap := make(map[string]struct{}, len(photoIds))
		for _, id := range photoIds {
			photoIdsMap[id] = struct{}{}
		}
		log.Info().Msg("Checking for removed files")
		deleted := []string{}
		for _, entry := range dlDirEntries {
			if entry.IsDir() && entry.Name() != "tmp" {
				// Check if the folder name is in the map of photo IDs
				if _, exists := photoIdsMap[entry.Name()]; !exists {
					deleted = append(deleted, entry.Name())
				}
			}
		}
		if len(deleted) > 0 {
			log.Info().Msgf("Folders found for %d local photos that were not found in this sync. Checking google photos to confirm they are not there", len(deleted))
		}
		i := 0
		for i < len(deleted) {
			imageId := deleted[i]
			var resp int
			if err := chromedp.Run(ctx,
				chromedp.Evaluate(`new Promise((res) => fetch('https://photos.google.com`+s.photoRelPath+`/photo/`+imageId+`').then(x => res(x.status)));`, &resp,
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
			log.Info().Msgf("Folders found for %d local photos that don't exist on google photos (in album if using -album), list saved to .removed", len(deleted))
			if err := os.WriteFile(path.Join(s.dlDir, ".removed"), []byte(strings.Join(deleted, "\n")), 0644); err != nil {
				return err
			}
		}
	}

	return s.processJobs(&asyncJobs, 0, nil)
}

func (s *Session) processJobs(jobs *[]Job, maxJobs int, dlCount *int) error {
	n := 0
	if dlCount == nil {
		dlCount = new(int)
	}
	for {
		*dlCount = 0
		for i := range *jobs {
			if (*jobs)[i].errChan == nil {
				continue
			}

			select {
			case err := <-(*jobs)[i].errChan:
				(*jobs)[i].errChan = nil
				if err == errStillProcessing {
					// Old highlight videos are no longer available
					log.Info().Msg("Skipping generated highlight video that Google seems to have lost")
				} else if err == errPhotoTakenAfterToDate {
					log.Warn().Msg("Skipping photo taken after -to date. If you see many of these messages, something has gone wrong.")
				} else if err == errPhotoTakenBeforeFromDate && maxJobs == 0 {
					log.Info().Msgf("Skipping photo taken before -from date. If you see more than %d of these messages, something has gone wrong.", *workersFlag)
				} else if err != nil {
					return err
				}
			case err := <-s.err:
				return err
			default:
			}

			if (*jobs)[i].errChan != nil {
				*dlCount++
			}
		}

		if *dlCount > 0 && timedLogReady("processJobs", 90*time.Second) {
			log.Info().Msgf("%d download jobs currently in progress", *dlCount)
		}

		if *dlCount <= maxJobs || maxJobs < 0 {
			break
		}

		// Let's wait for some downloads to finish before starting more
		time.Sleep(50 * time.Millisecond)
		n++
		if n > 50000 {
			return errors.New("waited too long for jobs to exit, exiting")
		}
	}
	return nil
}

// doFileDateUpdate updates the file date of the downloaded files to the photo date
func doFileDateUpdate(date time.Time, filePaths []string) error {
	if !*fileDateFlag {
		return nil
	}

	log.Debug().Msgf("Setting file date for %v", filePaths)

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

// Sets modified date of dlFile to given date
func setFileDate(dlFile string, date time.Time) error {
	if err := os.Chtimes(dlFile, date, date); err != nil {
		return err
	}
	return nil
}

func startDlListener(ctx context.Context, nextDl chan DownloadChannels, globalErrChan chan error) {
	dls := make(map[string]DownloadChannels)
	chromedp.ListenBrowser(ctx, func(v interface{}) {
		if ev, ok := v.(*browser.EventDownloadWillBegin); ok {
			log.Debug().Str("GUID", ev.GUID).Msgf("Download of %s started", ev.SuggestedFilename)
			if ev.SuggestedFilename == "downloads.html" {
				return
			}
			if len(nextDl) == 0 {
				globalErrChan <- fmt.Errorf("unexpected download of %s", ev.SuggestedFilename)
			}
			dls[ev.GUID] = <-nextDl
			dls[ev.GUID].newdl <- NewDownload{ev.GUID, ev.SuggestedFilename}
		}
	})

	chromedp.ListenBrowser(ctx, func(v interface{}) {
		if ev, ok := v.(*browser.EventDownloadProgress); ok {
			log.Trace().Msgf("Download event: %v", ev)
			if ev.State == browser.DownloadProgressStateInProgress {
				log.Trace().Str("GUID", ev.GUID).Msgf("Download progress")
				if len(dls[ev.GUID].progress) == 0 {
					dls[ev.GUID].progress <- false
				}
			}
			if ev.State == browser.DownloadProgressStateCompleted {
				log.Debug().Str("GUID", ev.GUID).Msgf("Download completed")
				go func() {
					time.Sleep(time.Second)
					dls[ev.GUID].progress <- true
					delete(dls, ev.GUID)
				}()
			}
		}
	})
}

func onlyPrintable(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 32 && r <= 126 {
			return r
		}
		return -1
	}, s)
}

func getContentOfFirstVisibleNodeScript(sel string) string {
	return fmt.Sprintf(`[...document.querySelectorAll('%s')].filter(x => x.checkVisibility()).map(x => x.textContent)[0] || ''`, sel)
}

func dirHasFiles(s string) (bool, error) {
	entries, err := os.ReadDir(s)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(entries) > 0, nil
}
