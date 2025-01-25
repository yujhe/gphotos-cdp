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
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/evilsocket/islazy/zip"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/input"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/kb"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	nItemsFlag   = flag.Int("n", -1, "number of items to download. If negative, get them all.")
	devFlag      = flag.Bool("dev", false, "dev mode. we reuse the same session dir (/tmp/gphotos-cdp), so we don't have to auth at every run.")
	dlDirFlag    = flag.String("dldir", "", "where to write the downloads. defaults to $HOME/Downloads/gphotos-cdp.")
	startFlag    = flag.String("start", "", "skip all photos until this location is reached. for debugging.")
	runFlag      = flag.String("run", "", "the program to run on each downloaded item, right after it is dowloaded. It is also the responsibility of that program to remove the downloaded item, if desired.")
	verboseFlag  = flag.Bool("v", false, "be verbose")
	fileDateFlag = flag.Bool("date", false, "set the file date to the photo date from the Google Photos UI")
	headlessFlag = flag.Bool("headless", false, "Start chrome browser in headless mode (cannot do authentication this way).")
	jsonLogFlag  = flag.Bool("json", false, "output logs in JSON format")
	logLevelFlag = flag.String("loglevel", "", "log level: debug, info, warn, error, fatal, panic")
)

var tick = 500 * time.Millisecond
var errStillProcessing = errors.New("video is still processing & can be downloaded later")

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
	if !*devFlag && *startFlag != "" {
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

	ctx, cancel := s.NewContext()
	defer cancel()

	if err := s.login(ctx); err != nil {
		log.Err(err).Msg("login failed")
		return
	}

	if err := chromedp.Run(ctx,
		chromedp.ActionFunc(s.firstNav),
		chromedp.ActionFunc(s.navN(*nItemsFlag)),
	); err != nil {
		log.Fatal().Msg(err.Error())
	}
	fmt.Println("OK")
}

type Session struct {
	parentContext context.Context
	parentCancel  context.CancelFunc
	dlDir         string // dir where the photos get stored
	dlDirTmp      string // dir where the photos get stored temporarily
	profileDir    string // user data session dir. automatically created on chrome startup.
	// lastDone is the most recent (wrt to Google Photos timeline) item (its URL
	// really) that was downloaded. If set, it is used as a sentinel, to indicate that
	// we should skip dowloading all items older than this one.
	lastDone string
	// firstItem is the most recent item in the feed. It is determined at the
	// beginning of the run, and is used as the final sentinel.
	firstItem string
}

// getLastDone returns the URL of the most recent item that was downloaded in
// the previous run. If any, it should have been stored in dlDir/.lastdone
func getLastDone(dlDir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dlDir, ".lastdone"))
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(data), nil
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
	lastDone, err := getLastDone(dlDir)
	if err != nil {
		return nil, err
	}
	s := &Session{
		profileDir: dir,
		dlDir:      dlDir,
		dlDirTmp:   dlDirTmp,
		lastDone:   lastDone,
	}
	return s, nil
}

func (s *Session) NewContext() (context.Context, context.CancelFunc) {
	// Let's use as a base for allocator options (It implies Headless)
	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.DisableGPU,
		chromedp.UserDataDir(s.profileDir),
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
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
		if v.Name() == ".lastdone" {
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
	return chromedp.Run(ctx,
		browser.SetDownloadBehavior(browser.SetDownloadBehaviorBehaviorAllow).WithDownloadPath(s.dlDirTmp),
		chromedp.ActionFunc(func(ctx context.Context) error {
			log.Debug().Msg("pre-navigate")
			return nil
		}),
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
			log.Debug().Msg("post-navigate")
			return nil
		}),
	)
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
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	if err := s.setFirstItem(ctx); err != nil {
		return err
	}

	if *startFlag != "" {
		// TODO(mpl): use RunResponse
		chromedp.Navigate(*startFlag).Do(ctx)
		chromedp.WaitReady("body", chromedp.ByQuery).Do(ctx)
		return nil
	}
	if s.lastDone != "" {
		resp, err := chromedp.RunResponse(ctx, chromedp.Navigate(s.lastDone))
		if err != nil {
			return err
		}
		if resp.Status == http.StatusOK {
			chromedp.WaitReady("body", chromedp.ByQuery).Do(ctx)
			return nil
		}
		lastDoneFile := filepath.Join(s.dlDir, ".lastdone")
		log.Info().Msgf("%s does not seem to exist anymore. Removing %s.", s.lastDone, lastDoneFile)
		s.lastDone = ""
		if err := os.Remove(lastDoneFile); err != nil {
			if os.IsNotExist(err) {
				log.Err(err).Msg("Failed to remove .lastdone file because it was already gone.")
			}
			return err
		}

		// restart from scratch
		resp, err = chromedp.RunResponse(ctx, chromedp.Navigate("https://photos.google.com/"))
		if err != nil {
			return err
		}
		code := resp.Status
		if code != http.StatusOK {
			return fmt.Errorf("unexpected %d code when restarting to https://photos.google.com/", code)
		}
		chromedp.WaitReady("body", chromedp.ByQuery).Do(ctx)
	}

	log.Debug().Msg("Finding end of page")

	if err := s.navToEnd(ctx); err != nil {
		return err
	}

	if err := s.navToLast(ctx); err != nil {
		return err
	}

	return nil
}

// setFirstItem looks for the first item, and sets it as s.firstItem.
// We always run it first even for code paths that might not need s.firstItem,
// because we also run it for the side-effect of waiting for the first page load to
// be done, and to be ready to receive scroll key events.
func (s *Session) setFirstItem(ctx context.Context) error {
	// wait for page to be loaded, i.e. that we can make an element active by using
	// the right arrow key.
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
		if !ok || !strings.HasPrefix(photoHref, "./photo/") {
			time.Sleep(tick)
			continue
		}

		s.firstItem = strings.TrimPrefix(photoHref, "./photo/")
		break
	}
	log.Debug().Msgf("Page loaded, most recent item in the feed is: %s", s.firstItem)
	return nil
}

// navToEnd scrolls down to the end of the page, i.e. to the oldest items.
func (s *Session) navToEnd(ctx context.Context) error {
	// try jumping to the end of the page. detect we are there and have stopped
	// moving when two consecutive screenshots are identical.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

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

// navToLast sends the "\n" event until we detect that an item is loaded as a
// new page. It then sends the right arrow key event until we've reached the very
// last item.
func (s *Session) navToLast(ctx context.Context) error {
	deadline := time.Now().Add(4 * time.Minute)
	var location, prevLocation string
	ready := false
	for {
		// Check if context canceled
		if time.Now().After(deadline) {
			dlScreenshot(ctx, filepath.Join(s.dlDir, "error"))
			return errors.New("timed out while finding last photo, see error.png")
		}

		chromedp.KeyEvent(kb.ArrowRight).Do(ctx)
		time.Sleep(tick)
		if !ready {
			// run js in chromedp to open last visible photo
			chromedp.Evaluate(`[...document.querySelectorAll('[data-latest-bg]')].pop().click()`, nil).Do(ctx)
			time.Sleep(tick)
		}
		if err := chromedp.Location(&location).Do(ctx); err != nil {
			return err
		}
		if !ready {
			if location != "https://photos.google.com/" {
				ready = true
				log.Info().Msgf("Nav to the end sequence is started because location is %v", location)
			}
			continue
		}

		if location == prevLocation {
			break
		}
		prevLocation = location
	}
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
func navLeft(ctx context.Context) error {
	st := time.Now()
	muNavWaiting.Lock()
	listenEvents = true
	muNavWaiting.Unlock()
	chromedp.KeyEvent(kb.ArrowLeft).Do(ctx)
	muNavWaiting.Lock()
	navWaiting = true
	muNavWaiting.Unlock()
	t := time.NewTimer(time.Minute)
	select {
	case <-navDone:
		if !t.Stop() {
			<-t.C
		}
	case <-t.C:
		return errors.New("timeout waiting for left navigation")
	}
	muNavWaiting.Lock()
	navWaiting = false
	muNavWaiting.Unlock()
	log.Debug().Msgf("navLeft took %dms", time.Since(st).Milliseconds())
	return nil
}

// markDone saves location in the dldir/.lastdone file, to indicate it is the
// most recent item downloaded
func markDone(dldir, location string) error {
	log.Debug().Msgf("Marking %v as done", location)

	oldPath := filepath.Join(dldir, ".lastdone")
	newPath := oldPath + ".bak"
	if err := os.Rename(oldPath, newPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	if err := os.WriteFile(oldPath, []byte(location), 0600); err != nil {
		// restore from backup
		if err := os.Rename(newPath, oldPath); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
		return err
	}
	return nil
}

// requestDownload1 sends the Shift+D event, to start the download of the currently
// viewed item.
func requestDownload1(ctx context.Context) error {
	log.Trace().Msg("Requesting download")
	return pressButton(ctx, "D", input.ModifierShift)
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

	muKbEvents.Lock()
	defer muKbEvents.Unlock()
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
func requestDownload2(ctx context.Context) error {
	muKbEvents.Lock()
	defer muKbEvents.Unlock()
	log.Trace().Msg("Requesting download (alternative method)")
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`[...document.querySelectorAll('[aria-label="More options"]')].pop().click()`, nil),
		chromedp.Sleep(tick),
		chromedp.Click(`[aria-label^="Download"]`, chromedp.ByQuery, chromedp.AtLeast(0)),
		chromedp.Sleep(tick),
	); err != nil {
		return err
	}
	return nil
}

// getPhotoData gets the date from the currently viewed item.
// First we open the info panel by clicking on the "i" icon (aria-label="Open info")
// if it is not already open. Then we read the date from the
// aria-label="Date taken: ?????" field.
func (s *Session) getPhotoData(ctx context.Context, imageId string, cancel chan bool) (time.Time, string, error) {
	var filename string
	var dateStr string
	var timeStr string
	var tzStr string
	timeout := time.NewTimer(40 * time.Second)
	log.Debug().Str("imageId", imageId).Msg("Extracting photo date text and original file name")

	// check if element [aria-label^="Date taken:"] is visible, if not click i button
	var n = 0
	for {
		n++
		var filenameNodes []*cdp.Node
		var dateNodes []*cdp.Node
		var timeNodes []*cdp.Node
		var tzNodes []*cdp.Node

		if err := chromedp.Run(ctx,
			chromedp.Nodes(`[aria-label^="Filename:"]`, &filenameNodes, chromedp.ByQuery, chromedp.AtLeast(0)),
			chromedp.Nodes(`[aria-label^="Date taken:"]`, &dateNodes, chromedp.ByQuery, chromedp.AtLeast(0)),
			chromedp.Nodes(`[aria-label^="Date taken:"] + div [aria-label^="Time taken:"]`, &timeNodes, chromedp.ByQuery, chromedp.AtLeast(0)),
			chromedp.Nodes(`[aria-label^="Date taken:"] + div [aria-label^="GMT"]`, &tzNodes, chromedp.ByQuery, chromedp.AtLeast(0)),
		); err != nil {
			return time.Time{}, "", err
		}

		if len(dateNodes) > 0 && len(timeNodes) > 0 && len(filenameNodes) > 0 {
			filename = strings.TrimPrefix(filenameNodes[0].AttributeValue("aria-label"), "Filename: ")
			dateStr = strings.TrimPrefix(dateNodes[0].AttributeValue("aria-label"), "Date taken: ")
			timeStr = strings.TrimPrefix(timeNodes[0].AttributeValue("aria-label"), "Time taken: ")
			if len(tzNodes) > 0 {
				tzStr = tzNodes[0].AttributeValue("aria-label")
			} else {
				// If timezone is not visible, use current timezone (parse date to account for DST)
				t, err := time.Parse("Jan 2, 2006", dateStr)
				if err != nil {
					t = time.Now()
				}
				_, offset := t.Zone()
				tzStr = fmt.Sprintf("%+03d%02d", offset/3600, (offset%3600)/60)
			}
			break
		}

		log.Info().Msg("Date not visible, clicking on i button")
		if err := func() error {
			muKbEvents.Lock()
			defer muKbEvents.Unlock()
			return chromedp.Run(ctx,
				chromedp.Click(`[aria-label="Open info"]`, chromedp.ByQuery, chromedp.AtLeast(0)),
			)
		}(); err != nil {
			return time.Time{}, "", err
		}
		select {
		case <-cancel:
			return time.Time{}, "", nil
		case <-timeout.C:
			return time.Time{}, "", fmt.Errorf("timeout waiting for date to appear for %v (see error.png)", imageId)
		case <-time.After(time.Duration(n) * tick):
		}
	}

	var datetimeStr = strings.Map(func(r rune) rune {
		if r >= 32 && r <= 126 {
			return r
		}
		return -1
	}, dateStr+" "+timeStr) + " " + strings.Map(func(r rune) rune {
		if (r >= '0' && r <= '9') || r == '+' || r == '-' {
			return r
		}
		return -1
	}, tzStr)
	date, err := time.Parse("Jan 2, 2006 Mon, 3:04PM Z0700", datetimeStr)
	if err != nil {
		return time.Time{}, "", err
	}
	log.Debug().Str("imageId", imageId).Msgf("Found date: %v and original filename: %v ", date, filename)
	return date, filename, nil
}

// download starts the download of the currently viewed item, and on successful
// completion saves its location as the most recent item downloaded. It returns
// with an error if the download stops making any progress for more than a minute.
func (s *Session) download(ctx context.Context, location string) (string, error) {
	st := time.Now()

	if err := requestDownload2(ctx); err != nil {
		return "", err
	}

	timeout1 := time.NewTimer(30 * time.Second)
	timeout2 := time.NewTimer(60 * time.Second)
	deadline := time.Now().Add(time.Minute)

	var filename string
	started := false
	var fileSize int64
	for {
		// Checking for gphotos warning that this video can't be downloaded (no known solution)
		// This check only works for requestDownload2 method (not requestDownload1)
		var nodes []*cdp.Node
		if err := chromedp.Nodes(`[aria-label="Video is still processing & can be downloaded later"] button`, &nodes, chromedp.ByQuery, chromedp.AtLeast(0)).Do(ctx); err != nil {
			return "", err
		}
		if len(nodes) > 0 {
			log.Warn().Msg("Received 'Video is still processing error', skipping for now")
			// Click the button to close the warning, otherwise it will block navigating to the next photo
			muKbEvents.Lock()
			defer muKbEvents.Unlock()
			if err := chromedp.MouseClickNode(nodes[0]).Do(ctx); err != nil {
				return "", err
			}
			return "", errStillProcessing
		}

		// This check only works for requestDownload1 method (not requestDownload2)
		var res bool
		if err := chromedp.Evaluate("document.body.innerHTML.indexOf('Video is still processing &amp; can be downloaded later') != -1", &res).Do(ctx); err != nil {
			return "", err
		}
		if res {
			log.Warn().Msg("Received 'Video is still processing error', skipping for now")
			return "", errStillProcessing
		}

		time.Sleep(tick)
		if !started {
			select {
			case <-timeout1.C:
				if err := requestDownload1(ctx); err != nil {
					return "", err
				}
			case <-timeout2.C:
				return "", fmt.Errorf("timeout waiting for download to start for %v", location)
			default:
			}
		}

		if started && time.Now().After(deadline) {
			return "", fmt.Errorf("hit deadline while downloading in %q", s.dlDirTmp)
		}

		entries, err := os.ReadDir(s.dlDirTmp)
		if err != nil {
			return "", err
		}
		var fileEntries []os.FileInfo
		for _, v := range entries {
			if v.IsDir() {
				continue
			}
			info, err := v.Info()
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				} else {
					return "", err
				}
			}
			fileEntries = append(fileEntries, info)
		}
		if len(fileEntries) < 1 {
			continue
		}
		if len(fileEntries) > 1 {
			return "", fmt.Errorf("more than one file (%d) in download dir: %v", len(fileEntries), fileEntries)
		}
		if !started {
			if len(fileEntries) > 0 {
				started = true
				deadline = time.Now().Add(time.Minute)
			}
		}
		newFileSize := fileEntries[0].Size()
		if newFileSize > fileSize {
			// push back the timeout as long as we make progress
			deadline = time.Now().Add(time.Minute)
			fileSize = newFileSize
		}
		if !strings.HasSuffix(fileEntries[0].Name(), ".crdownload") {
			// download is over
			filename = fileEntries[0].Name()
			break
		}
	}

	log.Debug().Msgf("Download took %v", time.Since(st))

	if err := markDone(s.dlDir, location); err != nil {
		return "", err
	}

	return filename, nil
}

func imageIdFromUrl(location string) (string, error) {
	parts := strings.Split(location, "/")
	if len(parts) < 5 {
		return "", fmt.Errorf("not enough slash separated parts in location %v: %d", location, len(parts))
	}
	return parts[4], nil
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

// dlAndMove creates a directory in s.dlDir named of the item ID found in
// location. It then moves dlFile in that directory. It returns the new path
// of the moved file.
func (s *Session) dlAndMove(ctx context.Context, location string, originalFilenameChan chan string, getPhotoDataErr chan error) ([]string, error) {
	dlFile, err := s.download(ctx, location)
	if err != nil {
		dlScreenshot(ctx, filepath.Join(s.dlDir, "error"))
		return []string{""}, err
	}

	outDir, err := s.makeOutDir(location)
	if err != nil {
		return []string{""}, err
	}

	var filename string
	if dlFile != "download" && dlFile != "" {
		filename = dlFile
	} else {
		select {
		case err := <-getPhotoDataErr:
			return []string{""}, err
		case filename = <-originalFilenameChan:
		}
	}

	if strings.HasSuffix(filename, ".zip") {
		filepaths, err := s.handleZip(filepath.Join(s.dlDirTmp, dlFile), outDir)
		if err != nil {
			return []string{""}, err
		}
		return filepaths, nil
	} else {
		newFile := filepath.Join(outDir, filename)
		log.Debug().Msgf("Moving %v to %v", dlFile, newFile)
		if err := os.Rename(filepath.Join(s.dlDirTmp, dlFile), newFile); err != nil {
			return []string{""}, err
		}
		return []string{newFile}, nil
	}
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

var (
	muNavWaiting             sync.RWMutex
	muKbEvents               sync.Mutex
	listenEvents, navWaiting = false, false
	navDone                  = make(chan bool, 1)
)

func listenNavEvents(ctx context.Context) {
	chromedp.ListenTarget(ctx, func(ev interface{}) {
		muNavWaiting.RLock()
		listen := listenEvents
		muNavWaiting.RUnlock()
		if !listen {
			return
		}
		switch ev.(type) {
		case *page.EventNavigatedWithinDocument:
			go func() {
				for {
					muNavWaiting.RLock()
					waiting := navWaiting
					muNavWaiting.RUnlock()
					if waiting {
						navDone <- true
						break
					}
					time.Sleep(tick)
				}
			}()
		}
	})
}

// navN successively downloads the currently viewed item, and navigates to the
// next item (to the left). It repeats N times or until the last (i.e. the most
// recent) item is reached. Set a negative N to repeat until the end is reached.
func (s *Session) navN(N int) func(context.Context) error {
	return func(ctx context.Context) error {
		n := 0
		if N == 0 {
			return nil
		}

		listenNavEvents(ctx)

		var location, prevLocation string
		for {
			if err := chromedp.Location(&location).Do(ctx); err != nil {
				return err
			}
			if location == prevLocation {
				break
			}
			prevLocation = location

			imageId, err := imageIdFromUrl(location)
			if err != nil {
				return err
			}
			entries, err := os.ReadDir(filepath.Join(s.dlDir, imageId))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if len(entries) == 0 {
				originalFilenameChan := make(chan string, 1)
				dateChan := make(chan time.Time, 1)
				getPhotoDataErr := make(chan error, 1)
				cancel := make(chan bool, 1)
				go func() {
					date, originalFilename, err := s.getPhotoData(ctx, imageId, cancel)
					if err != nil {
						getPhotoDataErr <- err
					}
					dateChan <- date
					originalFilenameChan <- originalFilename
				}()

				// Local dir doesn't exist or is empty, continue downloading
				filePaths, err := s.dlAndMove(ctx, location, originalFilenameChan, getPhotoDataErr)
				if err == errStillProcessing {
					log.Warn().Msg("Video is still processing & can be downloaded later, skipping for now")
					// write location to file to be downloaded later (append to .skipped)
					f, err := os.OpenFile(filepath.Join(s.dlDir, ".skipped"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
					if err != nil {
						return err
					}
					if _, err := f.WriteString(location + "\n"); err != nil {
						return err
					}
					cancel <- true // cancel getPhotoData
				} else {
					if err != nil {
						return err
					}
					var date time.Time
					select {
					case err := <-getPhotoDataErr:
						return err
					case date = <-dateChan:
					}
					if err := s.doFileDateUpdate(ctx, date, filePaths); err != nil {
						return err
					}
					for _, f := range filePaths {
						if err := doRun(f); err != nil {
							return err
						}
					}
				}
			} else {
				log.Debug().Msgf("Skipping %v, file already exists in download dir", imageId)
			}

			if err := markDone(s.dlDir, location); err != nil {
				return err
			}
			n++
			if N > 0 && n >= N {
				break
			}
			if strings.HasSuffix(location, s.firstItem) {
				break
			}

			if err := navLeft(ctx); err != nil {
				return fmt.Errorf("error at %v: %v", location, err)
			}
		}
		return nil
	}
}

// doFileDateUpdate updates the file date of the downloaded files to the photo date
func (s *Session) doFileDateUpdate(_ context.Context, date time.Time, filePaths []string) error {
	if !*fileDateFlag {
		return nil
	}

	log.Debug().Msgf("Setting file date for %v", filePaths)

	for _, f := range filePaths {
		if err := s.setFileDate(f, date); err != nil {
			return err
		}
		log.Info().Msgf("downloaded %v with date %v", filepath.Base(f), date.Format(time.DateOnly))
	}

	return nil
}

// Sets modified date of dlFile to given date
func (s *Session) setFileDate(dlFile string, date time.Time) error {
	if err := os.Chtimes(dlFile, date, date); err != nil {
		return err
	}
	return nil
}
