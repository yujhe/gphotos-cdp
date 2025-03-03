package main

import "fmt"

type NodeLabelMatch struct {
	matchType  string
	matchValue string
}

type GPhotosLocale struct {
	selectAllPhotosLabel            NodeLabelMatch
	fileNameLabel                   NodeLabelMatch
	dateLabel                       NodeLabelMatch
	today                           string
	yesterday                       string
	timeLabel                       NodeLabelMatch
	tzLabel                         NodeLabelMatch
	viewPreviousPhotoMatch          NodeLabelMatch
	moreOptionsLabel                NodeLabelMatch
	downloadLabel                   NodeLabelMatch
	downloadOriginalLabel           NodeLabelMatch
	openInfoMatch                   NodeLabelMatch
	videoStillProcessingDialogLabel NodeLabelMatch
	videoStillProcessingStatusText  string
	noWebpageFoundText              string
	shortDayNames                   []string
}

var locales map[string]GPhotosLocale = make(map[string]GPhotosLocale)

func initLocales() {
	locales["en"] = GPhotosLocale{
		selectAllPhotosLabel:            NodeLabelMatch{"startsWith", "Select all photos from"},
		fileNameLabel:                   NodeLabelMatch{"startsWith", "Filename:"},
		dateLabel:                       NodeLabelMatch{"startsWith", "Date taken:"},
		today:                           "Today",
		yesterday:                       "Yesterday",
		timeLabel:                       NodeLabelMatch{"startsWith", "Time taken:"},
		tzLabel:                         NodeLabelMatch{"startsWith", "GMT"},
		viewPreviousPhotoMatch:          NodeLabelMatch{"equals", "View previous photo"},
		moreOptionsLabel:                NodeLabelMatch{"equals", "More options"},
		downloadLabel:                   NodeLabelMatch{"equals", "Download - Shift+D"},
		downloadOriginalLabel:           NodeLabelMatch{"equals", "Download original"},
		openInfoMatch:                   NodeLabelMatch{"equals", "Open info"},
		videoStillProcessingDialogLabel: NodeLabelMatch{"startsWith", "Video still is processing"},
		videoStillProcessingStatusText:  "Video is still processing &amp; can be downloaded later",
		noWebpageFoundText:              "No webpage was found for the web address:",
		shortDayNames:                   []string{"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"},
	}
}

func getAriaLabelSelector(matcher NodeLabelMatch) string {
	eq := "="
	if matcher.matchType == "equals" {
		eq = "="
	} else if matcher.matchType == "startsWith" {
		eq = "^="
	} else if matcher.matchType == "contains" {
		eq = "*="
	} else if matcher.matchType == "endsWith" {
		eq = "$="
	}
	return fmt.Sprintf("[aria-label%s\"%s\"]", eq, matcher.matchValue)
}
