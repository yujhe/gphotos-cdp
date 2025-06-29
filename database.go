package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type PhotoRow struct {
	ImageID    string
	Filename   string       // Photo filename
	Url        string       // Google Photos URL
	Date       sql.NullTime // Photo original creation date
	DownloadAt sql.NullTime
}

type Database struct {
	db *sql.DB
}

func NewDatabaseIfNotExist(dbPath string) (*Database, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared", dbPath))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	// Create the table if it doesn't exist
	createTableSQL := `CREATE TABLE IF NOT EXISTS photos (
		image_id TEXT,
		filename TEXT,
		url TEXT PRIMARY KEY,
		date DATETIME DEFAULT NULL,
		download_at DATETIME DEFAULT NILL
	);`
	if _, err := db.Exec(createTableSQL); err != nil {
		return nil, err
	}

	// Create the index if it doesn't exist
	createIndexSQL := `CREATE INDEX IF NOT EXISTS idx_photos_date ON photos (date);`
	if _, err := db.Exec(createIndexSQL); err != nil {
		return nil, err
	}

	return &Database{db: db}, nil
}

func (d *Database) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *Database) InsertPhotoIfNotExists(photo PhotoRow) error {
	// insert new photo to database if not exist
	stmt := `
        INSERT OR IGNORE INTO photos
        (image_id, filename, url, date, download_at)
        VALUES (?, ?, ?, ?, ?)
    `
	_, err := d.db.Exec(stmt, photo.ImageID, photo.Filename, photo.Url, photo.Date, photo.DownloadAt)
	return err
}

func (d *Database) IsPhotoDownloaded(url string) (bool, error) {
	// Check if the photo is already downloaded
	stmt := `SELECT 1 FROM photos WHERE url = ? AND download_at IS NOT NULL`
	var exists int
	err := d.db.QueryRow(stmt, url).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}

	return true, nil
}

func (d *Database) MarkPhotoAsDownloaded(url string) error {
	// Update the download_at field for the photo
	stmt := `
		UPDATE photos
		SET download_at = CURRENT_TIMESTAMP
		WHERE url = ?
	`
	_, err := d.db.Exec(stmt, url)
	return err
}

func (d *Database) GetDownloadedPhotoUrlsSince(from time.Time) ([]string, error) {
	// Get all downloaded photo URLs since the specified date
	stmt := `
		SELECT url
		FROM photos
		WHERE download_at IS NOT NULL
			AND "date" >= ?
	`
	rows, err := d.db.Query(stmt, from)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var urls []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return nil, err
		}
		urls = append(urls, url)
	}

	return urls, nil
}
