package main

import (
	"database/sql"

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
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

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
