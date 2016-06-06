// Written 2016 by Marcin 'Zbroju' Zbroinski.
// Use of this source code is governed by a GNU General Public License
// that can be found in the LICENSE file.
// Version 2.0.0

package gsqlitehandler

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
)

// Error messages
const (
	errFileNotExists       = "file does not exist"
	errFileAlreadyExists   = "file already exists"
	errFileCannotBeCreated = "file cannot be created"
	errFileCannotBeOpen    = "file cannot be open"
	errFileNotAppDB        = "given file is not a valid file"
)

// SqliteDB struct is the basic wrapper type for sqlite object.
type SqliteDB struct {
	Path       string
	Properties map[string]string
	Handler    *sql.DB
}

// isCorrectDB compares tags from table PROPERTIES with the SqliteDB Properties.
// Returns true if the tags in table PROPERTIES are equal to Sqlite Properties, or false otherwise.
func (d *SqliteDB) isCorrectDB() bool {
	rows, err := d.Handler.Query("SELECT KEY, VALUE FROM PROPERTIES;")
	if err != nil {
		return false
	}
	defer rows.Close()
	var tagCounter int
	for rows.Next() {
		var key, value string
		err = rows.Scan(&key, &value)
		if err != nil {
			return false
		}
		if d.Properties[key] != "" && d.Properties[key] != value {
			return false
		}
		tagCounter++
	}
	if len(d.Properties) != tagCounter {
		return false
	}

	return true
}

// New initialize a new SqliteDB object with given file path (dbPath) and signature (properties).
func (d *SqliteDB) Init(dbPath string, properties map[string]string) {
	d.Path = dbPath
	d.Properties = properties
}

// CreateNew creates tables from the given SQL code (sqlCreateTablesStmt) and PROPERTIES table.
// PROPERTIES table is populated with SqliteDB Properties key-value pair(s).
func (d *SqliteDB) CreateNew(sqlCreateTablesStmt string) error {
	// Check if file exist and if so - return error
	if _, err := os.Stat(d.Path); !os.IsNotExist(err) {
		return errors.New(errFileAlreadyExists)
	}

	// Open file
	var fileErr error
	d.Handler, fileErr = sql.Open("sqlite3", d.Path)
	if fileErr != nil {
		return errors.New(errFileCannotBeCreated)
	}
	defer d.Handler.Close()

	// Create tables
	sqlStmt := fmt.Sprintf("BEGIN TRANSACTION;CREATE TABLE properties (key TEXT, value TEXT);%sCOMMIT;", sqlCreateTablesStmt)
	_, err := d.Handler.Exec(sqlStmt)
	if err != nil {
		os.Remove(d.Path)
		return errors.New(errFileCannotBeCreated)
	}

	// Insert properties values
	tx, err := d.Handler.Begin()
	if err != nil {
		os.Remove(d.Path)
		return errors.New(errFileCannotBeCreated)
	}
	stmt, err := tx.Prepare("INSERT INTO properties VALUES (?,?);")
	if err != nil {
		os.Remove(d.Path)
		return errors.New(errFileCannotBeCreated)
	}
	defer stmt.Close()
	for key, value := range d.Properties {
		_, err := stmt.Exec(key, value)
		if err != nil {
			tx.Rollback()
			os.Remove(d.Path)
			return errors.New(errFileCannotBeCreated)
		}
	}
	tx.Commit()

	// Return nil for error
	return nil
}

// Open tries to open the SqliteDB for given Path and checks if the database has the same tags (PROPERTIES) as
// current SqliteDB Properties. If so, returns true. Otherwise it returns false.
func (d *SqliteDB) Open() error {
	var fileErr error

	// Test if a file exist
	if _, err := os.Stat(d.Path); os.IsNotExist(err) {
		return errors.New(errFileNotExists)
	}

	d.Handler, fileErr = sql.Open("sqlite3", d.Path)
	if fileErr != nil {
		return errors.New(errFileCannotBeOpen)
	}
	if d.isCorrectDB() == false {
		return errors.New(errFileNotAppDB)
	} else {
		return nil
	}
}

// Close zeroes Path and Properties and eventually closes Handler.
func (d *SqliteDB) Close() {
	d.Path = ""
	d.Properties = make(map[string]string)
	d.Handler.Close()
}
