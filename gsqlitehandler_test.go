// Written 2016 by Marcin 'Zbroju' Zbroinski.
// Use of this source code is governed by a GNU General Public License
// that can be found in the LICENSE file.
package gsqlitehandler

import (
	_ "github.com/mattn/go-sqlite3"
	"os"
	"testing"
)

const (
	testDBFile = "testdb.sqlite"
)

func TestCreateNewFile(t *testing.T) {
	dbProperties := map[string]string{"applicationName": "gBicLog", "databaseVersion": "0.1"}
	sqlString := "CREATE TABLE tmp_table (id INTEGER PRIMARY KEY, name TEXT);"
	testdb := New(testDBFile, dbProperties)
	err := testdb.CreateNew(sqlString)
	if err != nil {
		t.Errorf("%s", err)
	}
	defer os.Remove(testDBFile)

	// Test if a file was created
	if _, err := os.Stat(testDBFile); os.IsNotExist(err) {
		t.Errorf("Test file not created at all.")
	}

	// Open file with the same properties
	err = testdb.Open()
	if err != nil {
		t.Errorf("%s", err)
	}
	testdb.Close()

	// Open file with different properties
	dbProperties["additional"] = "temporary"
	testdb = New(testDBFile, dbProperties)
	err = testdb.Open()
	if err == nil {
		t.Errorf("%s", err)
	}
	testdb.Close()

}
