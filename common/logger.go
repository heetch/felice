// The common package holds types and variables that are common to
// multiple parts of Felice, and generically useful.
package common

import (
	"io/ioutil"
	"log"
)

// Logger is used by all Felice packages for logging.  By default it
// is bound to ioutil.Discard, which will keep it blissfully quite.
// Should you wish to see logging output, you can set it to any
// logging implementation that matches the StdLogger interface.
var Logger StdLogger = log.New(ioutil.Discard, "[Felice] ", log.LstdFlags)

// StdLogger is the interface you need to implement on whatever logger
// you use for Logger.  Go's own built in log package is fine, but you
// could use logrus or apex.Log or wrap something incompatible that
// you like!
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
