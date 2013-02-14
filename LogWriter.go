package monet

import (
	"log/syslog"
	"log"
	"io"
	"os"
)

// The logger for the monetdb driver Server instance.
// *syslog.Writer is compliant to this interface.
type LogWriter interface{
	Err(m string)(err error)
	Warning(m string)(err error)
	Info(m string)(err error)
	Debug(m string)(err error)
}

// Logs everything to stderr and can be plugged in via
//	monet.MapiLogger = DebugToStderr
var DebugToStderr LogWriter = New(os.Stderr, "go/monet ", log.LstdFlags, syslog.LOG_DEBUG)

type logger struct{
	logLevel syslog.Priority
	*log.Logger
}

func New(writer io.Writer, prefix string, flag int, logLevel syslog.Priority) LogWriter {
	return &logger{logLevel, log.New(writer, prefix, flag)}
}

func (l *logger)Err(m string)(err error){
	err = l.write(syslog.LOG_ERR, m)
	return
}

func (l *logger)Warning(m string)(err error){
	err = l.write(syslog.LOG_WARNING, m)
	return
}

func (l *logger)Info(m string)(err error){
	err = l.write(syslog.LOG_INFO, m)
	return
}

func (l *logger)Debug(m string)(err error){
	err = l.write(syslog.LOG_DEBUG, m)
	return
}

func (l *logger)write(ll syslog.Priority, m string)(err error){
	if l.logLevel >= ll {
		l.Println(m)
	}
	return
}

type nolog struct{}
func (n *nolog)Debug(m string)(err error){
	return
}
func(n *nolog)Info(m string)(err error){
	return
}
func (n *nolog)Warning(m string)(err error){
	return
}
func (n *nolog)Err(m string)(err error){
	return
}
