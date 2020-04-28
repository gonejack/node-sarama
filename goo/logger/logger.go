package logger

type Logger interface {
	Printf(format string, v ...interface{})
	Fatalf(format string, v ...interface{})
}
