package logging

type TaskLogger interface {
	INFO(format string, v ...any)
	ERROR(format string, v ...any)
	WARN(format string, v ...any)
	LOG(format, system string, level Level, v ...any)

	//Write is used by Singer
	Write(p []byte) (n int, err error)
}
