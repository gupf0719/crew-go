package common

type Error interface {
	Error() (string, error)
}
