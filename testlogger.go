package monet

type writer struct {
	Msg string
}

func (w *writer) Clear() {
	w.Msg = ""
	return
}

func (w *writer) Write(p []byte) (n int, err error) {
	w.Msg += string(p)
	n, err = len(p), nil
	return
}