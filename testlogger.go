package monet

type Writer struct {
	Msg string
}

func (w *Writer) Clear() {
	w.Msg = ""
	return
}

func (w *Writer) Write(p []byte) (n int, err error) {
	w.Msg += string(p)
	n, err = len(p), nil
	return
}
