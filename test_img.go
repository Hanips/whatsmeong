package main
import (
	"fmt"
	"net/http"
	"io"
)
func main() {
	resp, err := http.Get("https://upload.wikimedia.org/wikipedia/commons/thumb/a/a7/React-icon.svg/512px-React-icon.svg.png")
	if err != nil { fmt.Println(err); return }
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	fmt.Println("Status:", resp.StatusCode)
	fmt.Println("Mimetype:", http.DetectContentType(b))
}
