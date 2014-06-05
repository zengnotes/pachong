package download

import (
	//"net/http"
	//"net/http/cookiejar"
	"bytes"
	curl "github.com/zengnotes/go-curl"
	"logger"
)

const (
	//BotName   = ""
	//BotURL    = "http://www.baidu.com"
	UserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.137 Safari/537.36" //BotName + " (+" + BotURL + ")"
)

/*
//var client = new(http.Client)

func init() {
	// Turns out cookiejar.New() returns a nil error
	//client.Jar, _ = cookiejar.New(nil)
}

func Do(req *http.Request) (resp *http.Response, err error) {
	req.Header.Add("User-Agent", UserAgent)
	return client.Do(req)
}
*/
func Get(url string) (bodybuf []byte, err error) {
	/*
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return
		}
		return Do(req)
	*/
	easy := curl.EasyInit()
	defer easy.Cleanup()
	//bodybuf = []byte{}
	easy.Setopt(curl.OPT_URL, url)
	easy.Setopt(curl.OPT_TIMEOUT, 5)
	fooTest := func(buf []byte, userdata interface{}) bool {
		var tempbuf bytes.Buffer
		tempbuf.Write(bodybuf)
		tempbuf.Write(buf)
		bodybuf = tempbuf.Bytes()
		return true
	}
	easy.Setopt(curl.OPT_COOKIEJAR, "./cookie.jar")
	easy.Setopt(curl.OPT_WRITEFUNCTION, fooTest)
	if err := easy.Perform(); err != nil {
		logger.Error.Printf("%s Download error: %s", url, err)
	}
	return
}
