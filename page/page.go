package page

import (
	"bytes"
	"download"
	"errors"
	"github.com/PuerkitoBio/goquery"
	"hash/crc32"
	_ "io/ioutil"
	_ "logger"
	"net/url"
	"strings"
	"time"
)

type Page struct {
	URL           string
	Title         string
	Checksum      uint32
	FirstDownload time.Time
	LastDownload  time.Time
	LastModified  time.Time
	url           *url.URL
	data          []byte
}

var ErrNotModified = errors.New("Not modified")

func New(rawurl string) (p *Page) {
	p = &Page{
		URL: rawurl,
	}
	return
}
func (p *Page) GetBody() string {
	return string(p.data)
}
func (p *Page) GetChecksum() uint32 {
	return crc32.ChecksumIEEE(p.data)
}

func (p *Page) Domain() string {
	d := p.GetURL().Host
	if strings.HasPrefix(d, "www.") {
		d = d[4:]
	}
	return d
}

func (p *Page) Download() (err error) {
	now := time.Now()
	//resp, err := download.Get(p.URL)
	//if err != nil {
	//	return
	//}
	//defer resp.Body.Close()

	//contentType := resp.Header.Get("Content-Type")
	//logger.Debug.Printf("%s Content-Type: %s", p.URL, contentType)

	//if p.data, err = ioutil.ReadAll(resp.Body); err != nil {
	//	return
	//}
	p.data, err = download.Get(p.URL)
	//logger.Error.Printf("url: %s , Title: %s ", p.URL, p.Title)
	p.LastDownload = now
	if p.FirstDownload.IsZero() || p.FirstDownload.UnixNano() < 0 {
		p.FirstDownload = now
	}
	sum := p.GetChecksum()
	if sum != p.Checksum {
		p.LastModified = now
		p.Checksum = sum
		return
	}

	return ErrNotModified
}

func (p *Page) GetURL() (u *url.URL) {
	if p.url != nil {
		return p.url
	}
	u, _ = url.Parse(p.URL)
	if u.Path == "" {
		u.Path = "/"
	}
	return
}

func cmpurl(url1 string, url2 string) (re bool, err error) {
	var u1, u2 *url.URL
	//var err error
	re = false
	u1, err = url.Parse(url1)
	if err != nil {
		return
	}
	u2, err = url.Parse(url2)
	if err != nil {
		return
	}
	if u1.Host == u2.Host {
		re = true
	}
	return
}

func (p *Page) Links() (links []string, err error) {
	d, err := goquery.NewDocumentFromReader(bytes.NewReader(p.data))
	if err != nil {
		return
	}

	base := p.GetURL()
	sel := d.Find("a[href]")
	links = make([]string, 0, sel.Length())
	sel.Each(func(i int, s *goquery.Selection) {
		// TODO add check for target attr
		refStr, exists := s.Attr("href")
		if !exists {
			return
		}
		ref, err := url.Parse(refStr)
		if err != nil {
			return
		}
		//是否是域名的下得链接
		link := base.ResolveReference(ref).String()
		//logger.Error.Printf("domain: %s , link: %s ", p.URL, link)
		if re, _ := cmpurl(p.URL, link); re == true {
			links = append(links, link)
		}
	})
	return
}

func (p *Page) SetTitle() (err error) {
	d, err := goquery.NewDocumentFromReader(bytes.NewReader(p.data))
	if err != nil {
		return
	}

	p.Title = d.Find("title").First().Text()
	return
}
