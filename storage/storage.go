package storage

import (
	"config"
	"errors"
	"page"
)

type Storage interface {
	Close() error
	GetPage(url string, p *page.Page) error
	GetPages(domain, key string, pages *[]*page.Page) error
	SavePage(p *page.Page) error
	UpdatePage(p *page.Page) error
	GetConfig(c *config.Config) error
	SaveConfig(c *config.Config) error
}

var ErrNotFound = errors.New("Not found")
