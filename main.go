//
//  DEBUG 2014/06/04 10:08:12.581479 page.go:58: http://www.moko.cc/xiaoajiao/ Content-Type: text/html;charset=UTF-8
//
package main

import (
	"config"
	"domain"
	"encoding/json"
	"feed"
	"flag"
	"log"
	"logger"
	"net/http"
	"os"
	"page"
	"queue"
	"scheduler"
	"storage"
)

var (
	storeSqlite     = flag.String("store.sqlite", "/Users/nothingness/sp", "Directory to store SQLite files")
	storeMongo      = flag.String("store.mongo", "", "Connection string to mongodb store - host:port/db")
	storeMongoShard = flag.Bool("store.mongo.shard", false, "Shard new mongo collections")
	storeMysql      = flag.String("store.mysql", "", "Connection string to mongodb store - user:pass@host:port/db")
	queueBeanstalk  = flag.String("queue.beanstalk", "", "Connection string to beanstalkd queue - host:port")
	queueMongo      = flag.String("queue.mongo", "", "Connection string to mongodb queue - host:port/db")
	queueMongoShard = flag.Bool("queue.mongo.shard", false, "Shard new mongo collections")
	once            = flag.Bool("once", true, "Only crawl sites once, then stop")
	listen          = flag.String("listen", ":8084", "Address:port to listen for HTTP requests")
	printConf       = flag.Bool("printconfig", false, "Print configuration from store and exit")
	rssOnly         = flag.Bool("rssonly", false, "Only run the web interface for RSS exports (don't spider)")
)

func Print_obj(obj interface{}, str string) {
	enc := json.NewEncoder(os.Stdout)
	enc.Encode(str)
	if err := enc.Encode(obj); err != nil {
		logger.Error.Fatalf("%s Error encoding config: %s", str, err)
	}

}

func init() {
	logger.Debug = log.New(os.Stdout, "  DEBUG ", logger.DefaultFlags)
	logger.Error = log.New(os.Stderr, "  ERROR ", logger.DefaultFlags)
	logger.Info = log.New(os.Stdout, "   INFO ", logger.DefaultFlags)
	logger.Trace = log.New(os.Stdout, "  TRACE ", logger.DefaultFlags)
	logger.Warn = log.New(os.Stdout, "   WARN ", logger.DefaultFlags)
}

func main() {
	flag.Parse()
	var err error

	// Set up storage backend
	var store storage.Storage
	switch {
	case *storeMysql != "":
		if store, err = storage.NewMySQL(*storeMysql); err != nil {
			logger.Error.Fatal(err)
		}
	case *storeMongo != "":
		//if store, err = storage.NewMongo(*storeMongo, *storeMongoShard); err != nil {
		//	logger.Error.Fatal(err)
		//}
	case *storeSqlite != "":
		if store, err = storage.NewSqlite(*storeSqlite); err != nil {
			logger.Error.Fatal(err)
		}
	default:
		store, _ = storage.NewMemory()
	}
	/*
		defaultconfig := new(config.Config)
		defaultconfig.Domains = make([]domain.Domain, 0, 128)

		defaultconfig.Domains = append(defaultconfig.Domains, domain.Domain{URL: "http://www.moko.cc/"})
		store.SaveConfig(defaultconfig)
	*/
	if *printConf {
		c := new(config.Config)
		if err := store.GetConfig(c); err != nil {
			logger.Error.Fatalf("Error getting config: %s", err)
		}
		Print_obj(c, "")
		return
	}

	// Set up queue backend
	var q queue.Queue
	switch {
	case *queueBeanstalk != "":
		if q, err = queue.NewBeanstalk(*queueBeanstalk); err != nil {
			logger.Error.Fatal(err)
		}
	case *queueMongo != "":
		//if q, err = queue.NewMongo(*queueMongo, *queueMongoShard); err != nil {
		//	logger.Error.Fatal(err)
		//}
	default:
		q = queue.NewMemory(1024)
	}

	//http监控
	http.Handle("/rss/", feed.New(store))
	go func() {
		if err := http.ListenAndServe(*listen, nil); err != nil {
			logger.Error.Fatal(err)
		}
	}()

	if *rssOnly {
		select {}
	}

	//初始化调度
	sch, err := scheduler.New(q, store)
	if err != nil {
		logger.Error.Fatal(err)
	}

	if *once {
		sch.Once() //设置once
	}

	//初始化队列
	p, d := new(page.Page), new(domain.Domain)
	for sch.Next() {
		//从调度中，获取要下载的地址
		if err := sch.Cur(d, p); err != nil {
			logger.Error.Fatal(err)
		}
		//logger.Debug.Printf("Processing: %s", p.URL)
		/*
			if err := d.CanDownload(p); err != nil {
				logger.Warn.Printf("Cannot download %s: %s", p.URL, err)
				continue
			}
		*/
		switch err := p.Download(); err {
		case nil:
			if err := p.SetTitle(); err != nil {
				logger.Warn.Printf("Error setting title: %s", err)
			}
			sch.Update(p, "update") //更新
		case page.ErrNotModified:
			logger.Warn.Printf("Not modified: %s", p.URL)
			//sch.Update(p) //更新采集时间
			//continue
			sch.Update(p, "update")
		default:
			//logger.Error.Printf("Error downloading: %s", err)
			continue
		}

		links, err := p.Links()
		if err != nil {
			logger.Error.Fatal(err)
		}
		for i := range links {
			l := page.New(links[i])
			/*
				if err := d.CanDownload(l); err != nil {
					continue
				}
			*/
			//logger.Warn.Printf("Link: %s", links[i])
			//是否在数据库中
			if store.GetPage(links[i], new(page.Page)) != storage.ErrNotFound {
				//logger.Warn.Printf("Already downloaded %s", links[i])
				continue
			}
			//如果不在队列,则添加到队列
			if err := sch.Add(links[i]); err != nil {
				//logger.Warn.Printf("Error adding %s: %s", links[i], err)
				continue
			}
			//添加
			if err := sch.Update(l, "insert"); err != nil {
				//logger.Warn.Printf("Error updating %s: %s", links[i], err)
				continue
			}

			//logger.Trace.Printf("New Link: %s", links[i])
		}
	}

	if err := sch.Err(); err != nil {
		logger.Error.Fatal(err)
	}
}
