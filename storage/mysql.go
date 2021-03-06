package storage

import (
	"config"
	"database/sql"
	"domain"
	"page"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQL struct {
	db      *sql.DB
	ensured map[string]bool
}

var _ Storage = new(MySQL)

func NewMySQL(dsn string) (s *MySQL, err error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return
	}
	s = &MySQL{
		db:      db,
		ensured: make(map[string]bool),
	}
	return
}

func (s *MySQL) Close() error {
	return s.db.Close()
}

func (s *MySQL) GetConfig(c *config.Config) (err error) {
	if err = s.ensureTable("config"); err != nil {
		return
	}

	rows, err := s.db.Query(`SELECT domain, name, delay, redl FROM domains`)
	if err != nil {
		return
	}

	c.Domains = make([]domain.Domain, 0, 128)
	var delay, redl int64
	var subrows *sql.Rows
	var str string
	for rows.Next() {
		d := domain.Domain{
			Exclude:     make([]string, 0, 8),
			Include:     make([]string, 0, 8),
			StartPoints: make([]string, 0, 8),
		}
		if err = rows.Scan(&d.URL, &d.Name, &delay, &redl); err != nil {
			return
		}
		d.Delay = time.Duration(delay)
		d.Redownload = time.Duration(redl)

		// Regex rules
		for typ, f := range map[string]*[]string{
			"exclude": &d.Exclude,
			"include": &d.Include,
		} {
			subrows, err = s.db.Query(`SELECT rule FROM regexes WHERE type = ? AND domain = ?`, typ, d.Domain())
			if err != nil {
				return
			}
			for subrows.Next() {
				if err = subrows.Scan(&str); err != nil {
					return
				}
				*f = append(*f, str)
			}
			if err = subrows.Err(); err != nil {
				return
			}
		}

		// Start Points
		subrows, err = s.db.Query(`SELECT path FROM start_points WHERE domain = ?`, d.Domain())
		if err != nil {
			return
		}
		for subrows.Next() {
			if err = subrows.Scan(&str); err != nil {
				return
			}
			d.StartPoints = append(d.StartPoints, str)
		}
		if err = subrows.Err(); err != nil {
			return
		}

		c.Domains = append(c.Domains, d)
	}

	return rows.Err()
}

func (s *MySQL) GetPage(url string, p *page.Page) (err error) {
	p.URL = url
	if err = s.ensureTable(p.Domain()); err != nil {
		return
	}

	var firstDownload, lastDownload, lastModified int64
	err = s.db.QueryRow(
		`
			SELECT
				url, first_download, last_download, last_modified, checksum
			FROM pages
			WHERE url = ?
			LIMIT 1
		`,
		url,
	).Scan(
		&p.URL,
		&firstDownload,
		&lastDownload,
		&lastModified,
		&p.Checksum,
	)
	if err == sql.ErrNoRows {
		p.URL = ""
		err = ErrNotFound
	}
	p.FirstDownload = time.Unix(0, firstDownload)
	p.LastDownload = time.Unix(0, lastDownload)
	p.LastModified = time.Unix(0, lastModified)
	return
}

func (s *MySQL) GetPages(domain, key string, pages *[]*page.Page) (err error) {
	if err = s.ensureTable("exports"); err != nil {
		return
	}

	// Find the last page ID downloaded
	var lastPageId uint64
	err = s.db.QueryRow(
		`
			SELECT last_page_id
			FROM exports
			WHERE domain = ?
				AND export_key = ?
			ORDER BY id DESC
			LIMIT 1
		`,
		domain,
		key,
	).Scan(&lastPageId)

	// If no previous export, pull the stuff starting from midnight, yesterday
	if err == sql.ErrNoRows {
		err = s.db.QueryRow(
			`
				SELECT MIN(id)
				FROM pages
				WHERE
					first_download > UNIX_TIMESTAMP(CURDATE()) * 1e9
					AND domain = ?
			`,
			domain,
		).Scan(&lastPageId)
	}

	if err != nil {
		return
	}

	ps := *pages
	defer func() { *pages = ps }()

	// Fetch chunk of pages from last ID downloaded to now; reverse order so
	// feed always shows most recent -> oldest
	rows, err := s.db.Query(
		`
			SELECT *
			FROM (
				SELECT id, url, first_download, last_download, last_modified, checksum
				FROM pages
				WHERE id > ?
					AND domain = ?
				ORDER BY id ASC
				LIMIT ?
			) AS p
			ORDER BY id DESC
		`,
		lastPageId,
		domain,
		cap(ps),
	)
	if err != nil {
		return
	}

	var id uint64
	var firstDownload, lastDownload, lastModified int64
	ps = ps[:]

	for rows.Next() {
		var p page.Page
		err = rows.Scan(
			&id,
			&p.URL,
			&firstDownload,
			&lastDownload,
			&lastModified,
			&p.Checksum,
		)
		if err != nil {
			return
		}
		p.FirstDownload = time.Unix(0, firstDownload)
		p.LastDownload = time.Unix(0, lastDownload)
		p.LastModified = time.Unix(0, lastModified)
		if lastPageId < id {
			lastPageId = id
		}
		ps = append(ps, &p)
	}

	if key != "" {
		_, err = s.db.Exec(
			`
				INSERT INTO exports
					(last_page_id, domain, export_key, exported)
				VALUES
					(?,            ?,      ?,          ?        )
			`,
			lastPageId,
			domain,
			key,
		)
	}
	return
}

func (s *MySQL) SaveConfig(c *config.Config) (err error) {
	if err = s.ensureTable("config"); err != nil {
		return
	}

	// Mark everything for deletion
	if _, err = s.db.Exec(`UPDATE domains SET del = 1`); err != nil {
		return
	}

	// Update data
	reStmt, err := s.db.Prepare(`INSERT IGNORE INTO regexes (domain, type, rule) VALUES (?,?,?)`)
	if err != nil {
		return
	}
	defer reStmt.Close()

	spStmt, err := s.db.Prepare(`INSERT IGNORE INTO start_points (domain, path) VALUES (?,?)`)
	if err != nil {
		return
	}
	defer spStmt.Close()

	for _, d := range c.Domains {
		domain := d.GetURL().Scheme + "://" + d.GetURL().Host

		_, err = s.db.Exec(
			`INSERT INTO domains
				(domain, name, delay, redl, del)
			VALUES
				(?,      ?,    ?,     ?,    0  )
			ON DUPLICATE KEY UPDATE
				name  = ?,
				delay = ?,
				redl  = ?,
				del   = 0
			`,
			// INSERT
			domain,
			d.Name,
			d.Delay.Nanoseconds(),
			d.Redownload.Nanoseconds(),
			// ON DUPLICATE KEY UPDATE
			d.Name,
			d.Delay.Nanoseconds(),
			d.Redownload.Nanoseconds(),
		)
		if err != nil {
			return
		}

		// Domain Exclusion Rules
		if _, err = s.db.Exec(`DELETE FROM regexes WHERE domain = ?`, domain); err != nil {
			return
		}
		for _, re := range d.Include {
			if _, err = reStmt.Exec(domain, "include", re); err != nil {
				return
			}
		}
		for _, re := range d.Exclude {
			if _, err = reStmt.Exec(domain, "exclude", re); err != nil {
				return
			}
		}

		// Domain Start Points
		if _, err = s.db.Exec(`DELETE FROM start_points WHERE domain = ?`, domain); err != nil {
			return
		}
		for _, sp := range d.StartPoints {
			if _, err = spStmt.Exec(domain, sp); err != nil {
				return
			}
		}
	}

	// Delete previously marked rows
	if _, err = s.db.Exec(`DELETE FROM domains WHERE del = 1`); err != nil {
		return
	}

	return
}

func (s *MySQL) SavePage(p *page.Page) (err error) {
	if err = s.ensureTable(p.Domain()); err != nil {
		return
	}

	_, err = s.db.Exec(
		`
			INSERT INTO pages
				(url, domain, first_download, last_download, last_modified, checksum)
			VALUES
				(?,   ?,      ?,               ?,             ?,             ?       )
			ON DUPLICATE KEY UPDATE
				first_download = ?,
				last_download  = ?,
				last_modified  = IF(checksum = ?, last_modified, ?),
				checksum       = ?
		`,
		// INSERT INTO
		p.URL,
		p.Domain(),
		p.FirstDownload.UnixNano(),
		p.LastDownload.UnixNano(),
		time.Now().UnixNano(),
		p.Checksum,
		// ON DUPLICATE KEY UPDATE
		p.FirstDownload.UnixNano(),
		p.LastDownload.UnixNano(),
		p.Checksum, time.Now().UnixNano(),
		p.Checksum,
	)
	return
}

func (s *MySQL) UpdatePage(p *page.Page) (err error) {
	if err = s.ensureTable(p.Domain()); err != nil {
		return
	}

	_, err = s.db.Exec(
		`
			INSERT INTO pages
				(url, domain, first_download, last_download, last_modified, checksum)
			VALUES
				(?,   ?,      ?,               ?,             ?,             ?       )
			ON DUPLICATE KEY UPDATE
				first_download = ?,
				last_download  = ?,
				last_modified  = IF(checksum = ?, last_modified, ?),
				checksum       = ?
		`,
		// INSERT INTO
		p.URL,
		p.Domain(),
		p.FirstDownload.UnixNano(),
		p.LastDownload.UnixNano(),
		time.Now().UnixNano(),
		p.Checksum,
		// ON DUPLICATE KEY UPDATE
		p.FirstDownload.UnixNano(),
		p.LastDownload.UnixNano(),
		p.Checksum, time.Now().UnixNano(),
		p.Checksum,
	)
	return

}

func (s *MySQL) ensureTable(name string) (err error) {
	if name == "" {
		return ErrNotFound
	}

	if s.ensured[name] {
		return
	}
	s.ensured[name] = true

	switch name {
	case "config":
		return s.configTables()
	case "exports":
		return s.exportsTable()
	default:
		return s.domainTable(name)
	}
	return
}

func (s *MySQL) configTables() (err error) {
	creates := []string{
		`CREATE TABLE IF NOT EXISTS domains (
			domain VARCHAR(255) NOT NULL PRIMARY KEY,
			name   VARCHAR(255) NOT NULL,
			delay  BIGINT,
			redl   BIGINT,
			del    TINYINT DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS regexes (
			domain VARCHAR(255) NOT NULL,
			type   VARCHAR(32) NOT NULL,
			rule   VARCHAR(255) NOT NULL,
			UNIQUE(domain, type, rule)
		)`,
		`CREATE TABLE IF NOT EXISTS start_points (
			domain VARCHAR(255) NOT NULL,
			path   VARCHAR(255) NOT NULL,
			UNIQUE(domain, path)
		)`,
	}
	for _, create := range creates {
		if _, err = s.db.Exec(create); err != nil {
			return
		}
	}
	return
}

func (s *MySQL) domainTable(name string) (err error) {
	creates := []string{
		`CREATE TABLE IF NOT EXISTS pages (
			id             BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
			domain         VARCHAR(255) NOT NULL,
			url            VARCHAR(255) NOT NULL,
			first_download BIGINT NOT NULL,
			last_download  BIGINT NOT NULL,
			last_modified  BIGINT NOT NULL,
			checksum       INT UNSIGNED NOT NULL,
			INDEX(domain),
			UNIQUE(url)
		)`,
	}
	for _, create := range creates {
		if _, err = s.db.Exec(create); err != nil {
			return
		}
	}
	return
}

func (s *MySQL) exportsTable() (err error) {
	creates := []string{
		`CREATE TABLE IF NOT EXISTS exports (
			id             BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
			domain         VARCHAR(255) NOT NULL,
			export_key     VARCHAR(255) NOT NULL,
			exported       INT NOT NULL DEFAULT 0,
			last_page_id   BIGINT UNSIGNED NOT NULL,
			added          TIMESTAMP NOT NULL,
			INDEX(export_key)
		)`,
	}
	for _, create := range creates {
		if _, err = s.db.Exec(create); err != nil {
			return
		}
	}
	return
}
