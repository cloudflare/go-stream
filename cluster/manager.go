package cluster

import (
	"encoding/json"
	"io/ioutil"
	"logger"
	"net/http"
	"stash.cloudflare.com/go-stream/util/slog"
	"strconv"
	"time"
)

const (
	MAX_ERAS_SAVED = 10
)

/* There should be a separate manager for each service */
type Manager interface {
	GetEra(t time.Time) Era
	GetCurrentEra() Era
}

type StaticManager struct {
	E Era
}

func (c *StaticManager) GetEra(t time.Time) Era {
	return c.E
}

func (c *StaticManager) GetCurrentEra() Era {
	return c.E
}

func NewStaticManager(e Era) *StaticManager {
	return &StaticManager{e}
}

type DynamicBBManager struct {
	Eras        map[time.Time]Era
	ErasAdded   []time.Time
	BBHosts     []string
	CurrentTime time.Time
}

type BBHost struct {
	Disk_free float32
	Load      float32
	Name      string
	Ip        string
	Port      int
}

type BBResult struct {
	Nodes []BBHost
}

func (c *DynamicBBManager) GetEra(t time.Time) Era {
	if ct, ok := c.Eras[t]; ok {
		return ct
	}
	return c.Eras[c.CurrentTime]
}

func (c *DynamicBBManager) GetCurrentEra() Era {
	return c.Eras[c.CurrentTime]
}

func (c *DynamicBBManager) pullLatestEra() (err error) {
	for _, url := range c.BBHosts {
		if resp, err := http.Get(url); err == nil {
			if bbbody, err := ioutil.ReadAll(resp.Body); err == nil {
				// Try parsing this.
				bbr := BBResult{}
				if err := json.Unmarshal(bbbody, &bbr); err == nil {
					ctime := time.Now()

					we := NewWeightedEra()
					for _, node := range bbr.Nodes {
						n := NewWeightedNode(node.Name, node.Ip, strconv.Itoa(node.Port), node.Disk_free, node.Load)
						slog.Logf(logger.Levels.Debug, "Trasport LOG INFO %v", n)
						we.Add(n)
					}

					// Once all the nodes are in for this era, re-weight the Era
					we.NormalizeAndPopulateMap()
					c.Eras[ctime] = we
					c.CurrentTime = ctime
					c.ErasAdded = append(c.ErasAdded, ctime)

					// And Remove any super old eras
					if len(c.ErasAdded) > MAX_ERAS_SAVED {
						delete(c.Eras, c.ErasAdded[0])
						c.ErasAdded = append(c.ErasAdded[:1], c.ErasAdded[2:]...)
					}

					// Once we have hit one BB server with no error, no need to try any others.
					break
				} else {
					slog.Logf(logger.Levels.Error, "Unmarshal Error %v", err)
				}
			} else {
				slog.Logf(logger.Levels.Error, "Read Error %v", err)
			}
		} else {
			slog.Logf(logger.Levels.Error, "Network GET Error %v", err)
		}
	}
	return
}

func (c *DynamicBBManager) keepErasCurrent() {
	for {
		time.Sleep(60 * time.Second)
		slog.Logf(logger.Levels.Debug, "Updating to new era")
		err := c.pullLatestEra()
		if err != nil {
			slog.Logf(logger.Levels.Error, "Cannot get a valid era %v", err)
		}
	}
}

func NewDynamicBBManager(bbHosts []string) *DynamicBBManager {
	dm := DynamicBBManager{make(map[time.Time]Era), make([]time.Time, 0, 0), bbHosts, time.Now()}
	err := dm.pullLatestEra()
	if err != nil || len(dm.Eras) == 0 {
		slog.Fatalf("Cannot get a valid era %v", err)
	}

	// Keep updating with periodic info
	go dm.keepErasCurrent()
	return &dm
}
