package main

import (
	"flag"
	"fmt"
	"github.com/tarantool/go-tarantool"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

/* ----- */

type Flags struct {
	conc   int
	total  int
	keylen int
	silent bool
}

func usage() {
	fmt.Printf("Usage %s subs ping change\n", filepath.Base(os.Args[0]))
	os.Exit(1)
}

type WorkerFunc func(*tarantool.Connection, int, int, *Progress)

type Progress struct {
	total int32
	count int32

	last_since time.Time
	last_count int32
}

func NewProgress(total int) *Progress {
	return &Progress{total: int32(total)}
}

func (p *Progress) increment() {
	new_count := atomic.AddInt32(&p.count, 1)

	if new_count == 1 {
		p.last_since = time.Now()
		p.last_count = 0
	} else if (new_count % (p.total / 10)) == 0 {
		now := time.Now()
		elapsed := now.Sub(p.last_since).Seconds()
		rps := float64(new_count-p.last_count) / elapsed
		p.last_since = now
		p.last_count = new_count
		log.Printf("Completed %6d requests, %9.2f rps\n", new_count, rps)
	}
}

/* ----- */

var LIST_DEVS []string = nil
var LIST_SUBS []string = nil
var LIST_MUTEX sync.Mutex

/* ----- */

func runFuncSubs(client *tarantool.Connection, keylen int, numreq int, p *Progress) {

	// Create and save devices and subscriptions
	list_devs := make([]string, 0, numreq)
	list_subs := make([]string, 0, numreq)

	model := NewPushDbModel(client)

	for i := 0; i < numreq; i++ {
		sub_id := genRandomString(keylen)
		dev_id := genRandomString(keylen)
		auth := genRandomString(AUTH_STRING_LEN)
		push_token := genPushToken()
		now := milliTime()

		// Model
		t_dev, code, err := model.doCreateDev(dev_id, auth, push_token, PUSH_TECH_GCM, now)
		if t_dev == nil || code != RES_OK || err != nil {
			log.Fatalf("Error calling function: %s", err.Error())
		}

		// Model
		code, err = model.doCreateSub(dev_id, sub_id, now)
		if code != RES_OK || err != nil {
			log.Fatalf("Error calling function: %s", err.Error())
		}

		p.increment()

		list_devs = append(list_devs, dev_id)
		list_subs = append(list_subs, sub_id)
	}

	// Add more subscriptions to the devices
	for i := 0; i < numreq; i++ {
		dev_id := list_devs[i]

		// Model
		sub_id := genRandomString(keylen)
		now := milliTime()
		code, err := model.doCreateSub(dev_id, sub_id, now)
		if code != RES_OK || err != nil {
			log.Fatalf("Error calling function: %d, %s", code, err.Error())
		}
		p.increment()

		// Model
		sub_id = genRandomString(keylen)
		now = milliTime()
		code, err = model.doCreateSub(dev_id, sub_id, now)
		if code != RES_OK || err != nil {
			log.Fatalf("Error calling function: %d, %s", code, err.Error())
		}
		p.increment()
	}

	LIST_MUTEX.Lock()

	if LIST_DEVS == nil {
		LIST_DEVS = make([]string, 0, numreq)
	}
	if LIST_SUBS == nil {
		LIST_SUBS = make([]string, 0, 3*numreq)
	}

	for _, dev_id := range list_devs {
		LIST_DEVS = append(LIST_DEVS, dev_id)
	}
	for _, sub_id := range list_subs {
		LIST_SUBS = append(LIST_SUBS, sub_id)
	}

	LIST_MUTEX.Unlock()
}

func runFuncPing(client *tarantool.Connection, keylen int, numreq int, p *Progress) {
	list_devs := LIST_DEVS
	list_subs := LIST_SUBS

	model := NewPushDbModel(client)

	for i := 0; i < numreq; i++ {
		sub_id := list_subs[i]
		dev_id := list_devs[i]

		ping_ts := milliTime()

		// Model
		code, err := model.doPingChangeSub(dev_id, sub_id, false, ping_ts)
		if code != RES_OK || err != nil {
			log.Fatalf("Error calling function: %d, %s", code, err.Error())
		}

		p.increment()
	}
}

func runFuncChange(client *tarantool.Connection, keylen int, numreq int, p *Progress) {
	list_devs := LIST_DEVS
	list_subs := LIST_SUBS

	model := NewPushDbModel(client)

	for i := 0; i < numreq; i++ {
		sub_id := list_subs[i]
		dev_id := list_devs[i]

		change_ts := milliTime()

		// Model
		code, err := model.doPingChangeSub(dev_id, sub_id, true, change_ts)
		if code != RES_OK || err != nil {
			log.Fatalf("Error calling function: %d, %s", code, err.Error())
		}

		p.increment()
	}
}

func runHarness(flags Flags, client *tarantool.Connection, worker WorkerFunc, requireLists bool) {
	rand.Seed(time.Now().UTC().UnixNano())

	if requireLists && LIST_SUBS == nil {
		log.Fatal("Please run subs first")
	}

	log.Printf("Key length: %d\n", flags.keylen)

	var wg sync.WaitGroup
	wg.Add(flags.conc)

	now := time.Now()
	progress := NewProgress(flags.total)

	for i := 0; i < flags.conc; i++ {
		numreq := flags.total / flags.conc
		keylen := flags.keylen
		go func(keylen, numreq int) {
			defer wg.Done()
			worker(client, keylen, numreq, progress)
		}(keylen, numreq)
	}

	wg.Wait()

	since := time.Since(now)

	log.Printf("Elapsed time: %s\n", since)
	log.Printf("Ops per second: %.2f\n", float64(progress.count)/since.Seconds())
}

func main() {
	// Flags
	var flags Flags
	flag.IntVar(&flags.conc, "c", 20, "Concurrency")
	flag.IntVar(&flags.total, "n", 100000, "Total count")
	flag.IntVar(&flags.keylen, "l", 40, "Key length")
	flag.BoolVar(&flags.silent, "s", false, "Silent")
	flag.Parse()

	nargs := flag.NArg()
	args := flag.Args()

	if nargs < 1 {
		usage()
	}
	if flags.conc < 1 || flags.total < 1 || flags.keylen < 10 {
		usage()
	}

	// Config
	config, err := NewPushConfig()
	if err != nil {
		fmt.Printf("Fatal config error: %s\n", err)
		os.Exit(1)
	}

	// Database connection, need only one
	var client *tarantool.Connection

	// Run commands
	for _, command := range args {
		// Connect if needed
		if client == nil {
			client_init, err := config.Db.Connect(config.Ews.PushServerConfig)
			if err != nil {
				log.Fatalf("Failed to connect: %s", err.Error())
			}

			client = client_init
		}

		if command == "subs" {
			log.Printf("Subs test, c = %d, n = %d\n", flags.conc, flags.total)
			runHarness(flags, client, runFuncSubs, false)
		} else if command == "ping" {
			log.Printf("Ping test, c = %d, n = %d\n", flags.conc, flags.total)
			runHarness(flags, client, runFuncPing, true)
		} else if command == "change" {
			log.Printf("Change test, c = %d, n = %d\n", flags.conc, flags.total)
			runHarness(flags, client, runFuncChange, true)
		} else {
			usage()
		}
	}
}
