package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// --- Hold flag ---

var (
	holdMu  sync.RWMutex
	holding bool
	// holdGeneration is incremented every time hold is activated.
	// Each paused connection captures the generation at pause time.
	// When /unhold is called, the generation is incremented so that
	// only connections paused in the *current* generation are released;
	// connections paused in a previous generation keep waiting.
	holdGeneration uint64
)

// --- Failure mode flag ---

var (
	failureMu     sync.RWMutex
	failureModeOn bool
)

// isMetadataQuery returns true if the query is the mysql-connector-j initial metadata SELECT.
func isMetadataQuery(query string) bool {
	return strings.HasPrefix(query, "/* mysql-connector-j") && strings.Contains(query, "@@session.auto_increment_increment")
}

// holdIfNeeded blocks the caller if the hold flag is active.
// It captures the current hold generation at the moment it starts waiting.
// If /unhold is called (which bumps the generation) the caller will NOT be
// released — it will keep waiting until the hold expires naturally or until
// a subsequent /hold+/unhold cycle that matches its generation is no longer
// needed.  New connections after /unhold see holding==false and return
// immediately.
func holdIfNeeded(label string) {
	// Snapshot the generation we entered the hold under.
	holdMu.RLock()
	h := holding
	enteredGeneration := holdGeneration
	holdMu.RUnlock()

	if !h {
		return
	}

	log.Printf("Hold active [%s] generation=%d — waiting...", label, enteredGeneration)

	for {
		time.Sleep(500 * time.Millisecond)

		holdMu.RLock()
		currentHolding := holding
		currentGeneration := holdGeneration
		holdMu.RUnlock()

		if !currentHolding {
			// Hold was lifted — but only resume if the generation has NOT
			// changed (i.e. the hold was lifted by expiry or by /unhold
			// without a new /hold in between for a different cycle).
			// Actually the correct semantic requested is:
			//   /unhold only affects NEW connections; paused ones stay paused.
			// So we NEVER release a paused connection via /unhold.
			// We only release when the 60-minute timer fires (same generation).
			if currentGeneration == enteredGeneration {
				// Timer-based expiry: same generation, hold lifted → release.
				log.Printf("Hold lifted (timer) [%s] — resuming", label)
				return
			}
			// Generation changed means a new hold cycle started and ended;
			// we were paused in an older cycle and should stay paused.
			// Wait for our own generation's timer (it won't fire because
			// holding is false now for the new gen, but the goroutine that
			// manages our gen's timer will set holding=false with our gen).
			// In practice: stay in the loop — we'll be held until the
			// 60-minute goroutine for our generation fires.
		}
		// If currentHolding==true and generation matches, keep waiting.
		// If currentHolding==true and generation differs, keep waiting (new hold cycle).
	}
}

// isFailureModeOn returns true if the failure mode flag is active.
func isFailureModeOn() bool {
	failureMu.RLock()
	defer failureMu.RUnlock()
	return failureModeOn
}

// --- Active connection registry ---

var (
	connMu      sync.Mutex
	activeConns = make(map[uint64]net.Conn)
	nextConnID  uint64
)

func registerConn(c net.Conn) uint64 {
	connMu.Lock()
	defer connMu.Unlock()
	nextConnID++
	id := nextConnID
	activeConns[id] = c
	return id
}

func unregisterConn(id uint64) {
	connMu.Lock()
	defer connMu.Unlock()
	delete(activeConns, id)
}

func invalidateAllConns() int {
	connMu.Lock()
	defer connMu.Unlock()
	count := len(activeConns)
	for id, c := range activeConns {
		log.Printf("Invalidating connection %d (%s)", id, c.RemoteAddr())
		c.Close()
	}
	activeConns = make(map[uint64]net.Conn)
	return count
}

func invalidateNConns(n int) int {
	connMu.Lock()
	defer connMu.Unlock()
	count := 0
	for id, c := range activeConns {
		if count >= n {
			break
		}
		log.Printf("Invalidating connection %d (%s)", id, c.RemoteAddr())
		c.Close()
		delete(activeConns, id)
		count++
	}
	return count
}

func invalidateOneConn() bool {
	connMu.Lock()
	defer connMu.Unlock()
	for id, c := range activeConns {
		log.Printf("Invalidating connection %d (%s)", id, c.RemoteAddr())
		c.Close()
		delete(activeConns, id)
		return true
	}
	return false
}

// --- HTTP control server ---

func startHTTPServer() {
	// POST /hold — enable hold: block all incoming connections for 60 minutes then release
	http.HandleFunc("/hold", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		holdMu.Lock()
		if holding {
			holdMu.Unlock()
			w.WriteHeader(http.StatusConflict)
			fmt.Fprintln(w, "hold already active")
			return
		}
		holding = true
		holdGeneration++
		myGeneration := holdGeneration
		holdMu.Unlock()

		log.Printf("Hold triggered (generation=%d) — connections will be held for 60 minutes", myGeneration)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "hold triggered: connections will be held for 60 minutes")

		go func(gen uint64) {
			time.Sleep(60 * time.Minute)
			holdMu.Lock()
			// Only clear the hold flag if the generation hasn't moved on.
			if holdGeneration == gen && holding {
				holding = false
				log.Printf("Hold period over (generation=%d, 60 min elapsed) — new connections resuming normally", gen)
			}
			holdMu.Unlock()
		}(myGeneration)
	})

	// POST /unhold — disable hold for NEW connections only.
	// Connections that are already paused inside holdIfNeeded will remain
	// paused because they captured a generation that is now stale.
	http.HandleFunc("/unhold", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		holdMu.Lock()
		// Bump the generation so that paused goroutines (which snapshotted
		// the old generation) will never match the "same generation + !holding"
		// condition that triggers a release.
		holdGeneration++
		holding = false
		holdMu.Unlock()

		log.Println("Hold released manually — new connections will proceed normally; already-paused connections remain paused")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "hold released: new connections proceed normally; already-paused connections remain paused")
	})

	// POST /invalidate-all — close all active connections at once to force Agroal to reconnect
	http.HandleFunc("/invalidate-all", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		count := invalidateAllConns()
		log.Printf("Invalidated all %d active connection(s)", count)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "invalidated all %d active connection(s)\n", count)
	})

	// POST /invalidate-n — close a provided number of active connections
	http.HandleFunc("/invalidate-n", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		nStr := r.URL.Query().Get("count")
		if nStr == "" {
			http.Error(w, "missing required query parameter: count", http.StatusBadRequest)
			return
		}

		n, err := strconv.Atoi(nStr)
		if err != nil || n <= 0 {
			http.Error(w, "count must be a positive integer", http.StatusBadRequest)
			return
		}

		count := invalidateNConns(n)
		log.Printf("Invalidated %d connection(s) (requested: %d)", count, n)
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "invalidated %d connection(s) (requested: %d)\n", count, n)
	})

	// POST /invalidate — close one random active connection to force Agroal to reconnect
	http.HandleFunc("/invalidate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if invalidateOneConn() {
			log.Println("Invalidated one random connection")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "invalidated one random connection")
		} else {
			log.Println("No active connections to invalidate")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintln(w, "no active connections to invalidate")
		}
	})

	// POST /failure — enable failure mode: all SQL queries will return an error
	http.HandleFunc("/failure", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		failureMu.Lock()
		if failureModeOn {
			failureMu.Unlock()
			w.WriteHeader(http.StatusConflict)
			fmt.Fprintln(w, "failure mode already active")
			return
		}
		failureModeOn = true
		failureMu.Unlock()

		log.Println("Failure mode enabled — all SQL queries will return an error")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "failure mode enabled: all SQL queries will return an error")
	})

	// POST /unfailure — disable failure mode, return to normal state
	http.HandleFunc("/unfailure", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		failureMu.Lock()
		failureModeOn = false
		failureMu.Unlock()

		log.Println("Failure mode disabled — SQL queries will be executed normally")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "failure mode disabled: SQL queries will be executed normally")
	})

	log.Println("HTTP control server listening on :8081")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}

// --- Proxy handler ---

type ProxyHandler struct {
	server.EmptyHandler
	backendConn *client.Conn
}

func NewProxyHandler(addr, user, password, db string) (*ProxyHandler, error) {
	conn, err := client.Connect(addr, user, password, db, func(c *client.Conn) error {
		c.SetTLSConfig(nil)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to backend MySQL: %w", err)
	}
	return &ProxyHandler{backendConn: conn}, nil
}

func (h *ProxyHandler) UseDB(db string) error {
	_, err := h.backendConn.Execute("USE " + db)
	return err
}

func (h *ProxyHandler) HandleQuery(query string) (*mysql.Result, error) {
	log.Printf("HandleQuery: %q", query)

	if isFailureModeOn() {
		log.Printf("Failure mode active — returning error for query: %q", query)
		return nil, fmt.Errorf("ERROR 2003 (HY000): Can't connect to MySQL server: simulated database failure")
	}

	result, err := h.backendConn.Execute(query)
	if err != nil {
		return nil, err
	}

	if isMetadataQuery(query) {
		holdIfNeeded("metadata query")
	}

	return result, nil
}

func (h *ProxyHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	log.Printf("HandleStmtPrepare: %q", query)

	if isFailureModeOn() {
		log.Printf("Failure mode active — returning error for prepared statement: %q", query)
		return 0, 0, nil, fmt.Errorf("ERROR 2003 (HY000): Can't connect to MySQL server: simulated database failure")
	}

	stmt, err := h.backendConn.Prepare(query)
	if err != nil {
		return 0, 0, nil, err
	}

	return stmt.ParamNum(), stmt.ColumnNum(), stmt, nil
}

func (h *ProxyHandler) HandleStmtExecute(ctx interface{}, query string, args []interface{}) (*mysql.Result, error) {
	log.Printf("Executing prepared statement: %s", query)

	if isFailureModeOn() {
		log.Printf("Failure mode active — returning error for prepared statement execution: %q", query)
		return nil, fmt.Errorf("ERROR 2003 (HY000): Can't connect to MySQL server: simulated database failure")
	}

	stmt, ok := ctx.(*client.Stmt)
	if !ok {
		return nil, fmt.Errorf("invalid statement context")
	}

	return stmt.Execute(args...)
}

func (h *ProxyHandler) HandleStmtClose(ctx interface{}) error {
	if stmt, ok := ctx.(*client.Stmt); ok {
		stmt.Close()
	}
	return nil
}

// --- Main ---

func main() {
	backendAddr := "127.0.0.1:3306"
	backendUser := "root"
	backendPassword := ""
	backendDB := "keycloak"

	go startHTTPServer()

	l, err := net.Listen("tcp", "127.0.0.1:4000")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Listening on port 4000, connect with 'mysql -h 127.0.0.1 -P 4000 -u root'")

	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}

		log.Println("Accepted connection")

		go func(c net.Conn) {
			connID := registerConn(c)
			defer unregisterConn(connID)

			handler, err := NewProxyHandler(backendAddr, backendUser, backendPassword, backendDB)
			if err != nil {
				log.Printf("Failed to connect to backend MySQL: %v", err)
				c.Close()
				return
			}

			log.Println("Connected to backend MySQL")

			srv := server.NewServer("8.4.0", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
			conn, err := srv.NewConn(c, "root", "", handler)
			if err != nil {
				log.Println(err)
				return
			}

			log.Println("Registered the connection with the server")

			for {
				if err := conn.HandleCommand(); err != nil {
					log.Println(err)
					return
				}
			}
		}(c)
	}
}
