package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

type Value struct {
	Data       string
	Expires_at int64
}

type Request struct {
	Key  string `json:"key"`
	Data string `json:"data"`
	TTL  int64  `json:"ttl"`
}

type Response struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Data    string `json:"data,omitempty"`
}

type LogEntry struct {
	Op    string // "SET" or "DELETE"
	Key   string
	Value string // only for SET
	Exp   int64  // expiry timestamp (absolute), only for SET
}

var (
	m     = make(map[string]Value)
	mu    sync.RWMutex
	walCh = make(chan LogEntry, 100) // Write-Ahead Log channel
)

func backgroundCleanup() {
	for {
		valuesToDelete := make([]string, 0)

		mu.RLock()
		fmt.Println("[INFO] Running background cleanup...")
		currTimestamp := time.Now().Unix()
		for key, value := range m {
			if currTimestamp > value.Expires_at {
				valuesToDelete = append(valuesToDelete, key)
			}
		}
		mu.RUnlock()

		mu.Lock()
		for _, key := range valuesToDelete {
			fmt.Println("Deleted expired key:", key)
			delete(m, key)
		}
		mu.Unlock()
		time.Sleep(30 * time.Second)
	}
}

func WalWriter(filename string) {
	f, _ := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

	writer := bufio.NewWriter(f) // buffered writer improves performance

	for entry := range walCh {
		var line string
		if entry.Op == "SET" {
			line = fmt.Sprintf("SET %s %s %d\n", entry.Key, entry.Value, entry.Exp)
		} else if entry.Op == "DELETE" {
			line = fmt.Sprintf("DELETE %s\n", entry.Key)
		}
		// Write to buffer
		_, err := writer.WriteString(line)
		if err != nil {
			fmt.Println("[ERROR] WAL write failed:", err)
			continue
		}

		// Flush buffer to file
		writer.Flush()

		// Force flush to disk
		err = f.Sync()
		if err != nil {
			fmt.Println("[ERROR] WAL fsync failed:", err)
		}
	}
}

func replayWAL(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		var entry LogEntry
		if _, err := fmt.Sscanf(line, "SET %s %s %d", &entry.Key, &entry.Value, &entry.Exp); err == nil {
			entry.Op = "SET"
			mu.Lock()
			m[entry.Key] = Value{
				Data:       entry.Value,
				Expires_at: entry.Exp,
			}
			mu.Unlock()
		} else if _, err := fmt.Sscanf(line, "DELETE %s", &entry.Key); err == nil {
			entry.Op = "DELETE"
			mu.Lock()
			delete(m, entry.Key)
			mu.Unlock()
		}
	}
}

func runCompaction() error {
	mu.RLock()
	defer mu.RUnlock()

	waitForWALDrained()

	tmpFile := "kv.wal.new"
	f, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("could not create temp WAL: %v", err)
	}
	defer f.Close()

	for key, val := range m {
		line := fmt.Sprintf("SET %s %s %d\n", key, val.Data, val.Expires_at)
		_, err := f.WriteString(line)
		if err != nil {
			return fmt.Errorf("write failed: %v", err)
		}
	}

	err = os.Rename(tmpFile, "kv.wal")
	if err != nil {
		return fmt.Errorf("rename failed: %v", err)
	}

	fmt.Println("[INFO] WAL compacted successfully")
	return nil
}

func waitForWALDrained() {
	for len(walCh) > 0 {
		time.Sleep(5 * time.Millisecond)
	}
}

func main() {
	replayWAL("kv.wal")
	go backgroundCleanup()
	go WalWriter("kv.wal")

	go func() {
		for {
			time.Sleep(10 * time.Minute) // ⏰ or 1hr, your call
			fmt.Println("[INFO] Running auto compaction...")
			err := runCompaction()
			if err != nil {
				fmt.Println("[ERROR] Compaction failed:", err)
			}
		}
	}()

	r := mux.NewRouter()
	r.HandleFunc("/kv/{key}", handlerGet).Methods("GET")
	r.HandleFunc("/kv", handlerSet).Methods("POST")
	r.HandleFunc("/kv/{key}", handlerDelete).Methods("DELETE")

	fmt.Println("Server is running on port 8080...")
	http.ListenAndServe(":8080", r)
}

func handlerGet(w http.ResponseWriter, r *http.Request) {

	mu.RLock()
	currTimestamp := time.Now().Unix()
	key := mux.Vars(r)["key"]
	val, exists := m[key]
	w.Header().Set("Content-Type", "application/json")
	if exists {
		if currTimestamp > val.Expires_at {
			mu.RUnlock()
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(Response{Status: "Invalid", Message: "Value has expired"})
			return
		}
		mu.RUnlock()
		json.NewEncoder(w).Encode(Response{Status: "Found", Message: "Value from map", Data: val.Data})
		return
	}
	mu.RUnlock()
	w.WriteHeader(http.StatusNotFound)
	json.NewEncoder(w).Encode(Response{Status: "Not Found", Message: "Key does not exist"})
}

func handlerSet(w http.ResponseWriter, r *http.Request) {

	mu.Lock()
	var req Request
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(Response{Status: "Error", Message: "Invalid JSON"})
		mu.Unlock()
		return
	}

	expiry := time.Now().Unix() + req.TTL
	walCh <- LogEntry{Op: "SET", Key: req.Key, Value: req.Data, Exp: expiry}
	key := req.Key
	m[key] = Value{
		Data:       req.Data,
		Expires_at: expiry,
	}
	mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Status: "OK", Message: "Value set successfully"})
}

func handlerDelete(w http.ResponseWriter, r *http.Request) {

	mu.Lock()
	key := mux.Vars(r)["key"]
	walCh <- LogEntry{Op: "DELETE", Key: key}
	delete(m, key)
	mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Response{Status: "OK", Message: "Value deleted successfully"})
}
