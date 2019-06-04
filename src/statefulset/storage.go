package main

import (
//	"os"
	"log"
	"strconv"
	"io"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

const Memory = 100

type status struct {
	freememory int
	storage map[string] string
}

func (s *status) Initialize() {
	s.freememory = Memory
	s.storage = make(map[string] string)
}

func (s *status) Store(key string, value string) bool {
	charsNeeded := len(value)
	if(s.freememory >= charsNeeded) {
		s.storage[key] = value
		s.freememory -= len(value)
		return true
	}
	return false
}

func (s *status) Retrieve(key string) (bool, string) {
	if value, exists := s.storage[key]; exists {	
		return true, value
	} else {
		return false, ""
	}
}

func (s *status) Delete(key string) bool {
	if _, exists := s.storage[key]; exists {
		s.freememory += len(s.storage[key])
		delete(s.storage, key)
		return true
	} else {
		return false
	}
}

var serverStatus* status



// Utilities

func JSONResponseFromString(w http.ResponseWriter, res string) {
        w.Header().Set("Content-Type", "application/json; charset=UTF-8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, res)
}




// REST API Handlers
func HealthCheck(w http.ResponseWriter, r *http.Request) {
        fmt.Fprintln(w, "{'status':'alive'}")
}

func StoreKey(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fmt.Println("Attempting to store value " + vars["value"] + " in key " + vars["key"])
	success := serverStatus.Store(vars["key"], vars["value"])
	if(success) {
		JSONResponseFromString(w, "{\"result\":\"'OK'\"}")
	} else {
		JSONResponseFromString(w, "{\"result\":\"Not OK\"}")
	}
}

func RetrieveKey(w http.ResponseWriter, r *http.Request) {
        vars := mux.Vars(r)
        fmt.Println("Attempting to retrieve value " + vars["value"])
        success, value := serverStatus.Retrieve(vars["key"])
        if(success) {
                JSONResponseFromString(w, "{\"result\":\"OK\", \"value\":"+value+"}")
        } else {
                JSONResponseFromString(w, "{\"result\":\"Not OK\"}, \"value\":\"\"}")
        }
}

func DeleteKey(w http.ResponseWriter, r *http.Request) {
        vars := mux.Vars(r)
	status := serverStatus.Delete(vars["key"])
	if(status) {
		JSONResponseFromString(w, "{\"result\":\"OK\"}")
	} else {
		JSONResponseFromString(w, "{\"result\":\"Not OK\"}")
	}
}

func ReportFreeSpace(w http.ResponseWriter, r *http.Request) {
	JSONResponseFromString(w, "{\"available\":"+strconv.Itoa(serverStatus.freememory)+"}")
}

func main() {
	StoragePort := "7777"
	serverStatus = new(status)
	serverStatus.Initialize()
	r := mux.NewRouter()
	r.HandleFunc("/healthcheck", HealthCheck).Methods("GET")
	r.HandleFunc("/{key}/{value}", StoreKey).Methods("PUT")
	r.HandleFunc("/{key}", DeleteKey).Methods("DELETE")
	r.HandleFunc("/freespace", ReportFreeSpace).Methods("GET")
	r.HandleFunc("/{key}", RetrieveKey).Methods("GET")
	fmt.Println("Storage cell started at port " + StoragePort)
	if err := http.ListenAndServe(":" + StoragePort, r); err != nil {
                log.Fatal(err)
        }
}
