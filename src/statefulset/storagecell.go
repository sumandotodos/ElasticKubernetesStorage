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

type KeyStore struct {
	freememory int
	storage map[string] string
}

func (s *KeyStore) Initialize() {
	s.freememory = Memory
	s.storage = make(map[string] string)
}

func (s *KeyStore) Store(key string, value string) bool {
	charsNeeded := len(value)
	if(s.freememory >= charsNeeded) {
		s.storage[key] = value
		s.freememory -= len(value)
		return true
	}
	return false
}

func (s *KeyStore) Retrieve(key string) (bool, string) {
	if value, exists := s.storage[key]; exists {	
		return true, value
	} else {
		return false, ""
	}
}

func (s *KeyStore) Delete(key string) bool {
	if _, exists := s.storage[key]; exists {
		s.freememory += len(s.storage[key])
		delete(s.storage, key)
		return true
	} else {
		return false
	}
}

var keyStore* KeyStore



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

func ReportCellInfo(w http.ResponseWriter, r *http.Request) {
	JSONResponseFromString(w, "{\"available\":"+strconv.Itoa(keyStore.freememory)+"}")
}

func StoreItem(w http.ResponseWriter, r *http.Request) {
	// thou shalt not store the item unless it fits
	// but it's the controller's responsibility to decide 
	// and enforce that

	vars := mux.Vars(r)
        fmt.Println("Attempting to store value " + vars["id"] + " in key " + vars["info"])
        success := keyStore.Store(vars["id"], vars["info"])
        if(success) {
                JSONResponseFromString(w, "{\"result\":\"'success'\"}")
        } else {
                JSONResponseFromString(w, "{\"result\":\"not ok\"}")
        }
}

func RetrieveItem(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
        fmt.Println("Attempting to retrieve value " + vars["info"])
        success, value := keyStore.Retrieve(vars["id"])
        if(success) {
                JSONResponseFromString(w, "{\"result\":\"OK\", \"value\":"+value+"}")
        } else {
                JSONResponseFromString(w, "{\"result\":\"not found\"}, \"value\":\"\"}")
        }
}

func DeleteItem(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
        status := keyStore.Delete(vars["id"])
        if(status) {
                JSONResponseFromString(w, "{\"result\":\"success\"}")
        } else {
                JSONResponseFromString(w, "{\"result\":\"not ok\"}")
        }
}

func UpdateItem(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["id"]
	if status, value := keyStore.Retrieve(key); status {
		keyStore.Store(key, value)
		JSONResponseFromString(w, "{\"result\":\"key not found\"}")
	} else {
		JSONResponseFromString(w, "{\"result\":\"key not found\"}")
	}
}

func main() {
	StoragePort := "7777"
	keyStore = new(KeyStore)
	keyStore.Initialize()
	r := mux.NewRouter()
	r.HandleFunc("/cellinfo", ReportCellInfo).Methods("GET")
	r.HandleFunc("/healthcheck", HealthCheck).Methods("GET")
	r.HandleFunc("/{id}/{info}", StoreItem).Methods("POST")
	r.HandleFunc("/{id}/{info}", DeleteItem).Methods("DELETE")
	r.HandleFunc("{id}/{info}", UpdateItem).Methods("PUT")
	r.HandleFunc("/{id}/{info}", RetrieveItem).Methods("GET")
	fmt.Println("Storage cell started at port " + StoragePort)
	if err := http.ListenAndServe(":" + StoragePort, r); err != nil {
                log.Fatal(err)
        }
}