package main

import (
	//"fmt"
	//"time"
	"math/rand"
	"net/http"
	"io"
	//"os"
	"strconv"
	"log"
	"time"
	"github.com/gorilla/mux"
)

var InstanceId string

func JSONResponseFromString(w http.ResponseWriter, res string) {
        w.Header().Set("Content-Type", "application/json; charset=UTF-8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, res)
}

func HealthTest(w http.ResponseWriter, r *http.Request) {
        JSONResponseFromString(w, "{\"alive\":\"yup\", \"instanceId\":\""+InstanceId+"\"}")
}


func main() {

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	InstanceId = strconv.Itoa(r1.Intn(99999))

	
	r := mux.NewRouter()

	r.HandleFunc("/healthcheck", HealthTest).Methods("GET")

	if err := http.ListenAndServe(":6666", r); err != nil {
                log.Fatal(err)
        }

}
