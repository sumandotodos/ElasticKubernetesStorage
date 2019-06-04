package main

import (
	"os"
	"strconv"
	"log"
	"fmt"
	"net/http"
	"context"
	"github.com/gorilla/mux"

	"go.mongodb.org/mongo-driver/bson"
        "go.mongodb.org/mongo-driver/mongo"
        "go.mongodb.org/mongo-driver/mongo/options"

)

var db_svr string
var db_port string

// Types for db documents

type Status struct {
	NumberOfCells  int  `json:"numberofcells"`
	TotalSpace     uint64 `json:"totalspace"`
	CellNamePrefix	string `json:"cellnameprefix"`
}

type CellStatus struct {
	CellId		int `json:"_id"`
	FreeSpace	uint64 `json:"freespace"`
	NumberOfFiles   uint64 `json:"numberoffile"`
}

type Directory struct {
	Collection	string `json:"collection"`
	Path		string `json:"path"`
	CellId		int	`json:"cellid"`
}

type DBConnectionContext struct {
        client	                 *mongo.Client
	serverstatus		 *mongo.Collection
	cellstatus		 *mongo.Collection
}

var dbConnectionContext DBConnectionContext

// DB functions

func getServerStatus(conn *DBConnectionContext) (Status, error) {
	var statusInDB Status
        err := conn.serverstatus.FindOne(context.TODO(), bson.D{{"_id", 0}}).Decode(&statusInDB)
  	return statusInDB, err
}

func initializeServerStatus(conn *DBConnectionContext) error {
	cellcapacity := InitializeNewCell()
	_, err := conn.serverstatus.InsertOne(context.TODO(), bson.D{{"_id", 0}, 
		{"numberofcells", 1}, {"totalspace", cellcapacity}, {"cellnameprefix", "storagecell"}})
	if err != nil {
		return err
	}
	_, err = conn.cellstatus.InsertOne(context.TODO(), bson.D{{"_id", 0},
                {"freespace", cellcapacity}, {"numberoffiles", 0} })
	return err
}

func HealthCheck(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "{'status':'alive'}")
}

func connectToDB() (*mongo.Client, error) {
        clientOptions := options.Client().ApplyURI("mongodb://" + db_svr + ":" + db_port)
        return mongo.Connect(context.TODO(), clientOptions)
}

// Utils

func InitializeNewCell() uint64 {
	return 100
}

// REST API Functions

func Retrieve(w http.ResponseWriter, r *http.Request) {
        vars := mux.Vars(r)
        fmt.Println("Attempting to retrieve value " + vars["value"])
}

func Store(w http.ResponseWriter, r *http.Request) {
        vars := mux.Vars(r)
        fmt.Println("Attempting to retrieve value " + vars["value"])
}

func main() {

	ControllerPort := "2222"
	
	db_svr = os.Getenv("DB_SVR")
	db_port = os.Getenv("DB_PORT")
	if db_svr == "" {
		db_svr = "localhost"
	}
	if db_port == "" {
		db_port = "27017"
	}

	fmt.Println("Trying to connecto to " + db_svr + ":" + db_port + "...")
	client, err := connectToDB()
        err = client.Ping(context.TODO(), nil)

        if err != nil {
                log.Fatal(err)
        }

        fmt.Println("Connected to MongoDB!")

	dbConnectionContext.client = client
	dbConnectionContext.serverstatus = client.Database("service").Collection("serverstatus")
	dbConnectionContext.cellstatus = client.Database("service").Collection("cellstatus")

	stat, staterr := getServerStatus(&dbConnectionContext)

	if staterr != nil {
		_ = initializeServerStatus(&dbConnectionContext)
		stat, _ = getServerStatus(&dbConnectionContext)
	}

	fmt.Println("Number of cells: " + strconv.Itoa(stat.NumberOfCells))

	r := mux.NewRouter()
	r.HandleFunc("/healthcheck", HealthCheck).Methods("GET")
	r.HandleFunc("/{key}/{value}", Store).Methods("PUT")
	r.HandleFunc("/{key}", Retrieve).Methods("GET")

	if err := http.ListenAndServe(":" + ControllerPort, r); err != nil {
		log.Fatal(err)
	}

}
