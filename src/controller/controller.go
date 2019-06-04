package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/gorilla/mux"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var db_svr string
var db_port string

var growThreshold float32 = 0.7
var shrinkThreshold float32 = 0.4

// Types for db documents

type Status struct {
	NumberOfCells  int    `json:"numberofcells"`
	TotalSpace     uint64 `json:"totalspace"`
	CellNamePrefix string `json:"cellnameprefix"`
	UsedSpace      uint64 `json:"usedspace"`
}

type CellStatus struct {
	CellId        int    `json:"_id"`
	Capacity      uint64 `json:"capacity"`
	FreeSpace     uint64 `json:"freespace"`
	NumberOfFiles uint64 `json:"numberoffile"`
}

type Directory struct {
	Category string `json:"category"`
	Path     string `json:"path"`
	CellId   int    `json:"cellid"`
}

type DBConnectionContext struct {
	client       *mongo.Client
	serverstatus *mongo.Collection
	cellstatus   *mongo.Collection
	directories  *mongo.Collection
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
		{"freespace", cellcapacity}, {"capacity", cellcapacity}, {"numberoffiles", 0}})
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

// Utilities

func JSONResponseFromString(w http.ResponseWriter, res string) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, res)
}

func InitializeNewCell() uint64 {
	return 100
}

func addDirectoryEntry(conn *DBConnectionContext, category string, fullpath string, cellid int) error {
	_, err := conn.directories.InsertOne(context.TODO(), bson.D{
		{"category", category}, {"path", fullpath}, {"cellid", cellid}})
	return err
}

func findCellWithFreeSpace(conn *DBConnectionContext, requestedSpace uint64) int {

	var results []*CellStatus

	cursor, err := conn.cellstatus.Find(context.TODO(), bson.D{{}})

	if err != nil {
		return -2
	} else {

		for cursor.Next(context.TODO()) {
			var elem CellStatus
			err := cursor.Decode(&elem)
			if err != nil {
				return -3
			}
			results = append(results, &elem)
		}

		cursor.Close(context.TODO())

		for _, element := range results {
			fmt.Println(element)
		}

		return 0

	}
}

func addUsedStorage(conn *DBConnectionContext, amount uint64, cellid int) {
	_, err := conn.serverstatus.UpdateOne(context.TODO(), bson.D{{"_id", 0}}, bson.D{{"$inc", bson.D{{"usedspace", amount}}}})
	if(err != nil) {
		fmt.Println(err)
	}
	_, err = conn.cellstatus.UpdateOne(context.TODO(), bson.D{{"_id", cellid}}, bson.D{{"$inc", bson.D{{"freespace", -int64(amount)}}}})
	if(err != nil) {
		fmt.Println(err)
	}
	_, err = conn.cellstatus.UpdateOne(context.TODO(), bson.D{{"_id", cellid}}, bson.D{{"$inc", bson.D{{"numberoffiles", 1}}}})
	if(err != nil) {
		fmt.Println(err)
	}
}

// REST API Functions

func Retrieve(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fmt.Println("Attempting to retrieve value " + vars["value"])
}

func Store(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fmt.Println("Attempting to retrieve value " + vars["value"])
	lengthOfValue := uint64(len(vars["value"]))
	cellid := findCellWithFreeSpace(&dbConnectionContext, lengthOfValue)
	if cellid == -1 {
		JSONResponseFromString(w, "{\"result\":\"'Try later'\"}")
	} else {
		fmt.Println("Storing data in cell " + strconv.Itoa(cellid))
		addDirErr := addDirectoryEntry(&dbConnectionContext, "default", vars["key"], cellid)
		if addDirErr != nil {
			JSONResponseFromString(w, "{\"result\":\"'Server error'\"}")
		} else {
			addUsedStorage(&dbConnectionContext, lengthOfValue, cellid)
			JSONResponseFromString(w, "{\"result\":\"'OK'\", \"bytes\":"+strconv.FormatUint(lengthOfValue, 10)+"}")
		}
	}
}

func GetServiceStatus(w http.ResponseWriter, r *http.Request) {

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
	dbConnectionContext.directories = client.Database("service").Collection("directories")

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
	r.HandleFunc("/status", GetServiceStatus).Methods("GET")

	if err := http.ListenAndServe(":"+ControllerPort, r); err != nil {
		log.Fatal(err)
	}

}
