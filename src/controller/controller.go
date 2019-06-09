package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type ServerStateEnum int

const (
	SNAFU       ServerStateEnum = 0
	ScalingUp   ServerStateEnum = 1
	Draining    ServerStateEnum = 2
	ScalingDown ServerStateEnum = 3
)

const revision int = 15

var ServerState ServerStateEnum = SNAFU

var db_svr string
var db_port string

var cell_port string
var cell_name_prefix string
var cell_service_name string

var cellCapacity int = 100
var growThreshold float32 = 0.7
var shrinkThreshold float32 = 0.4

// Types for db documents

type Status struct {
	NumberOfCells      int    `json:"numberofcells"`
	TotalSpace         uint64 `json:"totalspace"`
	CellNamePrefix     string `json:"cellnameprefix"`
	CellServiceName    string `json:"cellservicename"`
	UsedSpace          uint64 `json:"usedspace"`
	ScaleUpThreshold   uint64 `json:"suthreshold"`
	ScaleDownThreshold uint64 `json:"sdthreshold"`
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
var StatefulSetName string
var clientset *kubernetes.Clientset

var status Status



// DB functions

func pushServerStatus(conn *DBConnectionContext) error {
	_, err := conn.serverstatus.UpdateOne(context.TODO(), bson.D{{"_id", 0}},
		bson.D{
			{"$set", bson.D{
				{"numberofcells", status.NumberOfCells},
				{"totalspace", status.TotalSpace},
				{"usedspace", status.UsedSpace},
			},
			},
		})
	return err
}

func getServerStatus(conn *DBConnectionContext) (Status, error) {
	var statusInDB Status
	err := conn.serverstatus.FindOne(context.TODO(), bson.D{{"_id", 0}}).Decode(&statusInDB)
	return statusInDB, err
}

func initializeServerStatus(conn *DBConnectionContext) error {
	fmt.Println("  >> initializeServerStatus called")
	cellcapacity := InitializeNewCell()
	SUThreshold := (cellcapacity * 30)/100
	SDThreshold := (cellcapacity * 60)/100
	fmt.Println("SUThreshold :  " + strconv.Itoa(int(SUThreshold)))
	status.NumberOfCells = 1
    status.TotalSpace = uint64(cellcapacity)
    status.CellNamePrefix = cell_name_prefix
    status.CellServiceName = cell_service_name
    status.UsedSpace = uint64(0)
    status.ScaleUpThreshold = uint64(SUThreshold)
	status.ScaleDownThreshold = uint64(SDThreshold)
	fmt.Println("status.ScaleUpThreshold:  " + strconv.Itoa(int(status.ScaleUpThreshold)))
	_, err := conn.serverstatus.InsertOne(context.TODO(), bson.D{{"_id", 0},
		{"numberofcells", 1}, {"usedspace", uint64(0)}, 
		{"totalspace", cellcapacity}, {"cellservicename", cell_service_name}, 
		{"suthreshold", SUThreshold}, {"sdthreshold", SDThreshold},
		{"cellnameprefix", cell_name_prefix}})
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

func makeCellURL(cellid int) string {
	return "http://" + cell_name_prefix + "-" + strconv.Itoa(cellid) + "." + cell_service_name + ":" + cell_port
}

func makeCellHealthcheck(cellid int) string {
	return makeCellURL(cellid) + "/healthcheck"
}

// Utilities

func JSONResponseFromString(w http.ResponseWriter, res string) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, res)
}

func InitializeNewCell() uint64 {
	return uint64(cellCapacity)
}

func getDirectoryEntryCellId(conn *DBConnectionContext, category string, fullpath string) (int, error) {
	var directoryEntry Directory
	err := conn.directories.FindOne(context.TODO(), bson.D{
		{"category", category}, {"path", fullpath}}).Decode(&directoryEntry)
	if err != nil {
		return -1, err
	} else {
		return directoryEntry.CellId, err
	}
}

func addDirectoryEntry(conn *DBConnectionContext, category string, fullpath string, cellid int) error {
	_, err := conn.directories.InsertOne(context.TODO(), bson.D{
		{"category", category}, {"path", fullpath}, {"cellid", cellid}})
	return err
}

func removeDirectoryEntry(conn *DBConnectionContext, category string, fullpath string) error {
	_, err := conn.directories.DeleteOne(context.TODO(), bson.D{
		{"category", category}, {"path", fullpath}})
	return err
}

func detectLivingCells() int {
	var err error
	var id int
	id = 0
	err = nil
	for err == nil {
		url := makeCellHealthcheck(id)
		fmt.Println("URL: " + url)
		_, err = http.Get(url)
		if err == nil {
			fmt.Println("  >> the error was nil")
		} else {
			fmt.Println("  >> the error was: ")
			fmt.Println(err)
		}
		id = id + 1
	}
	return id - 1
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
	if err != nil {
		fmt.Println(err)
	}
	_, err = conn.cellstatus.UpdateOne(context.TODO(), bson.D{{"_id", cellid}}, bson.D{{"$inc", bson.D{{"freespace", -int64(amount)}}}})
	if err != nil {
		fmt.Println(err)
	}
	_, err = conn.cellstatus.UpdateOne(context.TODO(), bson.D{{"_id", cellid}}, bson.D{{"$inc", bson.D{{"numberoffiles", 1}}}})
	if err != nil {
		fmt.Println(err)
	}
}

// kubernetes driver functions

func ScaleStatefulSet(toSize int) error {
	sts, err := clientset.AppsV1().StatefulSets("default").Get(StatefulSetName, metav1.GetOptions{})
	if err == nil {
		*sts.Spec.Replicas = int32(toSize)
		_, err := clientset.AppsV1().StatefulSets("default").Update(sts)
		if err != nil {
			return err
		} else {
			return nil
		}
	} else {
		return err
	}
}

func PrunePVC(toAmount int) error {
	pvcs, err := clientset.CoreV1().PersistentVolumeClaims("default").List(metav1.ListOptions{})
	if err != nil {
		return err
	} else {
		for i := toAmount; i < len(pvcs.Items); i++ {
			pvcToDelete := pvcs.Items[i]
			deleteErr := clientset.CoreV1().PersistentVolumeClaims("default").Delete(pvcToDelete.ObjectMeta.Name, &metav1.DeleteOptions{})
			if deleteErr != nil {
				return deleteErr
			}
		}
		return nil
	}
}

// REST API Functions

func Delete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fmt.Println("  # controller # Attempting to retrieve value " + vars["id"])
	cellid, err := getDirectoryEntryCellId(&dbConnectionContext, "default", vars["id"])
	if err != nil {
		JSONResponseFromString(w, "{\"error\":"+err.Error()+"}")
	} else {
		cellURL := makeCellURL(cellid)
		client := &http.Client{}
		req, err := http.NewRequest("DELETE", cellURL+"/"+vars["key"]+"/_", nil)
		if err != nil {
			JSONResponseFromString(w, "{\"error\":"+err.Error()+"}")
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			JSONResponseFromString(w, "{\"error\":"+err.Error()+"}")
			return
		}
		defer resp.Body.Close()
		if err != nil {
			JSONResponseFromString(w, "{\"error\":"+err.Error()+"}")
		} else {
			JSONResponseFromString(w, "{\"result\":\"success\"}")
		}
	}
}

func Retrieve(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fmt.Println("  # controller # Attempting to retrieve value " + vars["id"])
	cellid, err := getDirectoryEntryCellId(&dbConnectionContext, "default", vars["id"])
	if err != nil {
		JSONResponseFromString(w, "{\"error\":"+err.Error()+"}")
	} else {
		cellURL := makeCellURL(cellid)
		result, err := http.Get(cellURL + "/" + vars["id"] + "/_")
		if err != nil {
			JSONResponseFromString(w, "{\"error\":"+err.Error()+"}")
		} else {
			defer result.Body.Close()
			body, _ := ioutil.ReadAll(result.Body)
			//fmt.Println("get:\n", keepLines(string(body), 3))
			JSONResponseFromString(w, "{\"result\":"+string(body)+"}")
		}
	}
}

func ScaleUp() {
	if(ServerState == SNAFU) {
		fmt.Println("Starting scale up...")
		ServerState = ScalingUp
		targetSize := status.NumberOfCells + 1
		err := ScaleStatefulSet(targetSize)
		if err != nil {
			fmt.Println("Error scaling sts")
			return
		} else {
			time.Sleep(10 * time.Second)
			// @TODO this delay must be replaced with
			// waiting for the new pod to be ready!!
			status.NumberOfCells = status.NumberOfCells + 1
			status.TotalSpace += uint64(cellCapacity)
			err = pushServerStatus(&dbConnectionContext)
			if err != nil {
				fmt.Println("Error pushing server status")
				return
			}
		}
		ServerState = SNAFU
	}
}

func Store(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	fmt.Println("  # controller # Attempting to store value " + vars["id"])
	lengthOfValue := uint64(len(vars["info"]))
	cellid := findCellWithFreeSpace(&dbConnectionContext, lengthOfValue)
	if cellid == -1 {
		JSONResponseFromString(w, "{\"result\":\"'Try later'\"}")
	} else {
		fmt.Println("Storing data in cell " + strconv.Itoa(cellid))
		addDirErr := addDirectoryEntry(&dbConnectionContext, "default", vars["id"], cellid)
		if addDirErr != nil {
			JSONResponseFromString(w, "{\"result\":\"'Server error'\"}")
		} else {
			cellURL := makeCellURL(cellid)
			_, err := http.Post(cellURL+"/"+vars["id"]+"/"+vars["info"], "application/text", nil)
			if err != nil {
				JSONResponseFromString(w, "{\"error\":"+err.Error()+"}")
			} else {
				addUsedStorage(&dbConnectionContext, lengthOfValue, cellid)
				status.UsedSpace += lengthOfValue
				if (status.TotalSpace - status.UsedSpace) < 30 {
					fmt.Println("Scale up condition found");
					go ScaleUp()
				} else {
					fmt.Println("There is still " + strconv.Itoa(int(status.TotalSpace - status.UsedSpace)) + " bytes free")
				}
				JSONResponseFromString(w, "{\"result\":\"'OK'\", \"bytes\":"+strconv.FormatUint(lengthOfValue, 10)+"}")
			}
		}
	}
}

func GetServiceStatus(w http.ResponseWriter, r *http.Request) {
	livingCells := detectLivingCells()
	JSONResponseFromString(w, "{\"revision\":"+strconv.Itoa(revision)+", \"cells-alive\":"+strconv.Itoa(livingCells)+", " + 
		"\"numberofcells\":" + strconv.Itoa(status.NumberOfCells) + ", " +
		"\"totalspace\":" + strconv.Itoa(int(status.TotalSpace)) + ", " +
		"\"usedspace\":" + strconv.Itoa(int(status.UsedSpace)) + ", " +
		"\"suthreshold\":" + strconv.Itoa(int(status.ScaleUpThreshold)) + "}")
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
	cell_port = os.Getenv("CELL_PORT")
	cell_service_name = os.Getenv("CELL_SERVICE_NAME")
	cell_name_prefix = os.Getenv("CELL_NAME_PREFIX")
	if cell_port == "" {
		cell_port = "7777"
	}
	if cell_service_name == "" {
		cell_service_name = "storage-cells-service"
	}
	if cell_name_prefix == "" {
		cell_name_prefix = "storagecells-sts"
	}

	StatefulSetName = os.Getenv("STSNAME")
	if StatefulSetName == "" {
		StatefulSetName = "storagecells-sts"
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
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

	fmt.Println("Trying to recover status from db...")
	//status, staterr := getServerStatus(&dbConnectionContext)

	//if (staterr != nil) || (status.NumberOfCells == 0) {
		fmt.Println("  ... none found, initializing")
		_ = initializeServerStatus(&dbConnectionContext)
		status, _ = getServerStatus(&dbConnectionContext)
	//}

	fmt.Println("Number of cells: " + strconv.Itoa(status.NumberOfCells))

	r := mux.NewRouter()
	r.HandleFunc("/healthcheck", HealthCheck).Methods("GET")
	r.HandleFunc("/status", GetServiceStatus).Methods("GET")

	r.HandleFunc("/post/{id}/{info}", Store).Methods("GET")
	r.HandleFunc("/get/{id}/{info}", Retrieve).Methods("GET")
	r.HandleFunc("/delete/{id}/{info}", Delete).Methods("DELETE")

	r.HandleFunc("/{id}/{info}", Store).Methods("POST")
	//r.HandleFunc("/{id}/{info}", Update).Methods("PUT")
	r.HandleFunc("/{id}/{info}", Retrieve).Methods("GET")
	r.HandleFunc("/{id}/{info}", Delete).Methods("DELETE")

	if err := http.ListenAndServe(":"+ControllerPort, r); err != nil {
		log.Fatal(err)
	}

}
