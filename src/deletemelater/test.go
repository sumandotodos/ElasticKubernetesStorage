package main

import (
	//"fmt"
	//"time"
	"net/http"
	"io"
	"strconv"
	"log"
	//"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"github.com/gorilla/mux"
)

func JSONResponseFromString(w http.ResponseWriter, res string) {
        w.Header().Set("Content-Type", "application/json; charset=UTF-8")
        w.WriteHeader(http.StatusOK)
        io.WriteString(w, res)
}

var clientset *kubernetes.Clientset

func HealthTest(w http.ResponseWriter, r *http.Request) {
        JSONResponseFromString(w, "{\"alive\":\"yup\"}")
}

func GetPods(w http.ResponseWriter, r *http.Request) {
	pods, err := clientset.CoreV1().Pods("default").List(metav1.ListOptions{})
        if err != nil {
		
            JSONResponseFromString(w, "{\"errorete\":\""+err.Error()+"\"}")
        } else {
        	JSONResponseFromString(w, "{\"number-of-pods\":"+strconv.Itoa(len(pods.Items))+"}")
	}
}

func main() {

	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	clientset, err = kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	
	r := mux.NewRouter()

	r.HandleFunc("/pods", GetPods).Methods("GET")
	r.HandleFunc("/healthcheck", HealthTest).Methods("GET")

	if err = http.ListenAndServe(":6666", r); err != nil {
                log.Fatal(err)
        }

	/*
	for {
		pods, err := clientset.CoreV1().Pods("").List(metav1.ListOptions{})
		if err != nil {
			panic(err.Error())
		}
		fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

		// Examples for error handling:
		// - Use helper functions like e.g. errors.IsNotFound()
		// - And/or cast to StatusError and use its properties like e.g. ErrStatus.Message
		_, err = clientset.CoreV1().Pods("default").Get("example-xxxxx", metav1.GetOptions{})
		if errors.IsNotFound(err) {
			fmt.Printf("Pod not found\n")
		} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
			fmt.Printf("Error getting pod %v\n", statusError.ErrStatus.Message)
		} else if err != nil {
			panic(err.Error())
		} else {
			fmt.Printf("Found pod\n")
		}

		time.Sleep(10 * time.Second)
	}
	*/

}
