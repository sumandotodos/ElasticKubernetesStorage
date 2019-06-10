package main

import (
	//"fmt"
	//"time"
	"net/http"
	"io"
	"os"
	"strconv"
	"log"
	"time"
	//"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//autoscaling "k8s.io/api/autoscaling/v1"
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

func WaitForPod(podname string) {
	pod, err := clientset.CoreV1().Pods("default").Get(podname, metav1.GetOptions{})
	if err != nil {
		return
	}
	for pod.Status.Phase != "Running" {
		time.Sleep(10 * time.Second)
		pod, err = clientset.CoreV1().Pods("default").Get(podname, metav1.GetOptions{})
	} 
}

func GetSts(w http.ResponseWriter, r *http.Request) {
        sts, err := clientset.AppsV1().StatefulSets("default").List(metav1.ListOptions{})
        if err != nil {
            JSONResponseFromString(w, "{\"errorete\":\""+err.Error()+"\"}")
        } else {
                JSONResponseFromString(w, "{\"number-of-stateful-sets\":"+strconv.Itoa(len(sts.Items))+"}")
        }
}

func ScaleStatefulset(w http.ResponseWriter, r *http.Request) {
        vars := mux.Vars(r)
	stsname := vars["stsname"]
	sts, err := clientset.AppsV1().StatefulSets("default").Get(stsname, metav1.GetOptions{})
	if err == nil {
		newNOfReplicas, _ := strconv.Atoi(vars["replicas"])
		*sts.Spec.Replicas = int32(newNOfReplicas)
		_, err := clientset.AppsV1().StatefulSets("default").Update(sts)
        	if err != nil {
            	JSONResponseFromString(w, "{\"errorete\":\""+err.Error()+"\"}")
        	} else {
			
            		JSONResponseFromString(w, "{\"new-replicas\":"+strconv.Itoa(newNOfReplicas)+"}")
        	}
	} else {
		JSONResponseFromString(w, "{\"result\":\"no such statefulset\"}")
	}
}

func PrunePVC(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	maxamount, _ := strconv.Atoi(vars["amount"])
	pvcs, err := clientset.CoreV1().PersistentVolumeClaims("default").List(metav1.ListOptions{})
	if err != nil {
            JSONResponseFromString(w, "{\"errorete\":\""+err.Error()+"\"}")
        } else {
                // delete last len(pvcs.Items)-maxamount pvcs
		for i := maxamount ; i < len(pvcs.Items); i++ {
			pvcToDelete := pvcs.Items[i]
			deleteErr := clientset.CoreV1().PersistentVolumeClaims("default").Delete(pvcToDelete.ObjectMeta.Name, &metav1.DeleteOptions{})
                	if deleteErr != nil {
				JSONResponseFromString(w, "{\"errorete\":\""+err.Error()+"\"}")
				break
			} 
		}
		JSONResponseFromString(w, "{\"result\":\"success\"}")
        }
}

func GetPVCs(w http.ResponseWriter, r *http.Request) {
        pvcs, err := clientset.CoreV1().PersistentVolumeClaims("default").List(metav1.ListOptions{})
        if err != nil {
            JSONResponseFromString(w, "{\"errorete\":\""+err.Error()+"\"}")
        } else {
		lastPVCString := pvcs.Items[len(pvcs.Items)-1].String()
        	JSONResponseFromString(w, "{\"number-of-pvcs\":"+strconv.Itoa(len(pvcs.Items))+"}, "+
		"{\"string-rep\":"+lastPVCString+"}")
	// Items[].ObjectMeta.Name y VolumeName        
	}
}

var StatefulSetName string

func main() {

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
	
	r := mux.NewRouter()

	r.HandleFunc("/pods", GetPods).Methods("GET")
	r.HandleFunc("/statefulsets", GetSts).Methods("GET")
	r.HandleFunc("/scalests/{stsname}/{replicas}", ScaleStatefulset).Methods("GET")
	r.HandleFunc("/pvcs", GetPVCs).Methods("GET")
	r.HandleFunc("/prunepvc/{amount}", PrunePVC).Methods("GET")
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
