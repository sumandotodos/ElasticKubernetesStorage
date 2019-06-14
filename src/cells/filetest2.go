package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type ShitRecord struct {
	Name	string `json:"name"`
	Age	int    `json:"age"`
}

type Shit struct {
	Owner string  `json:"owner"`
	Records	[]ShitRecord  `json:"records"`
}


func main() {

	file, _ := ioutil.ReadFile("shit.json")

	shit := &Shit{}

	_ = json.Unmarshal(file, shit)

	fmt.Println("The data was: ")
	fmt.Println(shit)

}


