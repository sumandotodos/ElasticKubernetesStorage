package main

import (
	"encoding/json"
//	"fmt"
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

	records := make([]ShitRecord, 3)

	records[0].Name = "John"
	records[0].Age = 55

	records[1].Name = "Terrence"
	records[1].Age = 1

	records[2].Name = "Gilligan"
	records[2].Age = 99

	shit := new(Shit)

	shit.Owner = "Mumumu"
	shit.Records = records



	file, _ := json.Marshal(shit)

	_ = ioutil.WriteFile("shit.json", file, 0644)

}


