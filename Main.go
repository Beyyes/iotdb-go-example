package main

import (
	"fmt"
	"github.com/apache/iotdb-client-go/client"
	"github.com/apache/iotdb-client-go/common"
	"log"
)

var session client.Session

func main() {
	config := &client.Config{
		Host:     "127.0.0.1",
		Port:     "6667",
		UserName: "root",
		Password: "root",
	}
	session = client.NewSession(config)
	if err := session.Open(false, 0); err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	insertRecordsAnke()

	fmt.Println("Query data: ")
	executeQueryStatement("select * from root.db_go.** align by device")

	fmt.Println("\r\nDelete data: delete from root.db_go.d1.* where time = 1\r\n")
	session.ExecuteStatement("delete from root.db_go.d1.* where time = 1")

	fmt.Println("Query data after delete: ")
	executeQueryStatement("select * from root.db_go.** align by device")
}

func executeQueryStatement(sql string) {
	var timeout int64 = 1000
	sessionDataSet, err := session.ExecuteQueryStatement(sql, &timeout)
	if err == nil {
		printDataSet1(sessionDataSet)
		sessionDataSet.Close()
	} else {
		log.Println(err)
	}
}

func printDataSet1(sds *client.SessionDataSet) {
	showTimestamp := !sds.IsIgnoreTimeStamp()
	if showTimestamp {
		fmt.Print("Time\t")
	}

	for i := 0; i < sds.GetColumnCount(); i++ {
		if i == 0 {
			fmt.Printf("%s\t\t\t", sds.GetColumnName(i))
		} else {
			fmt.Printf("%s\t\t", sds.GetColumnName(i))
		}

	}
	fmt.Println()

	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		if showTimestamp {
			fmt.Printf("%d\t", sds.GetInt64(client.TimestampColumnName))
		}
		for i := 0; i < sds.GetColumnCount(); i++ {
			columnName := sds.GetColumnName(i)
			v := sds.GetValue(columnName)
			if v == nil {
				v = "null"
			}
			fmt.Printf("%v\t\t", v)
		}
		fmt.Println()
	}
}

func insertRecordsAnke() {
	var (
		deviceId     = []string{"root.db_go.d1", "root.db_go.d2"}
		measurements = [][]string{{"s1", "s2"}, {"s1", "s2"}}
		dataTypes    = [][]client.TSDataType{{client.INT32, client.INT32}, {client.INT32, client.INT32}}
		values       = [][]interface{}{{int32(1), int32(1)}, {int32(2), int32(2)}}
		timestamp    = []int64{1, 2}
	)
	checkError(session.InsertRecords(deviceId, measurements, dataTypes, values, timestamp))
}

func checkError(status *common.TSStatus, err error) {
	if err != nil {
		log.Fatal(err)
	}

	if status != nil {
		if err = client.VerifySuccess(status); err != nil {
			log.Println(err)
		}
	}
}
