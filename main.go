package main

import (
	"log"
	"os"
	"strings"

	"github.com/spf13/viper"
)

type appConfig struct {
	queueName string
	queueURL  string

	dbICC         string
	dbPED         string
	dbATB2        string
	disconnectURL string
	reconnectURL  string
	cancelURL     string

	updateOrderURL string
}

var cfg appConfig

func main() {
	log.Printf("##### ICC Collection Consumer Started #####")

	// For no assign parameter env. using default to Test
	var env string
	if len(os.Args) > 1 {
		env = strings.ToLower(os.Args[1])
	} else {
		env = "development"
	}

	// Load configuration
	viper.SetConfigName("app")    // no need to include file extension
	viper.AddConfigPath("config") // set the path of your config file
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("## Config file not found. >> %s\n", err.Error())
	} else {
		// read config file
		cfg.queueName = viper.GetString(env + ".queuename")
		cfg.queueURL = viper.GetString(env + ".queueurl")
		cfg.disconnectURL = viper.GetString(env + ".disconnecturl")
		cfg.reconnectURL = viper.GetString(env + ".reconnecturl")
		cfg.cancelURL = viper.GetString(env + ".cancelurl")
		cfg.dbICC = viper.GetString(env + ".DBICC")
		cfg.dbATB2 = viper.GetString(env + ".DBATB2")
		cfg.dbPED = viper.GetString(env + ".DBPED")
		cfg.updateOrderURL = viper.GetString(env + ".updateorderurl")

		log.Printf("## Loading Configuration")
		log.Printf("## Env\t= %s", env)
	}

	q := ReceiveQueue{cfg.queueURL, cfg.queueName}
	ch := q.Connect()
	q.Receive(ch)
	//q.Close()

	//router := mux.NewRouter()
	//router.HandleFunc("/serviceconsumer", serviceConsumer).Methods("POST")

	//log.Fatal(http.ListenAndServe(":"+cfg.port, router))

}
