package main

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/streadway/amqp"
)

// ReceiveQueue struct...
type ReceiveQueue struct {
	URL       string
	QueueName string
}

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s", msg, err)
	}
}

// Close for
func (r ReceiveQueue) Close() {
	//q.conn.Close()
	//q.ch.Close()
}

// Connect for
func (r ReceiveQueue) Connect() *amqp.Channel {
	conn, err := amqp.Dial(r.URL)
	//defer conn.Close()
	if err != nil {
		failOnError(err, "Failed to connect to RabbitMQ")
		return nil
	}

	ch, err := conn.Channel()
	//defer ch.Close()
	if err != nil {
		failOnError(err, "Failed to open a channel")
		return nil
	}

	return ch
}

// Receive for receive message from queue
func (r ReceiveQueue) Receive(ch *amqp.Channel) {

	/*conn, err := amqp.Dial(q.URL)
	defer conn.Close()
	if err != nil {
		failOnError(err, "Failed to connect to RabbitMQ")
		return false
	}

	ch, err := conn.Channel()
	defer ch.Close()
	if err != nil {
		failOnError(err, "Failed to open a channel")
		return false
	}*/

	q, err := ch.QueueDeclarePassive(
		r.QueueName, // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		failOnError(err, "Failed to declare a queue")
	}

	msgs, err := ch.Consume(
		q.Name, // routing key
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		failOnError(err, "Failed to publish a message")

	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("## Received a message : %s", d.Body)
			log.Printf("## >> Order Id : %s, App Id : %s", d.MessageId, d.AppId)

			//log.Printf("## Message Id: %s, App Id: %s\n", d.MessageId, d.AppId)

			// Process Message Body
			//Read Json message
			var req OrderRequest
			err = json.Unmarshal(d.Body, &req)
			if err != nil {
				panic(err)
			}
			// Get ServiceCode from ActionCode & ActivityReasonCode
			var rs driver.Rows
			bResult := ExecuteStoreProcedure(cfg.dbATB2, "begin PK_IBS_CCBS_ORDER.BG_CCBS_ORDER_SERVICECODE(:1,:2,:3); end;", req.ActionCode,
				req.ActivityReasonCode, sql.Out{Dest: &rs})
			if bResult && rs != nil {
				values := make([]driver.Value, len(rs.Columns()))

				// ok
				for rs.Next(values) == nil {
					var res OrderResponse

					log.Printf("## Service Code = %s", values[0].(string))
					switch serviceCode := values[0].(string); serviceCode {
					case "CANCEL", "CANCELPTP":
						res, _ = Cancel(req)
						log.Printf("## Cancel Result = %s %d, %s", res.Status, res.ErrorCode, res.ErrorDescription)
					case "DISCONNECT":
						res, _ = Disconnect(req)
						log.Printf("## Disconnect Result = %s %d, %s", res.Status, res.ErrorCode, res.ErrorDescription)
					case "DISCONNECTPTP":
						res, _ = DisconnectPTP(req)
						log.Printf("## DisconnectPTP Result = %s %d, %s", res.Status, res.ErrorCode, res.ErrorDescription)
					case "RECONNECT":
						res, _ = Reconnect(req)
						log.Printf("## Reconnect Result = %s %d, %s", res.Status, res.ErrorCode, res.ErrorDescription)
					case "RECONPTP":
						res, _ = ReconnectPTP(req)
						log.Printf("## ReconnectPTP Result = %s %d, %s", res.Status, res.ErrorCode, res.ErrorDescription)
					}

					// struct to json
					msg, _ := json.Marshal(res)
					log.Printf("msg: %s", string(msg))
					// remove escape code
					msgSent := strconv.Quote(string(msg))
					/*if err != nil {
						log.Printf("%s", err.Error())
					}*/
					log.Printf("msgSent: %s", msgSent)

					//### Response update to broker
					//msgResult := fmt.Sprintf("%v", res)
					strReq := "{\r\n\t\"order_trans_id\":\"#ORDERTRANSID\",\r\n\t\"order_response_message\":#MESSAGE,\r\n\t\"order_status\":#STATUS\r\n}\r\n"
					strReq = strings.Replace(strReq, "#ORDERTRANSID", d.AppId, -1)
					strReq = strings.Replace(strReq, "#MESSAGE", msgSent, -1)
					strReq = strings.Replace(strReq, "#STATUS", res.Status, -1)

					log.Printf("%s", strReq)
					req := []byte(strReq)

					//json.Unmarshal(req, &serviceReq)
					response, err := http.Post(cfg.updateOrderURL, "application/json", bytes.NewBuffer(req))
					if err != nil {
						log.Printf("The HTTP request failed with error %s", err)
					} else {
						log.Printf("%v", response)
					}
					/*if err != nil {
						log.Printf("The HTTP request failed with error %s", err)
					} else {
						data, _ := ioutil.ReadAll(response.Body)
						//fmt.Println(string(data))
						//myReturn = json.Unmarshal(string(data))
						err = json.Unmarshal(data, &serviceRes)
						if err != nil {
							//panic(err)
							log.Printf("The HTTP response failed with error %s", err)
						} else {
							log.Printf("## Result >> %d %d %s", serviceRes.ProductID, serviceRes.ErrorCode, serviceRes.ErrorDesc)
							result.Status = serviceRes.ResultValue
							result.ErrorCode = serviceRes.ErrorCode
							result.ErrorDescription = serviceRes.ErrorDesc
						}
					}*/
				}
			}
		}
	}()

	log.Printf("## [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
