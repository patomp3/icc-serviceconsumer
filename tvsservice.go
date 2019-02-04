package main

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	goracle "gopkg.in/goracle.v2"
)

// OrderRequest Object
type OrderRequest struct {
	ActionCode         string `json:"ActionCode"`
	ActivityReasonCode string `json:"ActivityReasonCode"`
	BAN                string `json:"BAN"`
	CompanyCode        string `json:"CompanyCode"`
	ImpactLevel        string `json:"ImpactLevel"`
	IssueDate          string `json:"IssueDate"`
	PrimaryResourceID  string `json:"PrimaryResourceId"`
	SubscriberNumber   string `json:"SubscriberNumber"`
	TransactionID      string `json:"TransactionId"`
}

// OrderResponse Object
type OrderResponse struct {
	ErrorCode        int    `json:"ErrorCode"`
	ErrorDescription string `json:"ErrorDescription"`
	Status           string `json:"Status"`
	TransactionID    string `json:"TransactionId"`
}

// ServiceRequest for recon struct
type ServiceRequest struct {
	ByUser struct {
		ByChannel string `json:"byChannel"`
		ByUser    string `json:"byUser"`
	} `json:"ByUser"`
	Customer struct {
		CustomerID int `json:"CustomerID"`
	} `json:"Customer"`
	Product struct {
		Product []struct {
			ProductID int `json:"ProductId"`
		} `json:"Product"`
	} `json:"Product"`
	Reason int `json:"Reason"`
	Target struct {
		Target     int    `json:"Target"`
		TargetDate string `json:"TargetDate"`
	} `json:"Target"`
}

// ServiceResult for recon result
type ServiceResult struct {
	ErrorCode   int    `json:"ErrorCode"`
	ErrorDesc   string `json:"ErrorDesc"`
	ResultValue string `json:"ResultValue"`
	ProductID   int    `json:"ProductId"`
}

// Cancel Service
func Cancel(req OrderRequest) (OrderResponse, error) {
	var result OrderResponse

	log.Printf("## Process Cancel Service")

	// get product to Cancel
	var rs driver.Rows
	var productID int
	var commercialProductID int
	var reason int
	bResult := ExecuteStoreProcedure(cfg.dbICC, "begin PK_CCBS_COLLECTION.GetCancelAgreementDetail(:1,:2,:3,:4); end;", req.PrimaryResourceID, req.ActionCode,
		req.ActivityReasonCode, sql.Out{Dest: &rs})
	if bResult && rs != nil {
		values := make([]driver.Value, len(rs.Columns()))
		// ok
		haveProduct := false
		for rs.Next(values) == nil {
			haveProduct = true

			productID, _ = strconv.Atoi(strconv.FormatInt(values[0].(int64), 10))
			commercialProductID, _ = strconv.Atoi(strconv.FormatInt(values[1].(int64), 10))
			reason, _ = strconv.Atoi(values[2].(string))

			//strconv.Atoi(strconv.FormatInt(productID, 10))
			//_ = commercialProductID
			log.Printf("## Product Cancel = %d %d %d", productID, commercialProductID, reason)

			// Call Service to Cancel
			//jsonData := map[string]string{"ThaiId": "3909800183384"}
			//var serviceReq ServiceRequest
			var serviceRes ServiceResult

			strReq := "{\r\n\t\"ByUser\":{\r\n\t\t\"byChannel\":\"#BYCHANNEL\",\r\n\t\t\"byUser\":\"#BYUSER\"\r\n\t},\r\n\t\"Customer\":{\r\n\t\t\"CustomerID\":#CUSTOMERID\r\n\t},\r\n\t\"Product\":{\r\n\t\t\"Product\":[{\r\n\t\t\t\"ProductId\":#PRODUCTID\t\t\r\n\t\t}]\r\n\t},\r\n\t\"Reason\":#REASON,\r\n\t\"Target\":{\r\n\t\t\"Target\":#TARGET,\r\n\t\t\"TargetDate\":\"\"\r\n\t}\r\n}"
			strReq = strings.Replace(strReq, "#BYCHANNEL", "", -1)
			strReq = strings.Replace(strReq, "#BYUSER", "BILL-COL", -1)
			strReq = strings.Replace(strReq, "#CUSTOMERID", req.PrimaryResourceID, -1)
			strReq = strings.Replace(strReq, "#PRODUCTID", strconv.Itoa(productID), -1)
			strReq = strings.Replace(strReq, "#REASON", strconv.Itoa(reason), -1)
			strReq = strings.Replace(strReq, "#TARGET", "0", -1)

			req := []byte(strReq)

			//fmt.Printf("%s\n", strReq)
			/*serviceReq.Customer.CustomerID, _ = strconv.Atoi(req.PrimaryResourceID)
			var p [0]ServiceRequest.Product{1}
			serviceReq.Product := []ProductID{productID}
			//serviceReq.Product.Product[0].ProductID = productID
			serviceReq.Reason = reason
			serviceReq.Target.Target = 0
			serviceReq.ByUser.ByUser = "9912"
			serviceReq.ByUser.ByChannel = "9912"*/

			//json.Unmarshal(req, &serviceReq)
			response, err := http.Post(cfg.disconnectURL, "application/json", bytes.NewBuffer(req))
			if err != nil {
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
					log.Printf("## Result >> %d %d %s\n", serviceRes.ProductID, serviceRes.ErrorCode, serviceRes.ErrorDesc)
					result.Status = serviceRes.ResultValue
					result.ErrorCode = serviceRes.ErrorCode
					result.ErrorDescription = serviceRes.ErrorDesc
				}
			}
		}

		if !haveProduct {
			result.Status = "true"
			result.ErrorCode = 0
			result.ErrorDescription = "No products to cancel"
		}
	}

	return result, nil
}

// Disconnect Service
func Disconnect(req OrderRequest) (OrderResponse, error) {
	var result OrderResponse

	log.Printf("## Process Disconnecct Service")

	// get product to disconnect
	var rs driver.Rows
	var productID int
	var commercialProductID int
	var reason int
	bResult := ExecuteStoreProcedure(cfg.dbICC, "begin PK_CCBS_COLLECTION.GetDisconAgreementDetail(:1,:2,:3,:4); end;", req.PrimaryResourceID, req.ActionCode,
		req.ActivityReasonCode, sql.Out{Dest: &rs})
	if bResult && rs != nil {
		values := make([]driver.Value, len(rs.Columns()))
		// ok
		haveProduct := false
		for rs.Next(values) == nil {
			haveProduct = true

			productID, _ = strconv.Atoi(strconv.FormatInt(values[0].(int64), 10))
			commercialProductID, _ = strconv.Atoi(strconv.FormatInt(values[1].(int64), 10))
			reason, _ = strconv.Atoi(values[2].(string))

			//strconv.Atoi(strconv.FormatInt(productID, 10))
			//_ = commercialProductID
			log.Printf("## Product Disconnect = %d %d %d", productID, commercialProductID, reason)

			// Call Service to disconnect
			//jsonData := map[string]string{"ThaiId": "3909800183384"}
			//var serviceReq ServiceRequest
			var serviceRes ServiceResult

			strReq := "{\r\n\t\"ByUser\":{\r\n\t\t\"byChannel\":\"#BYCHANNEL\",\r\n\t\t\"byUser\":\"#BYUSER\"\r\n\t},\r\n\t\"Customer\":{\r\n\t\t\"CustomerID\":#CUSTOMERID\r\n\t},\r\n\t\"Product\":{\r\n\t\t\"Product\":[{\r\n\t\t\t\"ProductId\":#PRODUCTID\t\t\r\n\t\t}]\r\n\t},\r\n\t\"Reason\":#REASON,\r\n\t\"Target\":{\r\n\t\t\"Target\":#TARGET,\r\n\t\t\"TargetDate\":\"\"\r\n\t}\r\n}"
			strReq = strings.Replace(strReq, "#BYCHANNEL", "", -1)
			strReq = strings.Replace(strReq, "#BYUSER", "BILL-COL", -1)
			strReq = strings.Replace(strReq, "#CUSTOMERID", req.PrimaryResourceID, -1)
			strReq = strings.Replace(strReq, "#PRODUCTID", strconv.Itoa(productID), -1)
			strReq = strings.Replace(strReq, "#REASON", strconv.Itoa(reason), -1)
			strReq = strings.Replace(strReq, "#TARGET", "0", -1)

			req := []byte(strReq)

			//fmt.Printf("%s\n", strReq)
			/*serviceReq.Customer.CustomerID, _ = strconv.Atoi(req.PrimaryResourceID)
			var p [0]ServiceRequest.Product{1}
			serviceReq.Product := []ProductID{productID}
			//serviceReq.Product.Product[0].ProductID = productID
			serviceReq.Reason = reason
			serviceReq.Target.Target = 0
			serviceReq.ByUser.ByUser = "9912"
			serviceReq.ByUser.ByChannel = "9912"*/

			//json.Unmarshal(req, &serviceReq)
			response, err := http.Post(cfg.disconnectURL, "application/json", bytes.NewBuffer(req))
			if err != nil {
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
			}
		}

		if !haveProduct {
			result.Status = "true"
			result.ErrorCode = 0
			result.ErrorDescription = "No products to disconnect"
		}
	}

	return result, nil
}

// DisconnectPTP Service
func DisconnectPTP(req OrderRequest) (OrderResponse, error) {
	var result OrderResponse

	log.Printf("## Process DisconnectPTP Service")

	// get product to disconnect
	var rs driver.Rows
	var productID int
	var commercialProductID int
	var reason int
	bResult := ExecuteStoreProcedure(cfg.dbICC, "begin PK_CCBS_COLLECTION.GetDisconPTPAgreementDetail(:1,:2,:3,:4); end;", req.PrimaryResourceID, req.ActionCode,
		req.ActivityReasonCode, sql.Out{Dest: &rs})
	if bResult && rs != nil {
		values := make([]driver.Value, len(rs.Columns()))
		// ok
		haveProduct := false
		for rs.Next(values) == nil {
			haveProduct = true

			productID, _ = strconv.Atoi(strconv.FormatInt(values[0].(int64), 10))
			commercialProductID, _ = strconv.Atoi(strconv.FormatInt(values[1].(int64), 10))
			reason, _ = strconv.Atoi(values[2].(string))

			//strconv.Atoi(strconv.FormatInt(productID, 10))
			//_ = commercialProductID
			log.Printf("## Product DisconnectPTP = %d %d %d", productID, commercialProductID, reason)

			// Call Service to disconnect
			//jsonData := map[string]string{"ThaiId": "3909800183384"}
			//var serviceReq ServiceRequest
			var serviceRes ServiceResult

			strReq := "{\r\n\t\"ByUser\":{\r\n\t\t\"byChannel\":\"#BYCHANNEL\",\r\n\t\t\"byUser\":\"#BYUSER\"\r\n\t},\r\n\t\"Customer\":{\r\n\t\t\"CustomerID\":#CUSTOMERID\r\n\t},\r\n\t\"Product\":{\r\n\t\t\"Product\":[{\r\n\t\t\t\"ProductId\":#PRODUCTID\t\t\r\n\t\t}]\r\n\t},\r\n\t\"Reason\":#REASON,\r\n\t\"Target\":{\r\n\t\t\"Target\":#TARGET,\r\n\t\t\"TargetDate\":\"\"\r\n\t}\r\n}"
			strReq = strings.Replace(strReq, "#BYCHANNEL", "", -1)
			strReq = strings.Replace(strReq, "#BYUSER", "BILL-COL", -1)
			strReq = strings.Replace(strReq, "#CUSTOMERID", req.PrimaryResourceID, -1)
			strReq = strings.Replace(strReq, "#PRODUCTID", strconv.Itoa(productID), -1)
			strReq = strings.Replace(strReq, "#REASON", strconv.Itoa(reason), -1)
			strReq = strings.Replace(strReq, "#TARGET", "0", -1)

			req := []byte(strReq)

			//fmt.Printf("%s\n", strReq)
			/*serviceReq.Customer.CustomerID, _ = strconv.Atoi(req.PrimaryResourceID)
			var p [0]ServiceRequest.Product{1}
			serviceReq.Product := []ProductID{productID}
			//serviceReq.Product.Product[0].ProductID = productID
			serviceReq.Reason = reason
			serviceReq.Target.Target = 0
			serviceReq.ByUser.ByUser = "9912"
			serviceReq.ByUser.ByChannel = "9912"*/

			//json.Unmarshal(req, &serviceReq)
			response, err := http.Post(cfg.disconnectURL, "application/json", bytes.NewBuffer(req))
			if err != nil {
				fmt.Printf("The HTTP request failed with error %s", err)
			} else {
				data, _ := ioutil.ReadAll(response.Body)
				//fmt.Println(string(data))
				//myReturn = json.Unmarshal(string(data))
				err = json.Unmarshal(data, &serviceRes)
				if err != nil {
					//panic(err)
					fmt.Printf("The HTTP response failed with error %s", err)
				} else {
					fmt.Printf("## Result >> %d %d %s\n", serviceRes.ProductID, serviceRes.ErrorCode, serviceRes.ErrorDesc)
					result.Status = serviceRes.ResultValue
					result.ErrorCode = serviceRes.ErrorCode
					result.ErrorDescription = serviceRes.ErrorDesc
				}
			}
		}

		if !haveProduct {
			result.Status = "true"
			result.ErrorCode = 0
			result.ErrorDescription = "No products to disconnectPTP"
		}
	}

	return result, nil
}

// Reconnect Service
func Reconnect(req OrderRequest) (OrderResponse, error) {
	var result OrderResponse

	log.Printf("## Process Reconnect Service")

	// get product to Reconnect
	var rs driver.Rows
	var productID int
	var commercialProductID int
	var profile string
	var reason int
	var reconfeeAmt goracle.Number
	bResult := ExecuteStoreProcedure(cfg.dbICC, "begin PK_CCBS_COLLECTION.GetReconAgreementDetail(:1,:2,:3,:4); end;", req.PrimaryResourceID, req.ActionCode,
		req.ActivityReasonCode, sql.Out{Dest: &rs})
	if bResult && rs != nil {
		values := make([]driver.Value, len(rs.Columns()))
		// ok
		haveProduct := false
		for rs.Next(values) == nil {
			haveProduct = true

			productID, _ = strconv.Atoi(strconv.FormatInt(values[0].(int64), 10))
			commercialProductID, _ = strconv.Atoi(strconv.FormatInt(values[1].(int64), 10))
			profile = values[2].(string)
			reason, _ = strconv.Atoi(strconv.FormatInt(values[3].(int64), 10))
			reconfeeAmt = values[4].(goracle.Number)

			//strconv.Atoi(strconv.FormatInt(productID, 10))
			//_ = commercialProductID
			log.Printf("## Product Reconnect = %d %d %s %d %s", productID, commercialProductID, profile, reason, reconfeeAmt)

			// Call Service to Reconnect
			//jsonData := map[string]string{"ThaiId": "3909800183384"}
			//var serviceReq ServiceRequest
			var serviceRes ServiceResult

			strReq := "{\r\n\t\"ByUser\":{\r\n\t\t\"byChannel\":\"#BYCHANNEL\",\r\n\t\t\"byUser\":\"#BYUSER\"\r\n\t},\r\n\t\"Customer\":{\r\n\t\t\"CustomerID\":#CUSTOMERID\r\n\t},\r\n\t\"Product\":{\r\n\t\t\"Product\":[{\r\n\t\t\t\"ProductId\":#PRODUCTID\t\t\r\n\t\t}]\r\n\t},\r\n\t\"Reason\":#REASON,\r\n\t\"Target\":{\r\n\t\t\"Target\":#TARGET,\r\n\t\t\"TargetDate\":\"\"\r\n\t}\r\n}"
			strReq = strings.Replace(strReq, "#BYCHANNEL", "", -1)
			strReq = strings.Replace(strReq, "#BYUSER", "BILL-COL", -1)
			strReq = strings.Replace(strReq, "#CUSTOMERID", req.PrimaryResourceID, -1)
			strReq = strings.Replace(strReq, "#PRODUCTID", strconv.Itoa(productID), -1)
			strReq = strings.Replace(strReq, "#REASON", strconv.Itoa(reason), -1)
			strReq = strings.Replace(strReq, "#TARGET", "0", -1)

			req := []byte(strReq)

			//fmt.Printf("%s\n", strReq)
			/*serviceReq.Customer.CustomerID, _ = strconv.Atoi(req.PrimaryResourceID)
			var p [0]ServiceRequest.Product{1}
			serviceReq.Product := []ProductID{productID}
			//serviceReq.Product.Product[0].ProductID = productID
			serviceReq.Reason = reason
			serviceReq.Target.Target = 0
			serviceReq.ByUser.ByUser = "9912"
			serviceReq.ByUser.ByChannel = "9912"*/

			//json.Unmarshal(req, &serviceReq)
			response, err := http.Post(cfg.reconnectURL, "application/json", bytes.NewBuffer(req))
			if err != nil {
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
			}
		}

		if !haveProduct {
			result.Status = "true"
			result.ErrorCode = 0
			result.ErrorDescription = "No products to reconnect"
		}
	}

	return result, nil
}

// ReconnectPTP Service
func ReconnectPTP(req OrderRequest) (OrderResponse, error) {
	var result OrderResponse

	log.Printf("## Process ReconnectPTP Service")

	// get product to ReconnectPTP
	var rs driver.Rows
	var productID int
	var commercialProductID int
	var profile string
	var reason int
	var reconfeeAmt goracle.Number
	bResult := ExecuteStoreProcedure(cfg.dbICC, "begin PK_CCBS_COLLECTION.GetReconPTPAgreementDetail(:1,:2,:3,:4); end;", req.PrimaryResourceID, req.ActionCode,
		req.ActivityReasonCode, sql.Out{Dest: &rs})
	if bResult && rs != nil {
		values := make([]driver.Value, len(rs.Columns()))
		// ok
		haveProduct := false
		for rs.Next(values) == nil {
			haveProduct = true

			productID, _ = strconv.Atoi(strconv.FormatInt(values[0].(int64), 10))
			commercialProductID, _ = strconv.Atoi(strconv.FormatInt(values[1].(int64), 10))
			profile = values[2].(string)
			reason, _ = strconv.Atoi(strconv.FormatInt(values[3].(int64), 10))
			reconfeeAmt = values[4].(goracle.Number)

			//strconv.Atoi(strconv.FormatInt(productID, 10))
			//_ = commercialProductID
			log.Printf("## Product ReconnectPTP = %d %d %s %d %s", productID, commercialProductID, profile, reason, reconfeeAmt)

			// Call Service to Reconnect
			//jsonData := map[string]string{"ThaiId": "3909800183384"}
			//var serviceReq ServiceRequest
			var serviceRes ServiceResult

			strReq := "{\r\n\t\"ByUser\":{\r\n\t\t\"byChannel\":\"#BYCHANNEL\",\r\n\t\t\"byUser\":\"#BYUSER\"\r\n\t},\r\n\t\"Customer\":{\r\n\t\t\"CustomerID\":#CUSTOMERID\r\n\t},\r\n\t\"Product\":{\r\n\t\t\"Product\":[{\r\n\t\t\t\"ProductId\":#PRODUCTID\t\t\r\n\t\t}]\r\n\t},\r\n\t\"Reason\":#REASON,\r\n\t\"Target\":{\r\n\t\t\"Target\":#TARGET,\r\n\t\t\"TargetDate\":\"\"\r\n\t}\r\n}"
			strReq = strings.Replace(strReq, "#BYCHANNEL", "", -1)
			strReq = strings.Replace(strReq, "#BYUSER", "BILL-COL", -1)
			strReq = strings.Replace(strReq, "#CUSTOMERID", req.PrimaryResourceID, -1)
			strReq = strings.Replace(strReq, "#PRODUCTID", strconv.Itoa(productID), -1)
			strReq = strings.Replace(strReq, "#REASON", strconv.Itoa(reason), -1)
			strReq = strings.Replace(strReq, "#TARGET", "0", -1)

			req := []byte(strReq)

			//fmt.Printf("%s\n", strReq)
			/*serviceReq.Customer.CustomerID, _ = strconv.Atoi(req.PrimaryResourceID)
			var p [0]ServiceRequest.Product{1}
			serviceReq.Product := []ProductID{productID}
			//serviceReq.Product.Product[0].ProductID = productID
			serviceReq.Reason = reason
			serviceReq.Target.Target = 0
			serviceReq.ByUser.ByUser = "9912"
			serviceReq.ByUser.ByChannel = "9912"*/

			//json.Unmarshal(req, &serviceReq)
			response, err := http.Post(cfg.reconnectURL, "application/json", bytes.NewBuffer(req))
			if err != nil {
				fmt.Printf("The HTTP request failed with error %s", err)
			} else {
				data, _ := ioutil.ReadAll(response.Body)
				//fmt.Println(string(data))
				//myReturn = json.Unmarshal(string(data))
				err = json.Unmarshal(data, &serviceRes)
				if err != nil {
					//panic(err)
					fmt.Printf("The HTTP response failed with error %s", err)
				} else {
					fmt.Printf("## Result %d %d %s", serviceRes.ProductID, serviceRes.ErrorCode, serviceRes.ErrorDesc)
					result.Status = serviceRes.ResultValue
					result.ErrorCode = serviceRes.ErrorCode
					result.ErrorDescription = serviceRes.ErrorDesc
				}
			}
		}

		if !haveProduct {
			result.Status = "true"
			result.ErrorCode = 0
			result.ErrorDescription = "No products to reconnectPTP"
		}
	}

	return result, nil
}
