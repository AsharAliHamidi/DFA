package main

// This is a mortgage chaincode for Mashreq Bank
// Mashreq Bank will Deploy the chaincode
// After verification, Mashreq will Add details of verification (Property Details, Seller Account, Buyer Share)
// RERA will then check the sell condition and approve or reject the request
// Upon approval, it will Add hash of signed documents and links for DMS and then AutoEvents will be called
// Mashreq will Update remaining mortgage amount

import (
	//"encoding/base64"
	"strconv"
	"errors"
	"fmt"
	//"flag"
	"time"
	"encoding/json"
	//"database/sql"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/crypto/primitives"
	//"github.com/op/go-logging"
	"math/rand"
	//"github.com/go-mssqldb"
)
 
//var myLogger = logging.MustGetLogger("Reconciliation")
var myLogger = shim.NewLogger("Reconciliation")
type ReconChaincode struct  {
}

var statuses = map[int]string{
  1: "Initiated",
  2: "Recieved",
  3: "Authorized",
  4: "AuthRecieved",
  5: "Reconciled",
  6: "Settled",
} //To access this constant, use -> fmt.Println(statuses [5])

type ReconciliationStruct struct {
	Status string `json:"Status"`
	EpayRefNum string `json:"EpayRefNum"`
	EntityRefNum string `json:"EntityRefNum"`
	IssuerRefNum string `json:"IssuerRefNum"`
	BillNumber string `json:"BillNumber"`
	BillingCompany string `json:"BillingCompany"`
	Issuer string `json:"Amount"`
	Amount string `json:"Issuer"`
	BatchID string `json:"BatchID"`
	DateTime string `json:"DateTime"`
	Details string `json:"Details"`
}

type TranCounts struct {
	Total int `json:"Total"`
	Initiated int `json:"Initiated"`
	Recieved int `json:"Recieved"`
	Authorized int `json:"Authorized"`
	AuthRecieved int `json:"AuthRecieved"`
	Reconciled int `json:"Reconciled"`
	Settled int `json:"Settled"`
}

type TranCountsEntity struct {
	RTA string `json:"RTA"`
	DEWA string `json:"DEWA"`
	Etisalat string `json:"Etisalat"`
	DU string `json:"DU"`
	Customs string `json:"Customs"`
}

func createTableRecon(stub shim.ChaincodeStubInterface) error {
	// Create table one
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "Status", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "EpayRefNum", Type: shim.ColumnDefinition_STRING, Key: true}
	col3 := shim.ColumnDefinition{Name: "EntityRefNum", Type: shim.ColumnDefinition_STRING, Key: false}
	col4 := shim.ColumnDefinition{Name: "IssuerRefNum", Type: shim.ColumnDefinition_STRING, Key: false}
	col5 := shim.ColumnDefinition{Name: "BillNumber", Type: shim.ColumnDefinition_STRING, Key: false}
	col6 := shim.ColumnDefinition{Name: "BillingCompany", Type: shim.ColumnDefinition_STRING, Key: false}
	col7 := shim.ColumnDefinition{Name: "Issuer", Type: shim.ColumnDefinition_STRING, Key: false}
	col8 := shim.ColumnDefinition{Name: "Amount", Type: shim.ColumnDefinition_STRING, Key: false}
	col9 := shim.ColumnDefinition{Name: "BatchID", Type: shim.ColumnDefinition_STRING, Key: false}
	col10 := shim.ColumnDefinition{Name: "DateTime", Type: shim.ColumnDefinition_STRING, Key: false}
	col11 := shim.ColumnDefinition{Name: "Details", Type: shim.ColumnDefinition_STRING, Key: false}		
	colDefs = append(colDefs, &col1)
	colDefs = append(colDefs, &col2)
	colDefs = append(colDefs, &col3)
	colDefs = append(colDefs, &col4)
	colDefs = append(colDefs, &col5)
	colDefs = append(colDefs, &col6)
	colDefs = append(colDefs, &col7)
	colDefs = append(colDefs, &col8)
	colDefs = append(colDefs, &col9)
	colDefs = append(colDefs, &col10)
	colDefs = append(colDefs, &col11)
	return stub.CreateTable("Reconciliation", colDefs)
}

func createTableBatch(stub shim.ChaincodeStubInterface) error {
	// Create table one
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "BatchID", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "BillingCompany", Type: shim.ColumnDefinition_STRING, Key: true}
	col3 := shim.ColumnDefinition{Name: "Issuer", Type: shim.ColumnDefinition_STRING, Key: false}
	col4 := shim.ColumnDefinition{Name: "Amount", Type: shim.ColumnDefinition_STRING, Key: false}
	col5 := shim.ColumnDefinition{Name: "Status", Type: shim.ColumnDefinition_STRING, Key: false}
	col6 := shim.ColumnDefinition{Name: "DateTime", Type: shim.ColumnDefinition_STRING, Key: false}
	col7 := shim.ColumnDefinition{Name: "Details", Type: shim.ColumnDefinition_STRING, Key: false}		
	colDefs = append(colDefs, &col1)
	colDefs = append(colDefs, &col2)
	colDefs = append(colDefs, &col3)
	colDefs = append(colDefs, &col4)
	colDefs = append(colDefs, &col5)
	colDefs = append(colDefs, &col6)
	colDefs = append(colDefs, &col7)
	return stub.CreateTable("Batch", colDefs)
}

func createTableAccounts(stub shim.ChaincodeStubInterface) error {
	// Create table one
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "AccountNumber", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "Title", Type: shim.ColumnDefinition_STRING, Key: true}
	col3 := shim.ColumnDefinition{Name: "Balance", Type: shim.ColumnDefinition_STRING, Key: false}
	colDefs = append(colDefs, &col1)
	colDefs = append(colDefs, &col2)
	colDefs = append(colDefs, &col3)
	return stub.CreateTable("Accounts", colDefs)
}

func (t *ReconChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error)  {
	myLogger.Debug("Init Chaincode...")
	
	err := createTableRecon(stub)
	
	if err != nil {
		return nil, fmt.Errorf("Error creating table one during init. %s", err)
	}
	
	myLogger.Debug("Init Chaincode...done")

	return nil, nil
}

func (t *ReconChaincode) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error)  {
	if function == "initiateTran" {
		return t.initiateTran(stub, args)
	}	else if function == "gatewayTranLeg1" {
		return t.gatewayTranLeg1(stub, args)
	}	else if function == "networkTran" {
		return t.networkTran(stub, args)
	}	else if function == "gatewayTranLeg2" {
		return t.gatewayTranLeg2(stub, args)
	}	else if function == "reconcileTran" {
		return t.reconcileTran(stub, args)
	}	else if function == "LoadTest" {
		return t.LoadTest(stub, args)
	}
	return nil, errors.New("Received unknown function invocation")
}


func (t *ReconChaincode) LoadTest(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	myLogger.Debug("LoadTest...")

	//sum := 0
	for i := 0; i < 100000; i++ {
		timestamp := time.Now()
		
		status := "Initiated"
		epayRefNum := "epayID_" + string(i)
		entityRefNum := "entityID_" + string(i)
		issuerRefNum := "issuerID_" + string(i)
		billNumber := "billNumber_" + string(i)
		billingCompany := "RTA"
		issuer := "Emirates NBD"
		amount := "100"
		batchId := ""
		datetime := timestamp.String()
		details := "Transaction Initiated on Ledger"
		
		var columns []*shim.Column
		col1 := shim.Column{Value: &shim.Column_String_{String_: status}}
		col2 := shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
		col3 := shim.Column{Value: &shim.Column_String_{String_: entityRefNum}}
		col4 := shim.Column{Value: &shim.Column_String_{String_: issuerRefNum}}
		col5 := shim.Column{Value: &shim.Column_String_{String_: billNumber}}
		col6 := shim.Column{Value: &shim.Column_String_{String_: billingCompany}}
		col7 := shim.Column{Value: &shim.Column_String_{String_: issuer}}	
		col8 := shim.Column{Value: &shim.Column_String_{String_: amount}}
		col9 := shim.Column{Value: &shim.Column_String_{String_: batchId}}
		col10 := shim.Column{Value: &shim.Column_String_{String_: datetime}}
		col11 := shim.Column{Value: &shim.Column_String_{String_: details}}
		columns = append(columns, &col1)
		columns = append(columns, &col2)
		columns = append(columns, &col3)
		columns = append(columns, &col4)
		columns = append(columns, &col5)
		columns = append(columns, &col6)
		columns = append(columns, &col7)
		columns = append(columns, &col8)
		columns = append(columns, &col9)
		columns = append(columns, &col10)
		columns = append(columns, &col11)
		row := shim.Row{Columns: columns}
		ok, err := stub.InsertRow("Reconciliation", row)

		if err != nil {
			return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
		}
		if !ok {
			return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
		}
			
		myLogger.Debug("Transaction Initiated on Ledger with entity reference number: .", entityRefNum)
	}
		
	return nil, nil
}


func (t *ReconChaincode) initiateTran(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	myLogger.Debug("initiateTran...")

	timestamp := time.Now()
		
	status := "Initiated"
	epayRefNum := args[0]
	entityRefNum := args[1]
	issuerRefNum := ""
	billNumber := args[2]
	billingCompany := args[3]
	issuer := ""
	amount := args[4]
	batchId := ""
	datetime := timestamp.String()
	details := "Transaction Initiated on Ledger"

	var columns []*shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: status}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: entityRefNum}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: issuerRefNum}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: billNumber}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: billingCompany}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: issuer}}	
	col8 := shim.Column{Value: &shim.Column_String_{String_: amount}}
	col9 := shim.Column{Value: &shim.Column_String_{String_: batchId}}
	col10 := shim.Column{Value: &shim.Column_String_{String_: datetime}}
	col11 := shim.Column{Value: &shim.Column_String_{String_: details}}
	columns = append(columns, &col1)
	columns = append(columns, &col2)
	columns = append(columns, &col3)
	columns = append(columns, &col4)
	columns = append(columns, &col5)
	columns = append(columns, &col6)
	columns = append(columns, &col7)
	columns = append(columns, &col8)
	columns = append(columns, &col9)
	columns = append(columns, &col10)
	columns = append(columns, &col11)
	row := shim.Row{Columns: columns}
	ok, err := stub.InsertRow("Reconciliation", row)

	if err != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
		
	myLogger.Debug("Transaction Initiated on Ledger with entity reference number: .", entityRefNum)
		
		
	return nil, nil
}

func (t *ReconChaincode) gatewayTranLeg1(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	myLogger.Debug("gatewayTranLeg1...")

	timestamp := time.Now()
	
	status := "Recieved"	
	epayRefNum := args[0]
	entityRefNum := args[1]
	issuerRefNum := ""
	billNumber := args[2]
	billingCompany := args[3]
	issuer := ""
	amount := args[4]
	batchId := ""
	datetime := ""
	details := ""
	
	
	var columns []shim.Column
	col1Val := "Initiated"
	col2Val := args[0]
	//col3Val := args[1]
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: col2Val}}
	//col3 := shim.Column{Value: &shim.Column_String_{String_: col3Val}}
	columns = append(columns, col1)	
	columns = append(columns, col2)	
	//columns = append(columns, col3)	
	row, err := stub.GetRow("Reconciliation", columns)
	myLogger.Debug("row", row)
	myLogger.Debug("len(row.Columns)", len(row.Columns))
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	

	if (entityRefNum != row.Columns[2].GetString_()) || (billNumber != row.Columns[4].GetString_()) ||  (amount != row.Columns[7].GetString_()) || (billingCompany != row.Columns[5].GetString_()) {
		return nil, errors.New("Unable to reconcile record.")
	}
	
	datetime = row.Columns[9].GetString_() + ", " + timestamp.String()
	details = row.Columns[10].GetString_() + ", " + "Transaction recieved on gateway"
	
	myLogger.Debug("before delete")
	err = stub.DeleteRow("Reconciliation", columns)
	if err != nil {
		return nil, fmt.Errorf("Recon operation failed. %s", err)
	}
	
	myLogger.Debug("after delete")
	
	var cols []*shim.Column
	col1 = shim.Column{Value: &shim.Column_String_{String_: status}}
	col2 = shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: entityRefNum}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: issuerRefNum}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: billNumber}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: billingCompany}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: issuer}}	
	col8 := shim.Column{Value: &shim.Column_String_{String_: amount}}
	col9 := shim.Column{Value: &shim.Column_String_{String_: batchId}}
	col10 := shim.Column{Value: &shim.Column_String_{String_: datetime}}
	col11 := shim.Column{Value: &shim.Column_String_{String_: details}}
	cols = append(cols, &col1)
	cols = append(cols, &col2)
	cols = append(cols, &col3)
	cols = append(cols, &col4)
	cols = append(cols, &col5)
	cols = append(cols, &col6)
	cols = append(cols, &col7)
	cols = append(cols, &col8)
	cols = append(cols, &col9)
	cols = append(cols, &col10)
	cols = append(cols, &col11)
	row = shim.Row{Columns: cols}
	ok, err := stub.InsertRow("Reconciliation", row)

	if err != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	myLogger.Debug("Insert: ", ok)
	
	return nil, nil
}

func (t *ReconChaincode) networkTran(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	myLogger.Debug("networkTran...")
	
	timestamp := time.Now()
		
	status := "Authorized"
	entityRefNum := ""
	epayRefNum := args[0]
	issuerRefNum := args[1]
	billNumber := args[2]
	billingCompany := args[3]
	issuer := args[4]
	amount := args[5]	
	batchId := ""
	datetime := ""
	details := ""
	
	myLogger.Debug("here 1")
	var columns []shim.Column
	col1Val := "Recieved"
	col2Val := args[0]
	//col3Val := args[1]
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: col2Val}}
	//col3 := shim.Column{Value: &shim.Column_String_{String_: col3Val}}
	columns = append(columns, col1)	
	columns = append(columns, col2)	
	//columns = append(columns, col3)	
	myLogger.Debug("col1Val: ", col1Val)
	myLogger.Debug("col2Val: ", col2Val)
	row, err := stub.GetRow("Reconciliation", columns)
	
	myLogger.Debug("row", row)
	myLogger.Debug("len(row.Columns)", len(row.Columns))
	if err != nil {
	 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	if len(row.Columns) < 1 {
	 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
		
	if (billNumber != row.Columns[4].GetString_()) ||  (amount != row.Columns[7].GetString_()) || (billingCompany != row.Columns[5].GetString_()) {
	return nil, errors.New("Unable to reconcile record.")
	}
	
	
	
	entityRefNum = row.Columns[2].GetString_()
	datetime = row.Columns[9].GetString_() + ", " + timestamp.String()
	details = row.Columns[10].GetString_() + ", " + "Transaction authorized by Issuer"
	
	
	myLogger.Debug("before delete")
	err = stub.DeleteRow("Reconciliation", columns)
	if err != nil {
	 return nil, fmt.Errorf("Recon operation failed. %s", err)
	}
	
	myLogger.Debug("after delete")
	
	var cols []*shim.Column
	col1 = shim.Column{Value: &shim.Column_String_{String_: status}}
	col2 = shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: entityRefNum}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: issuerRefNum}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: billNumber}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: billingCompany}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: issuer}}
	col8 := shim.Column{Value: &shim.Column_String_{String_: amount}}
	col9 := shim.Column{Value: &shim.Column_String_{String_: batchId}}
	col10 := shim.Column{Value: &shim.Column_String_{String_: datetime}}
	col11 := shim.Column{Value: &shim.Column_String_{String_: details}}
	cols = append(cols, &col1)
	cols = append(cols, &col2)
	cols = append(cols, &col3)
	cols = append(cols, &col4)
	cols = append(cols, &col5)
	cols = append(cols, &col6)
	cols = append(cols, &col7)
	cols = append(cols, &col8)
	cols = append(cols, &col9)
	cols = append(cols, &col10)
	cols = append(cols, &col11)
	
	row = shim.Row{Columns: cols}
	ok, err := stub.InsertRow("Reconciliation", row)

	if err != nil {
	 return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
	 return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	myLogger.Debug("Insert: ", ok)
	
	return nil, nil
	
}

func (t *ReconChaincode) gatewayTranLeg2(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	myLogger.Debug("gatewayTranLeg2...")
	
	timestamp := time.Now()
		
	status := "AuthRecieved"
	entityRefNum := ""
	epayRefNum := args[0]
	issuerRefNum := ""
	billNumber := args[1]
	billingCompany := args[2]
	issuer := ""
	amount := args[3]	
	batchId := ""
	datetime := ""
	details := ""
	
	var columns []shim.Column
	col1Val := "Authorized"
	col2Val := args[0]
	//col3Val := args[1]
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: col2Val}}
	//col3 := shim.Column{Value: &shim.Column_String_{String_: col3Val}}
	columns = append(columns, col1)	
	columns = append(columns, col2)	
	//columns = append(columns, col3)	
	row, err := stub.GetRow("Reconciliation", columns)
	myLogger.Debug("row", row)
	myLogger.Debug("len(row.Columns)", len(row.Columns))
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	
	
	if (billNumber != row.Columns[4].GetString_()) ||  (amount != row.Columns[7].GetString_()) || (billingCompany != row.Columns[5].GetString_()) {
		return nil, errors.New("Unable to reconcile record.")
	}
	entityRefNum = row.Columns[2].GetString_()
	issuerRefNum = row.Columns[3].GetString_()
	issuer = row.Columns[6].GetString_()
	datetime = row.Columns[9].GetString_() + ", " + timestamp.String()
	details = row.Columns[10].GetString_() + ", " + "Authorization recieved at gateway"
	
	myLogger.Debug("before delete")
	err = stub.DeleteRow("Reconciliation", columns)
	if err != nil {
		return nil, fmt.Errorf("Recon operation failed. %s", err)
	}
	
	myLogger.Debug("after delete")
	
	var cols []*shim.Column
	col1 = shim.Column{Value: &shim.Column_String_{String_: status}}
	col2 = shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: entityRefNum}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: issuerRefNum}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: billNumber}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: billingCompany}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: issuer}}
	col8 := shim.Column{Value: &shim.Column_String_{String_: amount}}
	col9 := shim.Column{Value: &shim.Column_String_{String_: batchId}}
	col10 := shim.Column{Value: &shim.Column_String_{String_: datetime}}
	col11 := shim.Column{Value: &shim.Column_String_{String_: details}}
	cols = append(cols, &col1)
	cols = append(cols, &col2)
	cols = append(cols, &col3)
	cols = append(cols, &col4)
	cols = append(cols, &col5)
	cols = append(cols, &col6)
	cols = append(cols, &col7)
	cols = append(cols, &col8)
	cols = append(cols, &col9)
	cols = append(cols, &col10)
	cols = append(cols, &col11)
	
	row = shim.Row{Columns: cols}
	ok, err := stub.InsertRow("Reconciliation", row)

	if err != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	myLogger.Debug("Insert: ", ok)
	
	return nil, nil
}

func (t *ReconChaincode) reconcileTran(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	myLogger.Debug("reconcileTran...")

	timestamp := time.Now()
	
	status := "Reconciled"	
	epayRefNum := args[0]
	entityRefNum := args[1]
	issuerRefNum := ""
	billNumber := args[2]
	billingCompany := args[3]
	issuer := ""
	amount := args[4]
	batchId := ""
	datetime := ""
	details := ""
	
	
	var columns []shim.Column
	col1Val := "AuthRecieved"
	col2Val := args[0]
	//col3Val := args[1]
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: col2Val}}
	//col3 := shim.Column{Value: &shim.Column_String_{String_: col3Val}}
	columns = append(columns, col1)	
	columns = append(columns, col2)	
	//columns = append(columns, col3)	
	row, err := stub.GetRow("Reconciliation", columns)
	myLogger.Debug("row", row)
	myLogger.Debug("len(row.Columns)", len(row.Columns))
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	

	if (billNumber != row.Columns[4].GetString_()) ||  (amount != row.Columns[7].GetString_()) || (billingCompany != row.Columns[5].GetString_()) {
		return nil, errors.New("Unable to reconcile record.")
	}

	issuerRefNum = row.Columns[3].GetString_()
	issuer = row.Columns[6].GetString_()
	datetime = row.Columns[9].GetString_() + ", " + timestamp.String()
	details = row.Columns[10].GetString_() + ", " + "Transaction Reconciled"
	
	myLogger.Debug("before delete")
	err = stub.DeleteRow("Reconciliation", columns)
	if err != nil {
		return nil, fmt.Errorf("Recon operation failed. %s", err)
	}
	
	myLogger.Debug("after delete")
	
	var cols []*shim.Column
	col1 = shim.Column{Value: &shim.Column_String_{String_: status}}
	col2 = shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: entityRefNum}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: issuerRefNum}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: billNumber}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: billingCompany}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: issuer}}
	col8 := shim.Column{Value: &shim.Column_String_{String_: amount}}
	col9 := shim.Column{Value: &shim.Column_String_{String_: batchId}}
	col10 := shim.Column{Value: &shim.Column_String_{String_: datetime}}
	col11 := shim.Column{Value: &shim.Column_String_{String_: details}}
	cols = append(cols, &col1)
	cols = append(cols, &col2)
	cols = append(cols, &col3)
	cols = append(cols, &col4)
	cols = append(cols, &col5)
	cols = append(cols, &col6)
	cols = append(cols, &col7)
	cols = append(cols, &col8)
	cols = append(cols, &col9)
	cols = append(cols, &col10)
	cols = append(cols, &col11)
	
	row = shim.Row{Columns: cols}
	ok, err := stub.InsertRow("Reconciliation", row)

	if err != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	myLogger.Debug("Insert: ", ok)
	
	return nil, nil
}

func (t *ReconChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error)  {
	if function == "GetTranByStatus" {
		return t.GetTranByStatus(stub, args)
	} else if function == "GetAllTran" {
		return t.GetAllTran(stub, args)
	} else if function == "GetCounts" {
		return t.GetCounts(stub, args)
	} else if function == "GetTranByStatus2" {
		return t.GetTranByStatus2(stub, args)
	} else if function == "GetAllTran2" {
		return t.GetAllTran2(stub, args)
	} else if function == "GetTranByEpayID" {
		return t.GetTranByEpayID(stub, args)
	}
	
	
	
	return nil, errors.New("Received unknown function invocation")
}

func (t *ReconChaincode) GetTranByStatus(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}
	
	var columns []shim.Column
	col1Val := args[0]
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	columns = append(columns, col1)
			
	
	rowChannel, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowChannel)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}

	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
			}
		}
		if rowChannel == nil {
			break
		}
	}

	jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return jsonRows, nil
}

func (t *ReconChaincode) GetTranByStatus2(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}
	
	var columns []shim.Column
	col1Val := args[0]
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	columns = append(columns, col1)
			
	
	rowChannel, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowChannel)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}
	
	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
			}
		}
		if rowChannel == nil {
			break
		}
	}

	reconStruct := ReconciliationStruct{
		Status: rows[0].Columns[0].GetString_(),
		EpayRefNum: rows[0].Columns[1].GetString_(),
		EntityRefNum: rows[0].Columns[2].GetString_(),
		IssuerRefNum: rows[0].Columns[3].GetString_(),
		BillNumber: rows[0].Columns[4].GetString_(),
		BillingCompany: rows[0].Columns[5].GetString_(),
		Issuer: rows[0].Columns[7].GetString_(),
		Amount: rows[0].Columns[6].GetString_(),
		BatchID: rows[0].Columns[8].GetString_(),
		DateTime: rows[0].Columns[9].GetString_(),
		Details: rows[0].Columns[10].GetString_(),
	}
	reconBytes, err := json.Marshal(reconStruct)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	myLogger.Debug("reconStruct: ",reconStruct)
	myLogger.Debug("reconBytes: ",reconBytes)
	myLogger.Debug("before marshal Rows: ",rows)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return reconBytes, nil
}

func (t *ReconChaincode) GetAllTran(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	var columns []shim.Column
	
	rowChannel, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowChannel)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}

	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
			}
		}
		if rowChannel == nil {
			break
		}
	}

	jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return jsonRows, nil
}

func (t *ReconChaincode) GetAllTran2(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	var columns []shim.Column
	
	rowChannel, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowChannel)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}
	var Transactions []ReconciliationStruct
	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
				
				reconStruct := ReconciliationStruct{
				Status: row.Columns[0].GetString_(),
				EpayRefNum: row.Columns[1].GetString_(),
				EntityRefNum: row.Columns[2].GetString_(),
				IssuerRefNum: row.Columns[3].GetString_(),
				BillNumber: row.Columns[4].GetString_(),
				BillingCompany: row.Columns[5].GetString_(),
				Issuer: row.Columns[7].GetString_(),
				Amount: row.Columns[6].GetString_(),
				BatchID: row.Columns[8].GetString_(),
				DateTime: row.Columns[9].GetString_(),
				Details: row.Columns[10].GetString_(),
				}
				Transactions = append(Transactions, reconStruct)				
			}
		}
		if rowChannel == nil {
			break
		}
	}
	
	reconBytes, err := json.Marshal(Transactions)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	myLogger.Debug("reconStruct: ",Transactions)
	myLogger.Debug("reconBytes: ",reconBytes)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return reconBytes, nil
}

func (t *ReconChaincode) GetCounts(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	var cols1 []shim.Column	
	total, err := stub.GetRows("Reconciliation", cols1)		
	var totalRows []shim.Row
	for {
		select {
		case row, ok := <-total:
			if !ok {
				total = nil
			} else {
				totalRows = append(totalRows, row)
			}
		}
		if total == nil {
			break
		}
	}
	myLogger.Debug("Total number of transactions: ",len(totalRows))
	
	
	var cols2 []shim.Column
	cols2 = append(cols2, shim.Column{Value: &shim.Column_String_{String_: "Initiated"}})	
	initiated, err := stub.GetRows("Reconciliation", cols2)
	var initiatedRows []shim.Row
	for {
		select {
		case row, ok := <-initiated:
			if !ok {
				initiated = nil
			} else {
				initiatedRows = append(initiatedRows, row)
			}
		}
		if initiated == nil {
			break
		}
	}
	
	var cols3 []shim.Column
	cols3 = append(cols3, shim.Column{Value: &shim.Column_String_{String_: "Recieved"}})	
	recieved, err := stub.GetRows("Reconciliation", cols3)
	var recievedRows []shim.Row
	for {
		select {
		case row, ok := <-recieved:
			if !ok {
				recieved = nil
			} else {
				recievedRows = append(recievedRows, row)
			}
		}
		if recieved == nil {
			break
		}
	}
	
	var cols4 []shim.Column
	cols4 = append(cols4, shim.Column{Value: &shim.Column_String_{String_: "Authorized"}})	
	authorized, err := stub.GetRows("Reconciliation", cols4)
	var authorizedRows []shim.Row
	for {
		select {
		case row, ok := <-authorized:
			if !ok {
				authorized = nil
			} else {
				authorizedRows = append(authorizedRows, row)
			}
		}
		if authorized == nil {
			break
		}
	}
	
	var cols5 []shim.Column
	cols5 = append(cols5, shim.Column{Value: &shim.Column_String_{String_: "AuthRecieved"}})	
	authRecieved, err := stub.GetRows("Reconciliation", cols5)
	var authRecievedRows []shim.Row
	for {
		select {
		case row, ok := <-authRecieved:
			if !ok {
				authRecieved = nil
			} else {
				authRecievedRows = append(authRecievedRows, row)
			}
		}
		if authRecieved == nil {
			break
		}
	}
	
	var cols6 []shim.Column
	cols6 = append(cols6, shim.Column{Value: &shim.Column_String_{String_: "Reconciled"}})	
	reconciled, err := stub.GetRows("Reconciliation", cols6)
	var reconciledRows []shim.Row
	for {
		select {
		case row, ok := <-reconciled:
			if !ok {
				reconciled = nil
			} else {
				reconciledRows = append(reconciledRows, row)
			}
		}
		if reconciled == nil {
			break
		}
	}
	
	var cols7 []shim.Column
	cols7 = append(cols7, shim.Column{Value: &shim.Column_String_{String_: "Settled"}})	
	settled, err := stub.GetRows("Reconciliation", cols7)
	var settledRows []shim.Row
	for {
		select {
		case row, ok := <-settled:
			if !ok {
				settled = nil
			} else {
				settledRows = append(settledRows, row)
			}
		}
		if settled == nil {
			break
		}
	}
	
	myLogger.Debug("Total number of transactions: ",len(totalRows))
	myLogger.Debug("Number of initiated transactions: ",len(initiatedRows))
	myLogger.Debug("Number of recieved transactions: ",len(recievedRows))
	myLogger.Debug("Number of authorized transactions: ",len(authorizedRows))
	myLogger.Debug("Number of authRecieved transactions: ",len(authRecievedRows))
	myLogger.Debug("Number of reconciled transactions: ",len(reconciledRows))
	myLogger.Debug("Number of settled transactions: ",len(settledRows))

	tranC := TranCounts{
		Total: len(totalRows),
		Initiated: len(initiatedRows),
		Recieved: len(recievedRows),
		Authorized: len(authorizedRows),
		AuthRecieved: len(authRecievedRows),		
		Reconciled: len(reconciledRows),
		Settled: len(settledRows),
	}
	
	tranBytes, err := json.Marshal(tranC)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}

	return tranBytes, nil
}

func (t *ReconChaincode) GetTranByEpayID(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}
	
	var row shim.Row
	var st []string
	
	st = append(st, "Initiated")
	st = append(st, "Recieved")
	st = append(st, "Authorized")
	st = append(st, "AuthRecieved")
	st = append(st, "Reconciled")
	st = append(st, "Settled")
	
	for i, v := range st {
		var columns []shim.Column
		col1Val := v
		col2Val := args[0]
		myLogger.Debug("i: ", i)
		myLogger.Debug("col1Val: ", col1Val)
		myLogger.Debug("col2Val: ", col2Val)
		col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
		col2 := shim.Column{Value: &shim.Column_String_{String_: col2Val}}
		columns = append(columns, col1)	
		columns = append(columns, col2)	
		row1, err := stub.GetRow("Reconciliation", columns)
		myLogger.Debug("len(row1.Columns): ", len(row1.Columns))
		if err != nil {
		 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
		}
		if len(row1.Columns) > 1 {
			row = row1
			break
		}
	}
	if (len(row.Columns) < 1){
		return nil, nil
	}
	reconStruct := ReconciliationStruct{
		Status: row.Columns[0].GetString_(),
		EpayRefNum: row.Columns[1].GetString_(),
		EntityRefNum: row.Columns[2].GetString_(),
		IssuerRefNum: row.Columns[3].GetString_(),
		BillNumber: row.Columns[4].GetString_(),
		BillingCompany: row.Columns[5].GetString_(),
		Issuer: row.Columns[7].GetString_(),
		Amount: row.Columns[6].GetString_(),
		BatchID: row.Columns[8].GetString_(),
		DateTime: row.Columns[9].GetString_(),
		Details: row.Columns[10].GetString_(),
	}
	reconBytes, err := json.Marshal(reconStruct)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	myLogger.Debug("reconStruct: ",reconStruct)
	myLogger.Debug("reconBytes: ",reconBytes)
	myLogger.Debug("before marshal Rows: ",row)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return reconBytes, nil
}

func (t *ReconChaincode) CreateBatch(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: "Reconciled"}}
	columns = append(columns, col1)
	
	rowsToUpdate, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowsToUpdate)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}
	
	if (len(rowsToUpdate) > 0) {
		
		var rows []shim.Row
		var totalAmount int
		var issuerBank string
		var billCompany string
			
		batchID := "Batch_" + strconv.Itoa(rand.Intn(100000))
		timestamp := time.Now()
		
		for {
			select {
			case row, ok := <-rowsToUpdate:
				if !ok {
					rowsToUpdate = nil
				} else {
					if row.Columns[8].GetString_() == "" {
						rows = append(rows, row)
						tempAmount, err := strconv.Atoi(row.Columns[7].GetString_())
						if err != nil{ 
							return nil, fmt.Errorf("Error encountered.")
						}
						totalAmount = totalAmount + tempAmount
						issuerBank = row.Columns[6].GetString_()
						billCompany = row.Columns[5].GetString_()
						
						status := "BatchInitiated"	
						epayRefNum := row.Columns[1].GetString_()
						entityRefNum := row.Columns[2].GetString_()
						issuerRefNum := row.Columns[3].GetString_()
						billNumber := row.Columns[4].GetString_()
						billingCompany := row.Columns[5].GetString_()
						issuer := row.Columns[6].GetString_()
						amount := row.Columns[7].GetString_()
						batchId := batchID
						datetime := row.Columns[9].GetString_() + ", " + timestamp.String()
						details := row.Columns[10].GetString_() + ", " + "Batch Initiated"
						
						myLogger.Debug("before delete")
						err = stub.DeleteRow("Reconciliation", columns)
						if err != nil {
							return nil, fmt.Errorf("Recon operation failed. %s", err)
						}
						
						myLogger.Debug("after delete")
						
						var cols []*shim.Column
						col1 := shim.Column{Value: &shim.Column_String_{String_: status}}
						col2 := shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
						col3 := shim.Column{Value: &shim.Column_String_{String_: entityRefNum}}
						col4 := shim.Column{Value: &shim.Column_String_{String_: issuerRefNum}}
						col5 := shim.Column{Value: &shim.Column_String_{String_: billNumber}}
						col6 := shim.Column{Value: &shim.Column_String_{String_: billingCompany}}
						col7 := shim.Column{Value: &shim.Column_String_{String_: issuer}}
						col8 := shim.Column{Value: &shim.Column_String_{String_: amount}}
						col9 := shim.Column{Value: &shim.Column_String_{String_: batchId}}
						col10 := shim.Column{Value: &shim.Column_String_{String_: datetime}}
						col11 := shim.Column{Value: &shim.Column_String_{String_: details}}
						cols = append(cols, &col1)
						cols = append(cols, &col2)
						cols = append(cols, &col3)
						cols = append(cols, &col4)
						cols = append(cols, &col5)
						cols = append(cols, &col6)
						cols = append(cols, &col7)
						cols = append(cols, &col8)
						cols = append(cols, &col9)
						cols = append(cols, &col10)
						cols = append(cols, &col11)
						
						row = shim.Row{Columns: cols}
						ok, err := stub.InsertRow("Reconciliation", row)

						if err != nil {
							return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
						}
						if !ok {
							return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
						}
						myLogger.Debug("Insert: ", ok)					
					}
				}
			}
			if rowsToUpdate == nil {
				break
			}
		}
		
		// start - batch creation
								
		var columns []*shim.Column
		col1 := shim.Column{Value: &shim.Column_String_{String_: batchID}}
		col2 := shim.Column{Value: &shim.Column_String_{String_: billCompany}}
		col3 := shim.Column{Value: &shim.Column_String_{String_: issuerBank}}
		col4 := shim.Column{Value: &shim.Column_String_{String_: strconv.Itoa(totalAmount)}}
		col5 := shim.Column{Value: &shim.Column_String_{String_: "BatchInitiated"}}
		col6 := shim.Column{Value: &shim.Column_String_{String_: timestamp.String()}}
		col7 := shim.Column{Value: &shim.Column_String_{String_: "Setllement Batch Initiated"}}	
		columns = append(columns, &col1)
		columns = append(columns, &col2)
		columns = append(columns, &col3)
		columns = append(columns, &col4)
		columns = append(columns, &col5)
		columns = append(columns, &col6)
		columns = append(columns, &col7)
		row := shim.Row{Columns: columns}
		ok, err := stub.InsertRow("Batch", row)

		if err != nil {
			return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
		}
		if !ok {
			return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
		}
			
		myLogger.Debug("Setllement Batch initiated on Ledger with Batch ID: .", batchID)
			
		// end - batch creation
		
		return nil, nil
	}
	return nil, nil
}

func (t *ReconChaincode) UpdateInitiatedBatch(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: "BatchInitiated"}}
	columns = append(columns, col1)
	
	rowsToUpdate, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowsToUpdate)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}
	
	if (len(rowsToUpdate) > 0) {
		
		var rows []shim.Row
			
		//batchID := "Batch_" + strconv.Itoa(rand.Intn(100000))
		timestamp := time.Now()
		
		for {
			select {
			case row, ok := <-rowsToUpdate:
				if !ok {
					rowsToUpdate = nil
				} else {
					if row.Columns[8].GetString_() == args[0] {
						rows = append(rows, row)
												
						status := "SettlementInitiated"	
						epayRefNum := row.Columns[1].GetString_()
						entityRefNum := row.Columns[2].GetString_()
						issuerRefNum := row.Columns[3].GetString_()
						billNumber := row.Columns[4].GetString_()
						billingCompany := row.Columns[5].GetString_()
						issuer := row.Columns[6].GetString_()
						amount := row.Columns[7].GetString_()
						batchId := row.Columns[8].GetString_()
						datetime := row.Columns[9].GetString_() + ", " + timestamp.String()
						details := row.Columns[10].GetString_() + ", " + "Settlement Initiated"
						
						myLogger.Debug("before delete")
						err = stub.DeleteRow("Reconciliation", columns)
						if err != nil {
							return nil, fmt.Errorf("Recon operation failed. %s", err)
						}
						
						myLogger.Debug("after delete")
						
						var cols []*shim.Column
						col1 := shim.Column{Value: &shim.Column_String_{String_: status}}
						col2 := shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
						col3 := shim.Column{Value: &shim.Column_String_{String_: entityRefNum}}
						col4 := shim.Column{Value: &shim.Column_String_{String_: issuerRefNum}}
						col5 := shim.Column{Value: &shim.Column_String_{String_: billNumber}}
						col6 := shim.Column{Value: &shim.Column_String_{String_: billingCompany}}
						col7 := shim.Column{Value: &shim.Column_String_{String_: issuer}}
						col8 := shim.Column{Value: &shim.Column_String_{String_: amount}}
						col9 := shim.Column{Value: &shim.Column_String_{String_: batchId}}
						col10 := shim.Column{Value: &shim.Column_String_{String_: datetime}}
						col11 := shim.Column{Value: &shim.Column_String_{String_: details}}
						cols = append(cols, &col1)
						cols = append(cols, &col2)
						cols = append(cols, &col3)
						cols = append(cols, &col4)
						cols = append(cols, &col5)
						cols = append(cols, &col6)
						cols = append(cols, &col7)
						cols = append(cols, &col8)
						cols = append(cols, &col9)
						cols = append(cols, &col10)
						cols = append(cols, &col11)
						
						row = shim.Row{Columns: cols}
						ok, err := stub.InsertRow("Reconciliation", row)

						if err != nil {
							return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
						}
						if !ok {
							return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
						}
						myLogger.Debug("Insert: ", ok)					
					}
				}
			}
			if rowsToUpdate == nil {
				break
			}
		}		
		
		
		// start - batch updation
							

		var cols []shim.Column
		col1Value := args[0]
		column1 := shim.Column{Value: &shim.Column_String_{String_: col1Value}}
		cols = append(cols, column1)	
		row, err := stub.GetRow("Batch", columns)
		myLogger.Debug("row", row)
		myLogger.Debug("len(row.Columns)", len(row.Columns))
		if err != nil {
			return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
		}
		
		tempBatchID := row.Columns[0].GetString_()
		tempBillCompany := row.Columns[1].GetString_()
		tempIssuerBank := row.Columns[2].GetString_()
		tempAmount := row.Columns[3].GetString_()
		tempStatus := "SettlementInitiated"
		tempDateTime := row.Columns[5].GetString_() + ", " + timestamp.String()
		tempDetails := row.Columns[6].GetString_() + ", " + "Settlement Initiated"
		
		
		myLogger.Debug("before delete")
		err = stub.DeleteRow("Batch", cols)
		if err != nil {
			return nil, fmt.Errorf("Recon operation failed. %s", err)
		}
		
		myLogger.Debug("after delete")
	
	
		var columns []*shim.Column
		col1 := shim.Column{Value: &shim.Column_String_{String_: tempBatchID}}
		col2 := shim.Column{Value: &shim.Column_String_{String_: tempBillCompany}}
		col3 := shim.Column{Value: &shim.Column_String_{String_: tempIssuerBank}}
		col4 := shim.Column{Value: &shim.Column_String_{String_: tempAmount}}
		col5 := shim.Column{Value: &shim.Column_String_{String_: tempStatus}}
		col6 := shim.Column{Value: &shim.Column_String_{String_: tempDateTime}}
		col7 := shim.Column{Value: &shim.Column_String_{String_: tempDetails}}	
		columns = append(columns, &col1)
		columns = append(columns, &col2)
		columns = append(columns, &col3)
		columns = append(columns, &col4)
		columns = append(columns, &col5)
		columns = append(columns, &col6)
		columns = append(columns, &col7)
		newRow := shim.Row{Columns: columns}
		ok, err := stub.InsertRow("Batch", newRow)

		if err != nil {
			return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
		}
		if !ok {
			return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
		}
			
		myLogger.Debug("Setllement initiated on Ledger for Batch ID: .", tempBatchID)
			
		// end - batch updation
		
		return nil, nil
	}
	return nil, nil
}

func (t *ReconChaincode) SettleBatch(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: "SettlementInitiated"}}
	columns = append(columns, col1)
	
	rowsToUpdate, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowsToUpdate)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}
	
	if (len(rowsToUpdate) > 0) {
		
		var rows []shim.Row
			
		//batchID := "Batch_" + strconv.Itoa(rand.Intn(100000))
		timestamp := time.Now()
		
		for {
			select {
			case row, ok := <-rowsToUpdate:
				if !ok {
					rowsToUpdate = nil
				} else {
					if row.Columns[8].GetString_() == args[0] {
						rows = append(rows, row)
												
						status := "Settled"	
						epayRefNum := row.Columns[1].GetString_()
						entityRefNum := row.Columns[2].GetString_()
						issuerRefNum := row.Columns[3].GetString_()
						billNumber := row.Columns[4].GetString_()
						billingCompany := row.Columns[5].GetString_()
						issuer := row.Columns[6].GetString_()
						amount := row.Columns[7].GetString_()
						batchId := row.Columns[8].GetString_()
						datetime := row.Columns[9].GetString_() + ", " + timestamp.String()
						details := row.Columns[10].GetString_() + ", " + "Settled"
						
						myLogger.Debug("before delete")
						err = stub.DeleteRow("Reconciliation", columns)
						if err != nil {
							return nil, fmt.Errorf("Recon operation failed. %s", err)
						}
						
						myLogger.Debug("after delete")
						
						var cols []*shim.Column
						col1 := shim.Column{Value: &shim.Column_String_{String_: status}}
						col2 := shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
						col3 := shim.Column{Value: &shim.Column_String_{String_: entityRefNum}}
						col4 := shim.Column{Value: &shim.Column_String_{String_: issuerRefNum}}
						col5 := shim.Column{Value: &shim.Column_String_{String_: billNumber}}
						col6 := shim.Column{Value: &shim.Column_String_{String_: billingCompany}}
						col7 := shim.Column{Value: &shim.Column_String_{String_: issuer}}
						col8 := shim.Column{Value: &shim.Column_String_{String_: amount}}
						col9 := shim.Column{Value: &shim.Column_String_{String_: batchId}}
						col10 := shim.Column{Value: &shim.Column_String_{String_: datetime}}
						col11 := shim.Column{Value: &shim.Column_String_{String_: details}}
						cols = append(cols, &col1)
						cols = append(cols, &col2)
						cols = append(cols, &col3)
						cols = append(cols, &col4)
						cols = append(cols, &col5)
						cols = append(cols, &col6)
						cols = append(cols, &col7)
						cols = append(cols, &col8)
						cols = append(cols, &col9)
						cols = append(cols, &col10)
						cols = append(cols, &col11)
						
						row = shim.Row{Columns: cols}
						ok, err := stub.InsertRow("Reconciliation", row)

						if err != nil {
							return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
						}
						if !ok {
							return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
						}
						myLogger.Debug("Insert: ", ok)					
					}
				}
			}
			if rowsToUpdate == nil {
				break
			}
		}		
		
		
		// start - batch updation
							

		var cols []shim.Column
		col1Value := args[0]
		column1 := shim.Column{Value: &shim.Column_String_{String_: col1Value}}
		cols = append(cols, column1)	
		row, err := stub.GetRow("Batch", columns)
		myLogger.Debug("row", row)
		myLogger.Debug("len(row.Columns)", len(row.Columns))
		if err != nil {
			return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
		}
		
		tempBatchID := row.Columns[0].GetString_()
		tempBillCompany := row.Columns[1].GetString_()
		tempIssuerBank := row.Columns[2].GetString_()
		tempAmount := row.Columns[3].GetString_()
		tempStatus := "Settled"
		tempDateTime := row.Columns[5].GetString_() + ", " + timestamp.String()
		tempDetails := row.Columns[6].GetString_() + ", " + "Batch Settled"
		
		
		myLogger.Debug("before delete")
		err = stub.DeleteRow("Batch", cols)
		if err != nil {
			return nil, fmt.Errorf("Recon operation failed. %s", err)
		}
		
		myLogger.Debug("after delete")
	
	
		var columns []*shim.Column
		col1 := shim.Column{Value: &shim.Column_String_{String_: tempBatchID}}
		col2 := shim.Column{Value: &shim.Column_String_{String_: tempBillCompany}}
		col3 := shim.Column{Value: &shim.Column_String_{String_: tempIssuerBank}}
		col4 := shim.Column{Value: &shim.Column_String_{String_: tempAmount}}
		col5 := shim.Column{Value: &shim.Column_String_{String_: tempStatus}}
		col6 := shim.Column{Value: &shim.Column_String_{String_: tempDateTime}}
		col7 := shim.Column{Value: &shim.Column_String_{String_: tempDetails}}	
		columns = append(columns, &col1)
		columns = append(columns, &col2)
		columns = append(columns, &col3)
		columns = append(columns, &col4)
		columns = append(columns, &col5)
		columns = append(columns, &col6)
		columns = append(columns, &col7)
		newRow := shim.Row{Columns: columns}
		ok, err := stub.InsertRow("Batch", newRow)

		if err != nil {
			return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
		}
		if !ok {
			return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
		}
			
		myLogger.Debug("Setllement done on Ledger for Batch ID: .", tempBatchID)
			
		// end - batch updation
		
		return nil, nil
	}
	return nil, nil
}

func main() {
	
	primitives.SetSecurityLevel("SHA3", 256)
	err := shim.Start(new(ReconChaincode))
	if err != nil {
		fmt.Printf("Error starting Chaincode: %s", err)
	}
}
