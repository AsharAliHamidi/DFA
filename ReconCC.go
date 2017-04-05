package main

import (
	"strconv"
	"errors"
	"fmt"
	"time"
	"encoding/json"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/crypto/primitives"
	"math/rand"
	"strings"
)

var myLogger = shim.NewLogger("Reconciliation")
var RowsToFetch = 100

func main() {
	
	primitives.SetSecurityLevel("SHA3", 256)
	err := shim.Start(new(ReconChaincode))
	if err != nil {
		fmt.Printf("Error starting Chaincode: %s", err)
	}
}


//////////////// STRUCTS AND HELPER FUNCTIONS ////////////////


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

type BatchStruct struct {
	BatchID string `json:"BatchID"`
	BillingCompany string `json:"BillingCompany"`
	Issuer string `json:"Issuer"`
	Amount string `json:"Amount"`
	Status string `json:"Status"`
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
	BatchInitiated int `json:"BatchInitiated"`
	SettlementInitiated int `json:"SettlementInitiated"`
	Settled int `json:"Settled"`
	Rejected int `json:"Rejected"`
}

type TranAmounts struct {
	Total int `json:"Total"`
	Initiated int `json:"Initiated"`
	Recieved int `json:"Recieved"`
	Authorized int `json:"Authorized"`
	AuthRecieved int `json:"AuthRecieved"`
	Reconciled int `json:"Reconciled"`
	BatchInitiated int `json:"BatchInitiated"`
	SettlementInitiated int `json:"SettlementInitiated"`
	Settled int `json:"Settled"`
	Rejected int `json:"Rejected"`
}

type TranCountsEntity struct {
	RTA int `json:"RTA"`
	DEWA int `json:"DEWA"`
	Etisalat int `json:"Etisalat"`
	DU int `json:"DU"`
	DubaiCustoms int `json:"DubaiCustoms"`
	Others int `json:"Others"`
}

type TranStatus struct {
	Id string `json:"Id"`
	Details string `json:"Details"`
	Status string `json:"Status"`
}

type RequestStatus struct {
	Id string `json:"Id"`
	Details string `json:"Details"`
	Status string `json:"Status"`	
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
	// col12 := shim.ColumnDefinition{Name: "EntityFlag1", Type: shim.ColumnDefinition_STRING, Key: false}
	// col13 := shim.ColumnDefinition{Name: "EpayFlag1", Type: shim.ColumnDefinition_STRING, Key: false}
	// col14 := shim.ColumnDefinition{Name: "IssuerFlag", Type: shim.ColumnDefinition_STRING, Key: false}
	// col15 := shim.ColumnDefinition{Name: "EpayFlag2", Type: shim.ColumnDefinition_STRING, Key: false}
	// col16 := shim.ColumnDefinition{Name: "EntityFlag2", Type: shim.ColumnDefinition_STRING, Key: false}		
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
	// colDefs = append(colDefs, &col12)
	// colDefs = append(colDefs, &col13)
	// colDefs = append(colDefs, &col14)
	// colDefs = append(colDefs, &col15)
	// colDefs = append(colDefs, &col16)
	return stub.CreateTable("Reconciliation", colDefs)
}

func createTableReconTemp(stub shim.ChaincodeStubInterface) error {
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "Status", Type: shim.ColumnDefinition_STRING, Key: false}
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
	return stub.CreateTable("ReconTemp", colDefs)
}

func createTableBatch(stub shim.ChaincodeStubInterface) error {
	// Create table one
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "BatchID", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "BillingCompany", Type: shim.ColumnDefinition_STRING, Key: false}
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

func createTableStatusCounts(stub shim.ChaincodeStubInterface) error {
		
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "Id", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "Total", Type: shim.ColumnDefinition_STRING, Key: false}
	col3 := shim.ColumnDefinition{Name: "Initiated", Type: shim.ColumnDefinition_STRING, Key: false}
	col4 := shim.ColumnDefinition{Name: "Recieved", Type: shim.ColumnDefinition_STRING, Key: false}
	col5 := shim.ColumnDefinition{Name: "Authorized", Type: shim.ColumnDefinition_STRING, Key: false}
	col6 := shim.ColumnDefinition{Name: "AuthRecieved", Type: shim.ColumnDefinition_STRING, Key: false}
	col7 := shim.ColumnDefinition{Name: "Reconciled", Type: shim.ColumnDefinition_STRING, Key: false}
	col8 := shim.ColumnDefinition{Name: "BatchInitiated", Type: shim.ColumnDefinition_STRING, Key: false}
	col9 := shim.ColumnDefinition{Name: "SettlementInitiated", Type: shim.ColumnDefinition_STRING, Key: false}		
	col10 := shim.ColumnDefinition{Name: "Settled", Type: shim.ColumnDefinition_STRING, Key: false}		
	col11 := shim.ColumnDefinition{Name: "Rejected", Type: shim.ColumnDefinition_STRING, Key: false}		
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
	return stub.CreateTable("StatusCounts", colDefs)
}

func createTableCompanyCounts(stub shim.ChaincodeStubInterface) error {
	// Create table one
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "Id", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "RTA", Type: shim.ColumnDefinition_STRING, Key: false}
	col3 := shim.ColumnDefinition{Name: "Dewa", Type: shim.ColumnDefinition_STRING, Key: false}
	col4 := shim.ColumnDefinition{Name: "DU", Type: shim.ColumnDefinition_STRING, Key: false}
	col5 := shim.ColumnDefinition{Name: "Etisalat", Type: shim.ColumnDefinition_STRING, Key: false}
	col6 := shim.ColumnDefinition{Name: "DubaiCustoms", Type: shim.ColumnDefinition_STRING, Key: false}
	col7 := shim.ColumnDefinition{Name: "Others", Type: shim.ColumnDefinition_STRING, Key: false}
	colDefs = append(colDefs, &col1)
	colDefs = append(colDefs, &col2)
	colDefs = append(colDefs, &col3)
	colDefs = append(colDefs, &col4)
	colDefs = append(colDefs, &col5)
	colDefs = append(colDefs, &col6)
	colDefs = append(colDefs, &col7)
	return stub.CreateTable("CompanyCounts", colDefs)
}

func createTableTranAmounts(stub shim.ChaincodeStubInterface) error {
		
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "Id", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "Total", Type: shim.ColumnDefinition_STRING, Key: false}
	col3 := shim.ColumnDefinition{Name: "Initiated", Type: shim.ColumnDefinition_STRING, Key: false}
	col4 := shim.ColumnDefinition{Name: "Recieved", Type: shim.ColumnDefinition_STRING, Key: false}
	col5 := shim.ColumnDefinition{Name: "Authorized", Type: shim.ColumnDefinition_STRING, Key: false}
	col6 := shim.ColumnDefinition{Name: "AuthRecieved", Type: shim.ColumnDefinition_STRING, Key: false}
	col7 := shim.ColumnDefinition{Name: "Reconciled", Type: shim.ColumnDefinition_STRING, Key: false}
	col8 := shim.ColumnDefinition{Name: "BatchInitiated", Type: shim.ColumnDefinition_STRING, Key: false}
	col9 := shim.ColumnDefinition{Name: "SettlementInitiated", Type: shim.ColumnDefinition_STRING, Key: false}		
	col10 := shim.ColumnDefinition{Name: "Settled", Type: shim.ColumnDefinition_STRING, Key: false}		
	col11 := shim.ColumnDefinition{Name: "Rejected", Type: shim.ColumnDefinition_STRING, Key: false}		
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
	return stub.CreateTable("TranAmounts", colDefs)
}

func createTableTranStatus(stub shim.ChaincodeStubInterface) error {
		
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "Id", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "Details", Type: shim.ColumnDefinition_STRING, Key: false}
	col3 := shim.ColumnDefinition{Name: "Status", Type: shim.ColumnDefinition_STRING, Key: false}
	colDefs = append(colDefs, &col1)
	colDefs = append(colDefs, &col2)
	colDefs = append(colDefs, &col3)
	return stub.CreateTable("TranStatus", colDefs)
}

func createTableTranIndex(stub shim.ChaincodeStubInterface) error {
		
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "Index", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "EpayId", Type: shim.ColumnDefinition_STRING, Key: false}
	colDefs = append(colDefs, &col1)
	colDefs = append(colDefs, &col2)
	return stub.CreateTable("TranIndex", colDefs)
}

func createTableBatchIndex(stub shim.ChaincodeStubInterface) error {
		
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "Index", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "BatchId", Type: shim.ColumnDefinition_STRING, Key: false}
	colDefs = append(colDefs, &col1)
	colDefs = append(colDefs, &col2)
	return stub.CreateTable("BatchIndex", colDefs)
}

func createTableIndexes(stub shim.ChaincodeStubInterface) error {
		
	var colDefs []*shim.ColumnDefinition
	col1 := shim.ColumnDefinition{Name: "Type", Type: shim.ColumnDefinition_STRING, Key: true}
	col2 := shim.ColumnDefinition{Name: "MaxIndex", Type: shim.ColumnDefinition_STRING, Key: false}
	colDefs = append(colDefs, &col1)
	colDefs = append(colDefs, &col2)
	return stub.CreateTable("Indexes", colDefs)
}

func insertRowTableStatusCounts(stub shim.ChaincodeStubInterface) error {
		
	Id := "0"
	Total := "0"
	Initiated := "0"
	Recieved := "0"
	Authorized := "0"
	AuthRecieved := "0"
	Reconciled := "0"
	BatchInitiated := "0"
	SettlementInitiated :="0"
	Settled := "0"
	Rejected := "0"
	
	var cols []*shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: Id}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: Total}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: Initiated}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: Recieved}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: Authorized}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: AuthRecieved}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: Reconciled}}
	col8 := shim.Column{Value: &shim.Column_String_{String_: BatchInitiated}}
	col9 := shim.Column{Value: &shim.Column_String_{String_: SettlementInitiated}}
	col10 := shim.Column{Value: &shim.Column_String_{String_: Settled}}
	col11 := shim.Column{Value: &shim.Column_String_{String_: Rejected}}
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
	row := shim.Row{Columns: cols}
	
	ok, err := stub.InsertRow("StatusCounts", row)
	myLogger.Debug("insert", ok)
	if err != nil {
		return fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	
	return nil
	
}

func insertRowTableCompanyCounts(stub shim.ChaincodeStubInterface) error {
		
	Id := "0"
	RTA := "0"
	Dewa := "0"
	DU := "0"
	Etisalat := "0"
	DubaiCustoms := "0"
	Others := "0"
	
	var cols []*shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: Id}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: RTA}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: Dewa}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: DU}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: Etisalat}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: DubaiCustoms}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: Others}}
	cols = append(cols, &col1)
	cols = append(cols, &col2)
	cols = append(cols, &col3)
	cols = append(cols, &col4)
	cols = append(cols, &col5)
	cols = append(cols, &col6)
	cols = append(cols, &col7)
	row := shim.Row{Columns: cols}
	
	ok, err := stub.InsertRow("CompanyCounts", row)
	myLogger.Debug("insert", ok)
	if err != nil {
		return fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	
	return nil
}

func insertRowTableTranAmounts(stub shim.ChaincodeStubInterface) error {
		
	Id := "0"
	Total := "0"
	Initiated := "0"
	Recieved := "0"
	Authorized := "0"
	AuthRecieved := "0"
	Reconciled := "0"
	BatchInitiated := "0"
	SettlementInitiated :="0"
	Settled := "0"
	Rejected := "0"
	
	var cols []*shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: Id}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: Total}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: Initiated}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: Recieved}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: Authorized}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: AuthRecieved}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: Reconciled}}
	col8 := shim.Column{Value: &shim.Column_String_{String_: BatchInitiated}}
	col9 := shim.Column{Value: &shim.Column_String_{String_: SettlementInitiated}}
	col10 := shim.Column{Value: &shim.Column_String_{String_: Settled}}
	col11 := shim.Column{Value: &shim.Column_String_{String_: Rejected}}
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
	row := shim.Row{Columns: cols}
	
	ok, err := stub.InsertRow("TranAmounts", row)
	myLogger.Debug("insert", ok)
	if err != nil {
		return fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	
	return nil
	
}

func insertRowTableIndexes(stub shim.ChaincodeStubInterface) error {
		
	var cols []*shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: "Tran"}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: "0"}}
	cols = append(cols, &col1)
	cols = append(cols, &col2)
	row := shim.Row{Columns: cols}
	
	ok, err := stub.InsertRow("Indexes", row)
	myLogger.Debug("insert", ok)
	if err != nil {
		return fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	
	var cols1 []*shim.Column
	col11 := shim.Column{Value: &shim.Column_String_{String_: "Batch"}}
	col21 := shim.Column{Value: &shim.Column_String_{String_: "0"}}
	cols1 = append(cols1, &col11)
	cols1 = append(cols1, &col21)
	row1 := shim.Row{Columns: cols1}
	
	ok1, err1 := stub.InsertRow("Indexes", row1)
	myLogger.Debug("insert", ok1)
	if err1 != nil {
		return fmt.Errorf("insertTableOne operation failed. %s", err1)
	}
	if !ok1 {
		return errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	return nil
	
}


//////////////// INIT ////////////////


func (t *ReconChaincode) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error)  {
	myLogger.Debug("Init Chaincode...")
	
	
	err := createTableTranIndex(stub)
	if err != nil {
		return nil, fmt.Errorf("Error creating table Recon during init. %s", err)
	}
	err = createTableBatchIndex(stub)
	if err != nil {
		return nil, fmt.Errorf("Error creating table Recon during init. %s", err)
	}
	err = createTableIndexes(stub)
	if err != nil {
		return nil, fmt.Errorf("Error creating table Recon during init. %s", err)
	}
	err = insertRowTableIndexes(stub)
	if err != nil {
		return nil, fmt.Errorf("Error inserting row into table CompanyCounts during init. %s", err)
	}
	
	
	
	
	err = createTableRecon(stub)
	if err != nil {
		return nil, fmt.Errorf("Error creating table Recon during init. %s", err)
	}
	
	err = createTableReconTemp(stub)
	if err != nil {
		return nil, fmt.Errorf("Error creating table Recon during init. %s", err)
	}
	
	err = createTableBatch(stub)
	if err != nil {
		return nil, fmt.Errorf("Error creating table Batch during init. %s", err)
	}
	
	err = createTableStatusCounts(stub)
	if err != nil {
		return nil, fmt.Errorf("Error creating table StatusCounts during init. %s", err)
	}
	
	err = insertRowTableStatusCounts(stub)
		if err != nil {
		return nil, fmt.Errorf("Error inserting row into table StatusCounts during init. %s", err)
	}

	err = createTableCompanyCounts(stub)
	if err != nil {
		return nil, fmt.Errorf("Error creating table StatusCounts during init. %s", err)
	}
	
	err = insertRowTableCompanyCounts(stub)
		if err != nil {
		return nil, fmt.Errorf("Error inserting row into table CompanyCounts during init. %s", err)
	}

	err = createTableTranStatus(stub)
	if err != nil {
		return nil, fmt.Errorf("Error creating table StatusCounts during init. %s", err)
	}
	
		
	err = createTableTranAmounts(stub)
	if err != nil {
		return nil, fmt.Errorf("Error creating table StatusCounts during init. %s", err)
	}
	
	err = insertRowTableTranAmounts(stub)
		if err != nil {
		return nil, fmt.Errorf("Error inserting row into table StatusCounts during init. %s", err)
	}

	myLogger.Debug("Init Chaincode...done")

	return nil, nil
}


//////////////// INVOKE ////////////////


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
	}	else if function == "CreateBatch" {
		return t.CreateBatch(stub, args)
	}	else if function == "UpdateInitiatedBatch" {
		return t.UpdateInitiatedBatch(stub, args)
	}	else if function == "SettleBatch" {
		return t.SettleBatch(stub, args)
	}	else if function == "RejectTran" {
		return t.RejectTran(stub, args)
	}
	
	return nil, errors.New("Received unknown function invocation")
}

func (t *ReconChaincode) initiateTran_Original(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
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
	// flag1 := "Yes"
	// flag2 := "No"
	// flag3 := "No"
	// flag4 := "No"
	// flag5 := "No"	
	
	
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
	// col12 := shim.Column{Value: &shim.Column_String_{String_: flag1}}	
	// col13 := shim.Column{Value: &shim.Column_String_{String_: flag2}}
	// col14 := shim.Column{Value: &shim.Column_String_{String_: flag3}}
	// col15 := shim.Column{Value: &shim.Column_String_{String_: flag4}}
	// col16 := shim.Column{Value: &shim.Column_String_{String_: flag5}}
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
	// columns = append(columns, &col12)
	// columns = append(columns, &col13)
	// columns = append(columns, &col14)
	// columns = append(columns, &col15)
	// columns = append(columns, &col16)
	row := shim.Row{Columns: columns}
	ok, err := stub.InsertRow("Reconciliation", row)
	if len(row.Columns) < 1 {
	 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	if err != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
		
	myLogger.Debug("Transaction Initiated on Ledger with entity reference number: .", entityRefNum)
		
	t.UpdateStatusCount(stub, "New", "Initiated")
	t.UpdateCompanyCount(stub, billingCompany)
	t.UpdateTranAmount(stub, "New", "Initiated", amount)
	t.SetTranStatus(stub, args[0], "InitiateTran")
	t.SaveIndexes(stub, "Tran", epayRefNum)
	return nil, nil
	// myLogger.Debug("EpayId: ", epayRefNum)
	// myLogger.Debug("Status: ", status)
	// myLogger.Debug("BatchId: ", "")
		
	// returnVal := InvokeReturnValues{
		// EpayId: epayRefNum,
		// Status: status,
		// BatchId: "",
	// }	
	// returnBytes, err := json.Marshal(returnVal)
	// myLogger.Debug("returnVal: ", returnVal)
	// myLogger.Debug("returnBytes: ", returnBytes)
	// if err != nil {
		// myLogger.Errorf("Data marshaling error %v", err)
	// }
	//return returnBytes, nil
}

func (t *ReconChaincode) gatewayTranLeg1_Original(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
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
	// flag1 := ""
	// flag2 := "Yes"
	// flag3 := ""
	// flag4 := ""
	// flag5 := ""
	
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
	if len(row.Columns) < 1 {
	 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}

	if (entityRefNum != row.Columns[2].GetString_()) || (billNumber != row.Columns[4].GetString_()) ||  (amount != row.Columns[7].GetString_()) || (strings.TrimSpace(strings.ToUpper(billingCompany)) != strings.TrimSpace(strings.ToUpper(row.Columns[5].GetString_()))) {
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
	
	t.UpdateStatusCount(stub, "Initiated", "Recieved")
	t.UpdateTranAmount(stub, "Initiated", "Recieved", amount)
	t.SetTranStatus(stub, args[0], "GatewayTranLeg1")


	return nil, nil
	// returnVal := InvokeReturnValues{
		// EpayId: epayRefNum,
		// Status: status,
		// BatchId: "",
	// }	
	// returnBytes, err := json.Marshal(returnVal)
	// if err != nil {
		// myLogger.Errorf("Data marshaling error %v", err)
	// }
	// return returnBytes, nil
}

func (t *ReconChaincode) initiateTran(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	myLogger.Debug("initiateTran started")

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
	ok, err := stub.InsertRow("ReconTemp", row)
	
	if err != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
		
	myLogger.Debug("Transaction Initiated on Ledger with entity reference number: .", entityRefNum)
		
	t.UpdateStatusCount(stub, "New", "Initiated")
	t.UpdateCompanyCount(stub, billingCompany)
	t.UpdateTranAmount(stub, "New", "Initiated", amount)
	t.SetTranStatus(stub, args[0], "InitiateTran")
	
	myLogger.Debug("initiateTran ended successfully")
	return nil, nil
}

func (t *ReconChaincode) gatewayTranLeg1(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	myLogger.Debug("gatewayTranLeg1 started")
	// concatenated biller reference number to be sent as 6th argument.
	
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
	col1Val := args[5]
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	columns = append(columns, col1)	
	row, err := stub.GetRow("ReconTemp", columns)
	myLogger.Debug("row", row)
	myLogger.Debug("len(row.Columns)", len(row.Columns))
	if len(row.Columns) < 1 {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	
	if (entityRefNum != row.Columns[2].GetString_()) || (billNumber != row.Columns[4].GetString_()) ||  (amount != row.Columns[7].GetString_()) || (billingCompany != row.Columns[5].GetString_()) {
		return nil, errors.New("Unable to reconcile record.")
	}
	
	datetime = row.Columns[9].GetString_() + ", " + timestamp.String()
	details = row.Columns[10].GetString_() + ", " + "Transaction recieved on gateway"
	
	myLogger.Debug("before delete")
	err = stub.DeleteRow("ReconTemp", columns)
	if err != nil {
		return nil, fmt.Errorf("Recon operation failed. %s", err)
	}
	
	myLogger.Debug("after delete")
	
	var cols []*shim.Column
	col1 = shim.Column{Value: &shim.Column_String_{String_: status}}
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
	
	t.UpdateStatusCount(stub, "Initiated", "Recieved")
	t.UpdateTranAmount(stub, "Initiated", "Recieved", amount)
	t.SetTranStatus(stub, args[0], "GatewayTranLeg1")

	
	t.SaveIndexes(stub, "Tran", epayRefNum)
	myLogger.Debug("gatewayTranLeg1 ended successfully")
	return nil, nil
}

func (t *ReconChaincode) networkTran(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	myLogger.Debug("networkTran started")
	
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
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: col2Val}}
	columns = append(columns, col1)	
	columns = append(columns, col2)	
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
		
	if (amount != row.Columns[7].GetString_()) || (strings.TrimSpace(strings.ToUpper(billingCompany))) != (strings.TrimSpace(strings.ToUpper(row.Columns[5].GetString_()))) {
		return nil, errors.New("Unable to reconcile record.")
	}
	
	
	billNumber = row.Columns[4].GetString_()
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
	
	t.UpdateStatusCount(stub, "Recieved", "Authorized")
	t.UpdateTranAmount(stub, "Recieved", "Authorized", amount)
	t.SetTranStatus(stub, args[0], "NetworkTran")

	myLogger.Debug("networkTran ended successfully")
	return nil, nil
}

func (t *ReconChaincode) gatewayTranLeg2(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	myLogger.Debug("gatewayTranLeg2 started")
	
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
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: col2Val}}
	columns = append(columns, col1)	
	columns = append(columns, col2)	
	row, err := stub.GetRow("Reconciliation", columns)
	myLogger.Debug("row", row)
	myLogger.Debug("len(row.Columns)", len(row.Columns))
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	if len(row.Columns) < 1 {
	 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	
	if (billNumber != row.Columns[4].GetString_()) ||  (amount != row.Columns[7].GetString_()) || (strings.TrimSpace(strings.ToUpper(billingCompany)) != strings.TrimSpace(strings.ToUpper(row.Columns[5].GetString_()))) {
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
	
	t.UpdateStatusCount(stub, "Authorized", "AuthRecieved")
	t.UpdateTranAmount(stub, "Authorized", "AuthRecieved", amount)
	t.SetTranStatus(stub, args[0], "GatewayTranLeg2")
	
	myLogger.Debug("gatewayTranLeg2 ended successfully")
	return nil, nil
}

func (t *ReconChaincode) reconcileTran(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	myLogger.Debug("reconcileTran started")

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
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: col2Val}}
	columns = append(columns, col1)	
	columns = append(columns, col2)		
	row, err := stub.GetRow("Reconciliation", columns)
	myLogger.Debug("row", row)
	myLogger.Debug("len(row.Columns)", len(row.Columns))
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	if len(row.Columns) < 1 {
	 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}

	if (billNumber != row.Columns[4].GetString_()) ||  (amount != row.Columns[7].GetString_()) || (strings.TrimSpace(strings.ToUpper(billingCompany)) != strings.TrimSpace(strings.ToUpper(row.Columns[5].GetString_()))) {
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
	
	t.UpdateStatusCount(stub, "AuthRecieved", "Reconciled")
	t.UpdateTranAmount(stub, "AuthRecieved", "Reconciled", amount)
	t.SetTranStatus(stub, args[0], "ReconcileTran")
	
	myLogger.Debug("reconcileTran ended successfully")
	return nil, nil
}

func (t *ReconChaincode) CreateBatch(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	currentIndex := t.GetCurrentIndex(stub, "Batch")
	
	myLogger.Debug("CreateBatch started")
	
	var rows []shim.Row
	var totalAmount float64
	var issuerBank string
	var billCompany string
	var flag bool
	flag = false
	batchID := "batch_" + strconv.Itoa(currentIndex + 1) // + strconv.Itoa(rand.Intn(100000))
	myLogger.Debug("batchID: ",batchID)
	timestamp := time.Now()
	
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: "Reconciled"}}
	columns = append(columns, col1)
	
	rowsToUpdate, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowsToUpdate)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}
	
	for {
		select {
		case row, ok := <-rowsToUpdate:
			if !ok {
				myLogger.Debug("nil: ",nil)
				rowsToUpdate = nil
			} else {
				myLogger.Debug("row: ",row)
				myLogger.Debug("row.Columns[5].GetString_(): ",row.Columns[5].GetString_())
				myLogger.Debug("before if")
				if row.Columns[8].GetString_() == "" {
					myLogger.Debug("After IF")
					rows = append(rows, row)
					tempAmount, err1 := strconv.ParseFloat(row.Columns[7].GetString_(), 64)
					
					myLogger.Debug("tempAmount", tempAmount)
					if err1 != nil{ 
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
					myLogger.Debug("yahan")
					var delCols []shim.Column
					delCol1 := shim.Column{Value: &shim.Column_String_{String_: "Reconciled"}}
					delCols = append(delCols, delCol1)
					delCol2 := shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
					delCols = append(delCols, delCol2)
					
					myLogger.Debug("before transaction delete")
					err2 := stub.DeleteRow("Reconciliation", delCols)
					if err2 != nil {
						return nil, fmt.Errorf("Recon operation failed. %s", err2)
					}
					
					myLogger.Debug("after transaction delete")
					
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
					ok, err3 := stub.InsertRow("Reconciliation", row)
					myLogger.Debug("Tran Re-insert: ", ok)
					if err3 != nil {
						return nil, fmt.Errorf("insertTableOne operation failed. %s", err3)
					}
					if !ok {
						return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
					}
					myLogger.Debug("Insert: ", ok)	
					t.UpdateStatusCount(stub, "", "BatchInitiated")
					t.UpdateTranAmount(stub, "", "BatchInitiated", amount)
					flag = true					
				}
			}
		}
		if rowsToUpdate == nil {
			break
		}
	}
	myLogger.Debug("flag", flag)			
	if flag == true {
		// start - batch creation
		currentTime := timestamp.String()
		myLogger.Debug("strconv.FormatFloat(totalAmount, 'f', 6, 64): ", strconv.FormatFloat(totalAmount, 'f', 6, 64))		
		myLogger.Debug("batchID", batchID)			
		myLogger.Debug("billCompany", billCompany)			
		myLogger.Debug("issuerBank", issuerBank)			
		myLogger.Debug("currentTime", currentTime)	

		var columns1 []*shim.Column
		colm1 := shim.Column{Value: &shim.Column_String_{String_: batchID}}
		colm2 := shim.Column{Value: &shim.Column_String_{String_: billCompany}}
		colm3 := shim.Column{Value: &shim.Column_String_{String_: issuerBank}}
		//colm4 := shim.Column{Value: &shim.Column_String_{String_: strconv.Itoa(totalAmount)}}
		colm4 := shim.Column{Value: &shim.Column_String_{String_: strconv.FormatFloat(totalAmount, 'f', 6, 64)}}
		colm5 := shim.Column{Value: &shim.Column_String_{String_: "BatchInitiated"}}
		colm6 := shim.Column{Value: &shim.Column_String_{String_: currentTime}}
		colm7 := shim.Column{Value: &shim.Column_String_{String_: "Setllement Batch Initiated"}}	
		columns1 = append(columns1, &colm1)
		columns1 = append(columns1, &colm2)
		columns1 = append(columns1, &colm3)
		columns1 = append(columns1, &colm4)
		columns1 = append(columns1, &colm5)
		columns1 = append(columns1, &colm6)
		columns1 = append(columns1, &colm7)
		batchrow := shim.Row{Columns: columns1}
		myLogger.Debug("before insert")
		ok1, err4 := stub.InsertRow("Batch", batchrow)
		myLogger.Debug("Batch Insert: ", ok1)
		if err4 != nil {
			return nil, fmt.Errorf("insertTableOne operation failed. %s", err4)
		}
		if !ok1 {
			return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
		}

		myLogger.Debug("Setllement Batch initiated on Ledger with Batch ID: .", batchID)
			
		// end - batch creation
	}
	
	t.SetTranStatus(stub, batchID, "CreateBatch")
	t.SaveIndexes(stub, "Batch", batchID)
	myLogger.Debug("CreateBatch ended successfully")
	return nil, nil
}

func (t *ReconChaincode) CreateBatch_Old(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	billCompanies := t.GetUniqueBillCompany(stub)

	for index, val := range billCompanies {
		myLogger.Debug("index: ",index)
		myLogger.Debug("val: ",val)
		
		var rows []shim.Row
		var totalAmount float64
		var issuerBank string
		var billCompany string
		
		batchID := val + "_" + strconv.Itoa(rand.Intn(100000))
		myLogger.Debug("batchID: ",batchID)
		timestamp := time.Now()
		//myLogger.Debug("Rows: ",rowsToUpdate)
		
		var columns []shim.Column
		col1 := shim.Column{Value: &shim.Column_String_{String_: "Reconciled"}}
		columns = append(columns, col1)
		
		rowsToUpdate, err := stub.GetRows("Reconciliation", columns)
		myLogger.Debug("Rows: ",rowsToUpdate)
		if err != nil {
			return nil, fmt.Errorf("operation failed. %s", err)
		}
		
		for {
			select {
			case row, ok := <-rowsToUpdate:
				if !ok {
					myLogger.Debug("nil: ",nil)
					rowsToUpdate = nil
					//break
				} else {
					myLogger.Debug("row: ",row)
					myLogger.Debug("row.Columns[5].GetString_(): ",row.Columns[5].GetString_())
					myLogger.Debug("val: ",val)
					myLogger.Debug("before if")
					if (row.Columns[8].GetString_() == "") && (row.Columns[5].GetString_() == val) {
						myLogger.Debug("After IF")
						rows = append(rows, row)
						tempAmount, err1 := strconv.ParseFloat(row.Columns[7].GetString_(), 64)
						
						myLogger.Debug("tempAmount", tempAmount)
						if err1 != nil{ 
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
						myLogger.Debug("yahan")
						var delCols []shim.Column
						delCol1 := shim.Column{Value: &shim.Column_String_{String_: "Reconciled"}}
						delCols = append(delCols, delCol1)
						delCol2 := shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
						delCols = append(delCols, delCol2)
						
						myLogger.Debug("before transaction delete")
						err2 := stub.DeleteRow("Reconciliation", delCols)
						if err2 != nil {
							return nil, fmt.Errorf("Recon operation failed. %s", err2)
						}
						
						myLogger.Debug("after transaction delete")
						
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
						ok, err3 := stub.InsertRow("Reconciliation", row)
						myLogger.Debug("Tran Re-insert: ", ok)
						if err3 != nil {
							return nil, fmt.Errorf("insertTableOne operation failed. %s", err3)
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
		currentTime := timestamp.String()
		myLogger.Debug("strconv.FormatFloat(totalAmount, 'f', 6, 64): ", strconv.FormatFloat(totalAmount, 'f', 6, 64))		
		myLogger.Debug("batchID", batchID)			
		myLogger.Debug("billCompany", billCompany)			
		myLogger.Debug("issuerBank", issuerBank)			
		myLogger.Debug("currentTime", currentTime)	

		var columns1 []*shim.Column
		colm1 := shim.Column{Value: &shim.Column_String_{String_: batchID}}
		colm2 := shim.Column{Value: &shim.Column_String_{String_: billCompany}}
		colm3 := shim.Column{Value: &shim.Column_String_{String_: issuerBank}}
		//colm4 := shim.Column{Value: &shim.Column_String_{String_: strconv.Itoa(totalAmount)}}
		colm4 := shim.Column{Value: &shim.Column_String_{String_: strconv.FormatFloat(totalAmount, 'f', 6, 64)}}
		colm5 := shim.Column{Value: &shim.Column_String_{String_: "BatchInitiated"}}
		colm6 := shim.Column{Value: &shim.Column_String_{String_: currentTime}}
		colm7 := shim.Column{Value: &shim.Column_String_{String_: "Setllement Batch Initiated"}}	
		columns1 = append(columns1, &colm1)
		columns1 = append(columns1, &colm2)
		columns1 = append(columns1, &colm3)
		columns1 = append(columns1, &colm4)
		columns1 = append(columns1, &colm5)
		columns1 = append(columns1, &colm6)
		columns1 = append(columns1, &colm7)
		batchrow := shim.Row{Columns: columns1}
		myLogger.Debug("before insert")
		ok1, err4 := stub.InsertRow("Batch", batchrow)
		myLogger.Debug("Batch Insert: ", ok1)
		if err4 != nil {
			return nil, fmt.Errorf("insertTableOne operation failed. %s", err4)
		}
		if !ok1 {
			return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
		}
	
		myLogger.Debug("Setllement Batch initiated on Ledger with Batch ID: .", batchID)
	}
		
	// end - batch creation
	
	t.UpdateStatusCount(stub, "", "BatchInitiated")
	return nil, nil
}

func (t *ReconChaincode) UpdateInitiatedBatch(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	myLogger.Debug("UpdateInitiatedBatch started")
	
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: "BatchInitiated"}}
	columns = append(columns, col1)
	
	rowsToUpdate, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowsToUpdate)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}

	var rows []shim.Row
	timestamp := time.Now()
		
	for {
		select {
		case row, ok := <-rowsToUpdate:
			if !ok {
				rowsToUpdate = nil
			} else {
				myLogger.Debug("row.Columns[8].GetString_(): ",row.Columns[8].GetString_())
				myLogger.Debug("args[0]: ",args[0])
				if strings.TrimSpace(strings.ToUpper(row.Columns[8].GetString_())) == strings.TrimSpace(strings.ToUpper(string(args[0]))) {
					rows = append(rows, row)
					myLogger.Debug("inside. ")
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
					
					
					var delCols []shim.Column
					delCol1 := shim.Column{Value: &shim.Column_String_{String_: "BatchInitiated"}}
					delCols = append(delCols, delCol1)
					delCol2 := shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
					delCols = append(delCols, delCol2)
					
					myLogger.Debug("before delete")
					err1 := stub.DeleteRow("Reconciliation", delCols)
					if err1 != nil {
						return nil, fmt.Errorf("Recon operation failed. %s", err1)
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
					
					reconRow := shim.Row{Columns: cols}
					ok, err2 := stub.InsertRow("Reconciliation", reconRow)

					if err2 != nil {
						return nil, fmt.Errorf("insertTableOne operation failed. %s", err2)
					}
					if !ok {
						return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
					}
					myLogger.Debug("Insert: ", ok)		
					t.UpdateStatusCount(stub, "BatchInitiated", "SettlementInitiated")
					t.UpdateTranAmount(stub, "BatchInitiated", "SettlementInitiated", amount)
				}
			}
		}
		if rowsToUpdate == nil {
			break
		}
	}		
		
	// start - batch updation

	var cols1 []shim.Column
	col1Value := args[0]
	myLogger.Debug("col1Value: ", col1Value)
	column1 := shim.Column{Value: &shim.Column_String_{String_: col1Value}}
	cols1 = append(cols1, column1)	
	batchRow, err3 := stub.GetRow("Batch", cols1)
	myLogger.Debug("batchRow", batchRow)
	myLogger.Debug("len(batchRow.Columns)", len(batchRow.Columns))
	if err3 != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err3)
	}
	if (len(batchRow.Columns) < 1){
		return nil, nil
	}
	tempBatchID := batchRow.Columns[0].GetString_()
	tempBillCompany := batchRow.Columns[1].GetString_()
	tempIssuerBank := batchRow.Columns[2].GetString_()
	tempAmount := batchRow.Columns[3].GetString_()
	tempStatus := "SettlementInitiated"
	tempDateTime := batchRow.Columns[5].GetString_() + ", " + timestamp.String()
	tempDetails := batchRow.Columns[6].GetString_() + ", " + "Settlement Initiated"
		
	myLogger.Debug("before delete")
	err4 := stub.DeleteRow("Batch", cols1)
	if err != nil {
		return nil, fmt.Errorf("Recon operation failed. %s", err4)
	}	
	myLogger.Debug("after delete")

	var batchColumns []*shim.Column
	bc1 := shim.Column{Value: &shim.Column_String_{String_: tempBatchID}}
	bc2 := shim.Column{Value: &shim.Column_String_{String_: tempBillCompany}}
	bc3 := shim.Column{Value: &shim.Column_String_{String_: tempIssuerBank}}
	bc4 := shim.Column{Value: &shim.Column_String_{String_: tempAmount}}
	bc5 := shim.Column{Value: &shim.Column_String_{String_: tempStatus}}
	bc6 := shim.Column{Value: &shim.Column_String_{String_: tempDateTime}}
	bc7 := shim.Column{Value: &shim.Column_String_{String_: tempDetails}}	
	batchColumns = append(batchColumns, &bc1)
	batchColumns = append(batchColumns, &bc2)
	batchColumns = append(batchColumns, &bc3)
	batchColumns = append(batchColumns, &bc4)
	batchColumns = append(batchColumns, &bc5)
	batchColumns = append(batchColumns, &bc6)
	batchColumns = append(batchColumns, &bc7)
	batchRow1 := shim.Row{Columns: batchColumns}
	ok, err := stub.InsertRow("Batch", batchRow1)

	if err != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
		
	myLogger.Debug("Setllement initiated on Ledger for Batch ID: .", tempBatchID)
		
	// end - batch updation
	
	t.SetTranStatus(stub, tempBatchID, "UpdateInitiatedBatch")
	
	myLogger.Debug("UpdateInitiatedBatch ended successfully")
	return nil, nil
}

func (t *ReconChaincode) SettleBatch(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	myLogger.Debug("SettleBatch started")
	var columns []shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: "SettlementInitiated"}}
	columns = append(columns, col1)
	
	rowsToUpdate, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowsToUpdate)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}

	var rows []shim.Row
	timestamp := time.Now()
	
	for {
		select {
		case row, ok := <-rowsToUpdate:
			if !ok {
				rowsToUpdate = nil
			} else {
				if strings.TrimSpace(strings.ToUpper(row.Columns[8].GetString_())) == strings.TrimSpace(strings.ToUpper(args[0])) {
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
					
					var delCols []shim.Column
					delCol1 := shim.Column{Value: &shim.Column_String_{String_: "SettlementInitiated"}}
					delCols = append(delCols, delCol1)
					delCol2 := shim.Column{Value: &shim.Column_String_{String_: epayRefNum}}
					delCols = append(delCols, delCol2)
					
					myLogger.Debug("before delete")
					err1 := stub.DeleteRow("Reconciliation", delCols)
					if err1 != nil {
						return nil, fmt.Errorf("Recon operation failed. %s", err1)
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
					
					reconRow := shim.Row{Columns: cols}
					ok, err2 := stub.InsertRow("Reconciliation", reconRow)

					if err2 != nil {
						return nil, fmt.Errorf("insertTableOne operation failed. %s", err2)
					}
					if !ok {
						return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
					}
					myLogger.Debug("Insert: ", ok)	
					
					t.UpdateStatusCount(stub, "Reconciled", "")
					t.UpdateStatusCount(stub, "SettlementInitiated", "Settled")		
					t.UpdateTranAmount(stub, "Reconciled", "", amount)
					t.UpdateTranAmount(stub, "SettlementInitiated", "Settled", amount)
				}
			}
		}
		if rowsToUpdate == nil {
			break
		}
	}			
	
	// start - batch updation
	
	var cols1 []shim.Column
	col1Value := args[0]
	myLogger.Debug("col1Value: ", col1Value)
	column1 := shim.Column{Value: &shim.Column_String_{String_: col1Value}}
	cols1 = append(cols1, column1)	
	batchRow, err3 := stub.GetRow("Batch", cols1)
	myLogger.Debug("batchRow", batchRow)
	myLogger.Debug("len(batchRow.Columns)", len(batchRow.Columns))
	if err3 != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err3)
	}
	if (len(batchRow.Columns) < 1){
		return nil, nil
	}
	
	tempBatchID := batchRow.Columns[0].GetString_()
	tempBillCompany := batchRow.Columns[1].GetString_()
	tempIssuerBank := batchRow.Columns[2].GetString_()
	tempAmount := batchRow.Columns[3].GetString_()
	tempStatus := "Settled"
	tempDateTime := batchRow.Columns[5].GetString_() + ", " + timestamp.String()
	tempDetails := batchRow.Columns[6].GetString_() + ", " + "Batch Settled"
		
	myLogger.Debug("before delete")
	err4 := stub.DeleteRow("Batch", cols1)
	if err4 != nil {
		return nil, fmt.Errorf("Recon operation failed. %s", err4)
	}	
	myLogger.Debug("after delete")

	var batchColumns []*shim.Column
	bc1 := shim.Column{Value: &shim.Column_String_{String_: tempBatchID}}
	bc2 := shim.Column{Value: &shim.Column_String_{String_: tempBillCompany}}
	bc3 := shim.Column{Value: &shim.Column_String_{String_: tempIssuerBank}}
	bc4 := shim.Column{Value: &shim.Column_String_{String_: tempAmount}}
	bc5 := shim.Column{Value: &shim.Column_String_{String_: tempStatus}}
	bc6 := shim.Column{Value: &shim.Column_String_{String_: tempDateTime}}
	bc7 := shim.Column{Value: &shim.Column_String_{String_: tempDetails}}	
	batchColumns = append(batchColumns, &bc1)
	batchColumns = append(batchColumns, &bc2)
	batchColumns = append(batchColumns, &bc3)
	batchColumns = append(batchColumns, &bc4)
	batchColumns = append(batchColumns, &bc5)
	batchColumns = append(batchColumns, &bc6)
	batchColumns = append(batchColumns, &bc7)
	batchRow1 := shim.Row{Columns: batchColumns}
	ok, err5 := stub.InsertRow("Batch", batchRow1)

	if err5 != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err5)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
		
	myLogger.Debug("Setllement done on Ledger for Batch ID: .", tempBatchID)
		
	// end - batch updation

	t.SetTranStatus(stub, tempBatchID, "SettleBatch")
	
	myLogger.Debug("SettleBatch ended successfully")
	return nil, nil
}

func (t *ReconChaincode) RejectTran(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	myLogger.Debug("RejectTran started")

	timestamp := time.Now()
	
	status := "Rejected"	
	epayRefNum := args[1]
	entityRefNum := ""
	issuerRefNum := ""
	billNumber := ""
	billingCompany := ""
	issuer := ""
	amount := ""
	batchId := ""
	datetime := ""
	details := ""
	
	
	var columns []shim.Column
	col1Val := args[0]
	col2Val := args[1]
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: col2Val}}
	columns = append(columns, col1)	
	columns = append(columns, col2)		
	row, err := stub.GetRow("Reconciliation", columns)
	myLogger.Debug("row", row)
	myLogger.Debug("len(row.Columns)", len(row.Columns))
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	if len(row.Columns) < 1 {
	 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	// For count purpose
	statusTemp := row.Columns[0].GetString_()
	//
	
	entityRefNum = row.Columns[2].GetString_()
	issuerRefNum = row.Columns[3].GetString_()
	billNumber = row.Columns[4].GetString_()
	billingCompany = row.Columns[5].GetString_()
	issuer = row.Columns[6].GetString_()
	amount = row.Columns[7].GetString_()
	batchId = row.Columns[8].GetString_()
	datetime = row.Columns[9].GetString_() + ", " + timestamp.String()
	details = row.Columns[10].GetString_() + ", " + "Transaction Rejected"
	
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
	
	t.UpdateStatusCount(stub, statusTemp, "Rejected")
	t.UpdateTranAmount(stub, statusTemp, "Rejected", amount)
	t.SetTranStatus(stub, args[1], "RejectTran")
	
	myLogger.Debug("RejectTran ended successfully")
	return nil, nil
}


//////////////// INTERNAL INVOKE ////////////////


func (t *ReconChaincode) UpdateStatusCount(stub shim.ChaincodeStubInterface, oldStatus string, newStatus string) ([]byte, error){
	
	Id := "0"
	Total := ""
	Initiated := ""
	Recieved := ""
	Authorized := ""
	AuthRecieved := ""
	Reconciled := ""
	BatchInitiated := ""
	SettlementInitiated := ""
	Settled := ""
	Rejected := ""
	
	var columns []shim.Column
	rows, err := stub.GetRows("StatusCounts", columns)
	// myLogger.Debug("row", rows)
	// myLogger.Debug("len(row.Columns)", len(rows.Columns))
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				rows = nil
			} else {
				Total = row.Columns[1].GetString_()
				Initiated = row.Columns[2].GetString_()
				Recieved = row.Columns[3].GetString_()
				Authorized = row.Columns[4].GetString_()
				AuthRecieved = row.Columns[5].GetString_()
				Reconciled = row.Columns[6].GetString_()
				BatchInitiated = row.Columns[7].GetString_()
				SettlementInitiated = row.Columns[8].GetString_()
				Settled = row.Columns[9].GetString_()
				Rejected = row.Columns[10].GetString_()			
			}
		}
		if rows == nil {
			break
		}
	}
	
	myLogger.Debug("Id", Id)
	myLogger.Debug("Total", Total)
	myLogger.Debug("Initiated", Initiated)
	myLogger.Debug("Recieved", Recieved)
	myLogger.Debug("Authorized", Authorized)
	myLogger.Debug("AuthRecieved", AuthRecieved)
	myLogger.Debug("Reconciled", Reconciled)
	myLogger.Debug("BatchInitiated", BatchInitiated)
	myLogger.Debug("SettlementInitiated", SettlementInitiated)
	myLogger.Debug("Settled", Settled)
	myLogger.Debug("Rejected", Rejected)

	if oldStatus == "New" {
		intVal, err1 := strconv.Atoi(Total)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Total = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in Total")
	}
	if oldStatus == "Initiated" {
		intVal, err1 := strconv.Atoi(Initiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Initiated = strconv.Itoa(intVal - 1)
		myLogger.Debug("decremenet in Initiated")
	}
	if oldStatus == "Recieved" {
		intVal, err1 := strconv.Atoi(Recieved)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Recieved = strconv.Itoa(intVal - 1)
		myLogger.Debug("decremenet in Recieved")
	}
	if oldStatus == "Authorized" {
		intVal, err1 := strconv.Atoi(Authorized)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Authorized = strconv.Itoa(intVal - 1)
		myLogger.Debug("decremenet in Authorized")
	}
	if oldStatus == "AuthRecieved" {
		intVal, err1 := strconv.Atoi(AuthRecieved)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		AuthRecieved = strconv.Itoa(intVal - 1)
		myLogger.Debug("decremenet in AuthRecieved")
	}
	if oldStatus == "Reconciled" {
		intVal, err1 := strconv.Atoi(Reconciled)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Reconciled = strconv.Itoa(intVal - 1)
		myLogger.Debug("decremenet in Reconciled")
	}
	if oldStatus == "BatchInitiated" {
		intVal, err1 := strconv.Atoi(BatchInitiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		BatchInitiated = strconv.Itoa(intVal - 1)
		myLogger.Debug("decremenet in BatchInitiated")
	}
	if oldStatus == "SettlementInitiated" {
		intVal, err1 := strconv.Atoi(SettlementInitiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		SettlementInitiated = strconv.Itoa(intVal - 1)
		myLogger.Debug("decremenet in SettlementInitiated")
	}
	if oldStatus == "Settled" {
		intVal, err1 := strconv.Atoi(Settled)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Settled = strconv.Itoa(intVal - 1)
		myLogger.Debug("decremenet in Settled")
	}
	if oldStatus == "Rejected" {
		intVal, err1 := strconv.Atoi(Rejected)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Rejected = strconv.Itoa(intVal - 1)
		myLogger.Debug("decremenet in Initiated")
	}
	
	
	if newStatus == "Initiated" {
		intVal, err1 := strconv.Atoi(Initiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Initiated = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in Initiated")
	}
	if newStatus == "Recieved" {
		intVal, err1 := strconv.Atoi(Recieved)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Recieved = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in Recieved")
	}
	if newStatus == "Authorized" {
		intVal, err1 := strconv.Atoi(Authorized)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Authorized = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in Authorized")
	}
	if newStatus == "AuthRecieved" {
		intVal, err1 := strconv.Atoi(AuthRecieved)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		AuthRecieved = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in AuthRecieved")
	}
	if newStatus == "Reconciled" {
		intVal, err1 := strconv.Atoi(Reconciled)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Reconciled = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in Reconciled")
	}
	if newStatus == "BatchInitiated" {
		intVal, err1 := strconv.Atoi(BatchInitiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		BatchInitiated = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in BatchInitiated")
	}
	if newStatus == "SettlementInitiated" {
		intVal, err1 := strconv.Atoi(SettlementInitiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		SettlementInitiated = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in SettlementInitiated")
	}
	if newStatus == "Settled" {
		intVal, err1 := strconv.Atoi(Settled)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Settled = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in Settled")
	}
	if newStatus == "Rejected" {
		intVal, err1 := strconv.Atoi(Rejected)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Rejected = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in Rejected")
	}
	
	myLogger.Debug("Id", Id)
	myLogger.Debug("Total", Total)
	myLogger.Debug("Initiated", Initiated)
	myLogger.Debug("Recieved", Recieved)
	myLogger.Debug("Authorized", Authorized)
	myLogger.Debug("AuthRecieved", AuthRecieved)
	myLogger.Debug("Reconciled", Reconciled)
	myLogger.Debug("BatchInitiated", BatchInitiated)
	myLogger.Debug("SettlementInitiated", SettlementInitiated)
	myLogger.Debug("Settled", Settled)
	myLogger.Debug("Rejected", Rejected)

	// err = createTableStatusCounts(stub)
	// if err != nil {
		// return nil, fmt.Errorf("Error creating table StatusCounts during init. %s", err)
	// }
	
	var cols []*shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: Id}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: Total}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: Initiated}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: Recieved}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: Authorized}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: AuthRecieved}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: Reconciled}}
	col8 := shim.Column{Value: &shim.Column_String_{String_: BatchInitiated}}
	col9 := shim.Column{Value: &shim.Column_String_{String_: SettlementInitiated}}
	col10 := shim.Column{Value: &shim.Column_String_{String_: Settled}}
	col11 := shim.Column{Value: &shim.Column_String_{String_: Rejected}}
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
	row1 := shim.Row{Columns: cols}
	
	ok, err := stub.ReplaceRow("StatusCounts", row1)
	myLogger.Debug("Replace: ", ok)
	myLogger.Debug("err: ", err)
	if err != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	
	return nil, nil
}

func (t *ReconChaincode) UpdateCompanyCount(stub shim.ChaincodeStubInterface, company string) ([]byte, error){
	
	Id := "0"
	RTA := "0"
	Dewa := "0"
	DU := "0"
	Etisalat := "0"
	DubaiCustoms := "0"
	Others := "0"
	
	var columns []shim.Column
	rows, err := stub.GetRows("CompanyCounts", columns)
	// myLogger.Debug("row", rows)
	// myLogger.Debug("len(row.Columns)", len(rows.Columns))
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				rows = nil
			} else {
				RTA = row.Columns[1].GetString_()
				Dewa = row.Columns[2].GetString_()
				DU = row.Columns[3].GetString_()
				Etisalat = row.Columns[4].GetString_()
				DubaiCustoms = row.Columns[5].GetString_()
				Others = row.Columns[6].GetString_()	
			}
		}
		if rows == nil {
			break
		}
	}
	
	myLogger.Debug("Id", Id)
	myLogger.Debug("RTA", RTA)
	myLogger.Debug("Dewa", Dewa)
	myLogger.Debug("DU", DU)
	myLogger.Debug("Etisalat", Etisalat)
	myLogger.Debug("DubaiCustoms", DubaiCustoms)
	myLogger.Debug("Others", Others)

	
	if strings.ToLower(company) == "rta"  {
		intVal, err1 := strconv.Atoi(RTA)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		RTA = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in RTA")
	} else if strings.ToLower(company) == "dewa" {
		intVal, err1 := strconv.Atoi(Dewa)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Dewa = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in Dewa")
	} else if strings.ToLower(company) == "du" {
		intVal, err1 := strconv.Atoi(DU)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		DU = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in DU")
	} else if strings.ToLower(company) == "etisalat" {
		intVal, err1 := strconv.Atoi(Etisalat)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Etisalat = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in Etisalat")
	} else if (strings.ToLower(company) == "dubaicustoms") || (strings.ToLower(company) == "customs") {
		intVal, err1 := strconv.Atoi(DubaiCustoms)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		DubaiCustoms = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in DubaiCustoms")
	} else {
		intVal, err1 := strconv.Atoi(Others)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Others = strconv.Itoa(intVal + 1)
		myLogger.Debug("incremenet in Others")
	}
	
	myLogger.Debug("Id", Id)
	myLogger.Debug("RTA", RTA)
	myLogger.Debug("Dewa", Dewa)
	myLogger.Debug("DU", DU)
	myLogger.Debug("Etisalat", Etisalat)
	myLogger.Debug("DubaiCustoms", DubaiCustoms)
	myLogger.Debug("Others", Others)

	
	var cols []*shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: Id}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: RTA}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: Dewa}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: DU}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: Etisalat}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: DubaiCustoms}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: Others}}
	cols = append(cols, &col1)
	cols = append(cols, &col2)
	cols = append(cols, &col3)
	cols = append(cols, &col4)
	cols = append(cols, &col5)
	cols = append(cols, &col6)
	cols = append(cols, &col7)
	row := shim.Row{Columns: cols}
	
	ok, err := stub.ReplaceRow("CompanyCounts", row)
	myLogger.Debug("Replace: ", ok)
	myLogger.Debug("err: ", err)
	if err != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	
	return nil, nil
}

func (t *ReconChaincode) UpdateTranAmount(stub shim.ChaincodeStubInterface, oldStatus string, newStatus string, amount string) ([]byte, error){
	
	Id := "0"
	Total := ""
	Initiated := ""
	Recieved := ""
	Authorized := ""
	AuthRecieved := ""
	Reconciled := ""
	BatchInitiated := ""
	SettlementInitiated := ""
	Settled := ""
	Rejected := ""
	
	tranAmount, error := strconv.Atoi(amount)
	if error != nil{ 
		return nil, fmt.Errorf("Error encountered.")
	}
	
	var columns []shim.Column
	rows, err := stub.GetRows("TranAmounts", columns)
	// myLogger.Debug("row", rows)
	// myLogger.Debug("len(row.Columns)", len(rows.Columns))
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}

	for {
		select {
		case row, ok := <-rows:
			if !ok {
				rows = nil
			} else {
				Total = row.Columns[1].GetString_()
				Initiated = row.Columns[2].GetString_()
				Recieved = row.Columns[3].GetString_()
				Authorized = row.Columns[4].GetString_()
				AuthRecieved = row.Columns[5].GetString_()
				Reconciled = row.Columns[6].GetString_()
				BatchInitiated = row.Columns[7].GetString_()
				SettlementInitiated = row.Columns[8].GetString_()
				Settled = row.Columns[9].GetString_()
				Rejected = row.Columns[10].GetString_()			
			}
		}
		if rows == nil {
			break
		}
	}
	
	myLogger.Debug("Id", Id)
	myLogger.Debug("Total", Total)
	myLogger.Debug("Initiated", Initiated)
	myLogger.Debug("Recieved", Recieved)
	myLogger.Debug("Authorized", Authorized)
	myLogger.Debug("AuthRecieved", AuthRecieved)
	myLogger.Debug("Reconciled", Reconciled)
	myLogger.Debug("BatchInitiated", BatchInitiated)
	myLogger.Debug("SettlementInitiated", SettlementInitiated)
	myLogger.Debug("Settled", Settled)
	myLogger.Debug("Rejected", Rejected)

	if oldStatus == "New" {
		intVal, err1 := strconv.Atoi(Total)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Total = strconv.Itoa(intVal + tranAmount)
		myLogger.Debug("incremenet in Total")
	}
	if oldStatus == "Initiated" {
		intVal, err1 := strconv.Atoi(Initiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Initiated = strconv.Itoa(intVal - tranAmount)
		myLogger.Debug("decremenet in Initiated")
	}
	if oldStatus == "Recieved" {
		intVal, err1 := strconv.Atoi(Recieved)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Recieved = strconv.Itoa(intVal - tranAmount)
		myLogger.Debug("decremenet in Recieved")
	}
	if oldStatus == "Authorized" {
		intVal, err1 := strconv.Atoi(Authorized)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Authorized = strconv.Itoa(intVal - tranAmount)
		myLogger.Debug("decremenet in Authorized")
	}
	if oldStatus == "AuthRecieved" {
		intVal, err1 := strconv.Atoi(AuthRecieved)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		AuthRecieved = strconv.Itoa(intVal - tranAmount)
		myLogger.Debug("decremenet in AuthRecieved")
	}
	if oldStatus == "Reconciled" {
		intVal, err1 := strconv.Atoi(Reconciled)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Reconciled = strconv.Itoa(intVal - tranAmount)
		myLogger.Debug("decremenet in Reconciled")
	}
	if oldStatus == "BatchInitiated" {
		intVal, err1 := strconv.Atoi(BatchInitiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		BatchInitiated = strconv.Itoa(intVal - tranAmount)
		myLogger.Debug("decremenet in BatchInitiated")
	}
	if oldStatus == "SettlementInitiated" {
		intVal, err1 := strconv.Atoi(SettlementInitiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		SettlementInitiated = strconv.Itoa(intVal - tranAmount)
		myLogger.Debug("decremenet in SettlementInitiated")
	}
	if oldStatus == "Settled" {
		intVal, err1 := strconv.Atoi(Settled)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Settled = strconv.Itoa(intVal - tranAmount)
		myLogger.Debug("decremenet in Settled")
	}
	if oldStatus == "Rejected" {
		intVal, err1 := strconv.Atoi(Rejected)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Rejected = strconv.Itoa(intVal - tranAmount)
		myLogger.Debug("decremenet in Initiated")
	}
	
	
	if newStatus == "Initiated" {
		intVal, err1 := strconv.Atoi(Initiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Initiated = strconv.Itoa(intVal + tranAmount)
		myLogger.Debug("incremenet in Initiated")
	}
	if newStatus == "Recieved" {
		intVal, err1 := strconv.Atoi(Recieved)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Recieved = strconv.Itoa(intVal + tranAmount)
		myLogger.Debug("incremenet in Recieved")
	}
	if newStatus == "Authorized" {
		intVal, err1 := strconv.Atoi(Authorized)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Authorized = strconv.Itoa(intVal + tranAmount)
		myLogger.Debug("incremenet in Authorized")
	}
	if newStatus == "AuthRecieved" {
		intVal, err1 := strconv.Atoi(AuthRecieved)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		AuthRecieved = strconv.Itoa(intVal + tranAmount)
		myLogger.Debug("incremenet in AuthRecieved")
	}
	if newStatus == "Reconciled" {
		intVal, err1 := strconv.Atoi(Reconciled)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Reconciled = strconv.Itoa(intVal + tranAmount)
		myLogger.Debug("incremenet in Reconciled")
	}
	if newStatus == "BatchInitiated" {
		intVal, err1 := strconv.Atoi(BatchInitiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		BatchInitiated = strconv.Itoa(intVal + tranAmount)
		myLogger.Debug("incremenet in BatchInitiated")
	}
	if newStatus == "SettlementInitiated" {
		intVal, err1 := strconv.Atoi(SettlementInitiated)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		SettlementInitiated = strconv.Itoa(intVal + tranAmount)
		myLogger.Debug("incremenet in SettlementInitiated")
	}
	if newStatus == "Settled" {
		intVal, err1 := strconv.Atoi(Settled)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Settled = strconv.Itoa(intVal + tranAmount)
		myLogger.Debug("incremenet in Settled")
	}
	if newStatus == "Rejected" {
		intVal, err1 := strconv.Atoi(Rejected)
		if err1 != nil{ 
			return nil, fmt.Errorf("Error encountered.")
		}
		Rejected = strconv.Itoa(intVal + tranAmount)
		myLogger.Debug("incremenet in Rejected")
	}
	
	myLogger.Debug("Id", Id)
	myLogger.Debug("Total", Total)
	myLogger.Debug("Initiated", Initiated)
	myLogger.Debug("Recieved", Recieved)
	myLogger.Debug("Authorized", Authorized)
	myLogger.Debug("AuthRecieved", AuthRecieved)
	myLogger.Debug("Reconciled", Reconciled)
	myLogger.Debug("BatchInitiated", BatchInitiated)
	myLogger.Debug("SettlementInitiated", SettlementInitiated)
	myLogger.Debug("Settled", Settled)
	myLogger.Debug("Rejected", Rejected)

	// err = createTableStatusCounts(stub)
	// if err != nil {
		// return nil, fmt.Errorf("Error creating table StatusCounts during init. %s", err)
	// }
	
	var cols []*shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: Id}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: Total}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: Initiated}}
	col4 := shim.Column{Value: &shim.Column_String_{String_: Recieved}}
	col5 := shim.Column{Value: &shim.Column_String_{String_: Authorized}}
	col6 := shim.Column{Value: &shim.Column_String_{String_: AuthRecieved}}
	col7 := shim.Column{Value: &shim.Column_String_{String_: Reconciled}}
	col8 := shim.Column{Value: &shim.Column_String_{String_: BatchInitiated}}
	col9 := shim.Column{Value: &shim.Column_String_{String_: SettlementInitiated}}
	col10 := shim.Column{Value: &shim.Column_String_{String_: Settled}}
	col11 := shim.Column{Value: &shim.Column_String_{String_: Rejected}}
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
	row1 := shim.Row{Columns: cols}
	
	ok, err := stub.ReplaceRow("TranAmounts", row1)
	myLogger.Debug("Replace: ", ok)
	myLogger.Debug("err: ", err)
	if err != nil {
		return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
	}
	if !ok {
		return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
	}
	
	return nil, nil
}

func (t *ReconChaincode) SetTranStatus(stub shim.ChaincodeStubInterface, id string, detail string) ([]byte, error){
	
	Id := id
	Detail := detail
	Status := "Success"
	
	myLogger.Debug("Id: ", Id)
	myLogger.Debug("Detail: ", Detail)
	myLogger.Debug("Status: ", Status)
	
	var cols []shim.Column
	colVal := id
	col := shim.Column{Value: &shim.Column_String_{String_: colVal}}
	cols = append(cols, col)	
	row, err := stub.GetRow("TranStatus", cols)
	myLogger.Debug("row", row)
	myLogger.Debug("len(row.Columns)", len(row.Columns))
	if err != nil {
		return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	
	var columns []*shim.Column
	col1 := shim.Column{Value: &shim.Column_String_{String_: Id}}
	col2 := shim.Column{Value: &shim.Column_String_{String_: Detail}}
	col3 := shim.Column{Value: &shim.Column_String_{String_: Status}}
	columns = append(columns, &col1)
	columns = append(columns, &col2)
	columns = append(columns, &col3)
	insertRow := shim.Row{Columns: columns}
	
	if (len(row.Columns) > 0) {
		ok, err := stub.ReplaceRow("TranStatus", insertRow)
		myLogger.Debug("ReplaceRow: ", ok)
		myLogger.Debug("err: ", err)
		if err != nil {
			return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
		}
		if !ok {
			return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
		}
	} else {
		ok, err := stub.InsertRow("TranStatus", insertRow)
		myLogger.Debug("Insert: ", ok)
		myLogger.Debug("err: ", err)
		if err != nil {
			return nil, fmt.Errorf("insertTableOne operation failed. %s", err)
		}
		if !ok {
			return nil, errors.New("insertTableOne operation failed. Row with given key already exists")
		}
	}
	
	return nil, nil
}

func (t *ReconChaincode) GetUniqueIds(stub shim.ChaincodeStubInterface, numberOfRows int, tranType string) ([]string){
	
	var st []string
	
	maxIndex := t.GetCurrentIndex(stub, tranType)
	minIndex := maxIndex-numberOfRows
	
	//maxIndex, minIndex = t.Paginate(stub, tranType, 2, 10)
	
	if minIndex < 0 {
		minIndex = 0
	}
	myLogger.Debug("maxIndex: ", maxIndex)
	myLogger.Debug("minIndex: ", minIndex)
	if tranType == "Tran" {
		for i := maxIndex; i > minIndex; i-- {
			var columns []shim.Column
			col1 := shim.Column{Value: &shim.Column_String_{String_: strconv.Itoa(i)}}
			columns = append(columns, col1)
			row, err := stub.GetRow("TranIndex", columns)
			if err != nil {
				return nil
			}
			st = append(st, row.Columns[1].GetString_())
		}
	} else if tranType == "Batch" {
		for i := maxIndex; i > minIndex; i-- {
			myLogger.Debug("Inside batch")
			var columns []shim.Column
			col1 := shim.Column{Value: &shim.Column_String_{String_: strconv.Itoa(i)}}
			columns = append(columns, col1)
			row, err := stub.GetRow("BatchIndex", columns)
			if err != nil {
				return nil
			}
			st = append(st, row.Columns[1].GetString_())
		}
	} else {
		return nil
	}
	myLogger.Debug("st: ", st)
	return st
}

func (t *ReconChaincode) GetCurrentIndex(stub shim.ChaincodeStubInterface, tranType string) (int){

	// Get current latest Index from ledger
	var columns []shim.Column
	col1Val := tranType
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	columns = append(columns, col1)		
	row, err := stub.GetRow("Indexes", columns)
	myLogger.Debug("row", row)
	myLogger.Debug("len(row.Columns)", len(row.Columns))
	if err != nil {
		return 0
	}
	if len(row.Columns) < 1 {
	 return 0
	}
	maxIndex, error := strconv.Atoi(row.Columns[1].GetString_())
	if error != nil {
		return 0
	}
	// end
	
	return maxIndex
}

func (t *ReconChaincode) SaveIndexes(stub shim.ChaincodeStubInterface, tranType string, uniqueId string) ([]string){

	
	// Get current latest Index from ledger
	maxIndex := t.GetCurrentIndex(stub, tranType)
	newIndex := maxIndex + 1
	myLogger.Debug("current index: ", maxIndex)
	myLogger.Debug("new index: ", newIndex)
	myLogger.Debug("Current index successfully gotten")
	// end
	
	// Increment index value in ledger	
	var columns1 []*shim.Column
	col11 := shim.Column{Value: &shim.Column_String_{String_: tranType}}
	col21 := shim.Column{Value: &shim.Column_String_{String_: strconv.Itoa(newIndex)}}
	columns1 = append(columns1, &col11)
	columns1 = append(columns1, &col21)
	insertRow := shim.Row{Columns: columns1}
	ok, err1 := stub.ReplaceRow("Indexes", insertRow)
	if err1 != nil {
		return nil
	}
	if !ok {
		return nil
	}
	myLogger.Debug("Insert: ", ok)
	myLogger.Debug("Increment in index successfully done")
	// end
	
	// Insert index and epayId/batchId mapping in table
	if tranType == "Tran" {
		var cols []*shim.Column
		colm1 := shim.Column{Value: &shim.Column_String_{String_: strconv.Itoa(newIndex)}}
		colm2 := shim.Column{Value: &shim.Column_String_{String_: uniqueId}}
		cols = append(cols, &colm1)
		cols = append(cols, &colm2)
		insertRow = shim.Row{Columns: cols}
		ok, err1 = stub.InsertRow("TranIndex", insertRow)

		if err1 != nil {
			return nil
		}
		if !ok {
			return nil
		}
		myLogger.Debug("Insert: ", ok)
		myLogger.Debug("Index-EpayId mapping successfully saved")
	} else if tranType == "Batch" {
		var cols []*shim.Column
		colm1 := shim.Column{Value: &shim.Column_String_{String_: strconv.Itoa(newIndex)}}
		colm2 := shim.Column{Value: &shim.Column_String_{String_: uniqueId}}
		cols = append(cols, &colm1)
		cols = append(cols, &colm2)
		insertRow = shim.Row{Columns: cols}
		ok, err1 = stub.InsertRow("BatchIndex", insertRow)

		if err1 != nil {
			return nil
		}
		if !ok {
			return nil
		}
		myLogger.Debug("Insert: ", ok)
		myLogger.Debug("Index-BatchId mapping successfully saved")
	}
	
	return nil
	// end
}

func (t *ReconChaincode) Paginate(stub shim.ChaincodeStubInterface, tranType string, pageNumber int, pageSize int) (int, int){

	maxIndex := t.GetCurrentIndex(stub, tranType)
	
	if pageSize * (pageNumber-1) > maxIndex {
		newMax := maxIndex - (pageSize * (pageNumber-1))
		newMin := newMax - pageSize
		return newMax, newMin
	} else {
		return 0, 0
	}
}



//////////////// QUERY ////////////////


func (t *ReconChaincode) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error)  {
	if function == "GetTranByStatus" {
		return t.GetTranByStatus(stub, args)
	} else if function == "GetAllTran" {
		return t.GetAllTran(stub, args)
	} else if function == "GetCounts" {
		return t.GetCounts(stub, args)
	} else if function == "GetTranByEpayID" {
		return t.GetTranByEpayID(stub, args)
	} else if function == "GetAllBatch" {
		return t.GetAllBatch(stub, args)
	} else if function == "GetBatchByBatchID" {
		return t.GetBatchByBatchID(stub, args)
	} else if function == "GetTranByBatchID" {
		return t.GetTranByBatchID(stub, args)
	} else if function == "GetAllTranWithoutFilter" {
		return t.GetAllTranWithoutFilter(stub, args)
	} else if function == "GetExceptions" {
		return t.GetExceptions(stub, args)
	} else if function == "GetUnsettledBatch" {
		return t.GetUnsettledBatch(stub, args)
	} else if function == "GetBatchByStatus" {
		return t.GetBatchByStatus(stub, args)
	}  else if function == "GetCompanyCounts" {
		return t.GetCompanyCounts(stub, args)
	}  else if function == "GetAmounts" {
		return t.GetAmounts(stub, args)
	}  else if function == "GetRequestStatusById" {
		return t.GetRequestStatusById(stub, args)
	}  else if function == "GetAllTranTest" {
		return t.GetAllTranTest(stub, args)
	}
	
	return nil, errors.New("Received unknown function invocation")
}

func (t *ReconChaincode) GetTranByStatus_Old(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
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
	//myLogger.Debug("reconStruct: ",reconStruct)
	//myLogger.Debug("reconBytes: ",reconBytes)
	//myLogger.Debug("before marshal Rows: ",rows)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return reconBytes, nil
}

func (t *ReconChaincode) GetTranByStatus(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	epayIds := t.GetUniqueIds(stub, RowsToFetch, "Tran")
	myLogger.Debug("epayIds: ", epayIds)
	var Transactions []ReconciliationStruct
	
	var rows []shim.Row

	for ei, ev := range epayIds {
		
		var columns []shim.Column
		col1Val := args[0]
		col2Val := ev
		myLogger.Debug("ei: ", ei)
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
			rows = append(rows, row1)
			
			reconStruct := ReconciliationStruct{
				Status: row1.Columns[0].GetString_(),
				EpayRefNum: row1.Columns[1].GetString_(),
				EntityRefNum: row1.Columns[2].GetString_(),
				IssuerRefNum: row1.Columns[3].GetString_(),
				BillNumber: row1.Columns[4].GetString_(),
				BillingCompany: row1.Columns[5].GetString_(),
				Issuer: row1.Columns[7].GetString_(),
				Amount: row1.Columns[6].GetString_(),
				BatchID: row1.Columns[8].GetString_(),
				DateTime: row1.Columns[9].GetString_(),
				Details: row1.Columns[10].GetString_(),
			}
			Transactions = append(Transactions, reconStruct)
		}
	}
	
	reconBytes, err := json.Marshal(Transactions)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	myLogger.Debug("reconStruct: ",Transactions)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	myLogger.Debug("Rows Fetched: ", RowsToFetch)
	return reconBytes, nil
}

func (t *ReconChaincode) GetAllTran_Old(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
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
				if (row.Columns[0].GetString_() == "Initiated") || (row.Columns[0].GetString_() == "Recieved") || (row.Columns[0].GetString_() == "Authorized")|| (row.Columns[0].GetString_() == "AuthRecieved") || (row.Columns[0].GetString_() == "Reconciled") {
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
		}
		if rowChannel == nil {
			break
		}
	}
	
	reconBytes, err := json.Marshal(Transactions)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	//myLogger.Debug("reconStruct: ",Transactions)
	//myLogger.Debug("reconBytes: ",reconBytes)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return reconBytes, nil
}

func (t *ReconChaincode) GetAllTran(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	epayIds := t.GetUniqueIds(stub, RowsToFetch, "Tran")
	myLogger.Debug("epayIds: ", epayIds)
	var Transactions []ReconciliationStruct
	
	var rows []shim.Row
	var st []string
	
	st = append(st, "Initiated")
	st = append(st, "Recieved")
	st = append(st, "Authorized")
	st = append(st, "AuthRecieved")
	st = append(st, "Reconciled")
	// st = append(st, "BatchInitiated")
	// st = append(st, "SettlementInitiated")
	// st = append(st, "Settled")

	for ei, ev := range epayIds {
		for si, sv := range st {
			var columns []shim.Column
			col1Val := sv
			col2Val := ev
			myLogger.Debug("ei: ", ei)
			myLogger.Debug("si: ", si)
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
				rows = append(rows, row1)
				
				reconStruct := ReconciliationStruct{
					Status: row1.Columns[0].GetString_(),
					EpayRefNum: row1.Columns[1].GetString_(),
					EntityRefNum: row1.Columns[2].GetString_(),
					IssuerRefNum: row1.Columns[3].GetString_(),
					BillNumber: row1.Columns[4].GetString_(),
					BillingCompany: row1.Columns[5].GetString_(),
					Issuer: row1.Columns[7].GetString_(),
					Amount: row1.Columns[6].GetString_(),
					BatchID: row1.Columns[8].GetString_(),
					DateTime: row1.Columns[9].GetString_(),
					Details: row1.Columns[10].GetString_(),
				}
				Transactions = append(Transactions, reconStruct)
			}
		}
	}
	
	reconBytes, err := json.Marshal(Transactions)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	myLogger.Debug("reconStruct: ",Transactions)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	myLogger.Debug("Rows Fetched: ", RowsToFetch)
	return reconBytes, nil
}

func (t *ReconChaincode) GetAllTranWithoutFilter_Old(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
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
	//myLogger.Debug("reconStruct: ",Transactions)
	//myLogger.Debug("reconBytes: ",reconBytes)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return reconBytes, nil
}

func (t *ReconChaincode) GetAllTranWithoutFilter(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	epayIds := t.GetUniqueIds(stub, RowsToFetch, "Tran")
	myLogger.Debug("epayIds: ", epayIds)
	var Transactions []ReconciliationStruct
	
	var rows []shim.Row
	var st []string
	
	st = append(st, "Initiated")
	st = append(st, "Recieved")
	st = append(st, "Authorized")
	st = append(st, "AuthRecieved")
	st = append(st, "Reconciled")
	st = append(st, "BatchInitiated")
	st = append(st, "SettlementInitiated")
	st = append(st, "Settled")

	for ei, ev := range epayIds {
		for si, sv := range st {
			var columns []shim.Column
			col1Val := sv
			col2Val := ev
			myLogger.Debug("ei: ", ei)
			myLogger.Debug("si: ", si)
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
				rows = append(rows, row1)
				
				reconStruct := ReconciliationStruct{
					Status: row1.Columns[0].GetString_(),
					EpayRefNum: row1.Columns[1].GetString_(),
					EntityRefNum: row1.Columns[2].GetString_(),
					IssuerRefNum: row1.Columns[3].GetString_(),
					BillNumber: row1.Columns[4].GetString_(),
					BillingCompany: row1.Columns[5].GetString_(),
					Issuer: row1.Columns[7].GetString_(),
					Amount: row1.Columns[6].GetString_(),
					BatchID: row1.Columns[8].GetString_(),
					DateTime: row1.Columns[9].GetString_(),
					Details: row1.Columns[10].GetString_(),
				}
				Transactions = append(Transactions, reconStruct)
			}
		}
	}
	
	reconBytes, err := json.Marshal(Transactions)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	myLogger.Debug("reconStruct: ",Transactions)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	myLogger.Debug("Rows Fetched: ", RowsToFetch)
	return reconBytes, nil
}

func (t *ReconChaincode) GetAllBatch_Old(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	var columns []shim.Column
	
	rowChannel, err := stub.GetRows("Batch", columns)
	myLogger.Debug("Rows: ",rowChannel)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}
	var Batches []BatchStruct
	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				rows = append(rows, row)
				
				batch := BatchStruct{
				BatchID: row.Columns[0].GetString_(),
				BillingCompany: row.Columns[1].GetString_(),
				Issuer: row.Columns[2].GetString_(),
				Amount: row.Columns[3].GetString_(),
				Status: row.Columns[4].GetString_(),
				DateTime: row.Columns[5].GetString_(),
				Details: row.Columns[6].GetString_(),
				}
				Batches = append(Batches, batch)				
			}
		}
		if rowChannel == nil {
			break
		}
	}
	
	batchBytes, err := json.Marshal(Batches)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	//myLogger.Debug("reconStruct: ",Batches)
	//myLogger.Debug("reconBytes: ",batchBytes)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return batchBytes, nil
}

func (t *ReconChaincode) GetAllBatch(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	myLogger.Debug("inside")
	batchIds := t.GetUniqueIds(stub, RowsToFetch, "Batch")
	myLogger.Debug("batchIds: ", batchIds)

	var Batches []BatchStruct

	for bi, bv := range batchIds {
		
		var columns []shim.Column
		col1Val := string(bv)
		myLogger.Debug("bi: ", bi)
		myLogger.Debug("col1Val: ", col1Val)
		col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
		columns = append(columns, col1)	
		row1, err := stub.GetRow("Batch", columns)
		myLogger.Debug("len(row1.Columns): ", len(row1.Columns))
		if err != nil {
		 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
		}
		if len(row1.Columns) > 1 {
			bacthStruct := BatchStruct {
				BatchID: row1.Columns[0].GetString_(),
				BillingCompany: row1.Columns[1].GetString_(),
				Issuer: row1.Columns[2].GetString_(),
				Amount: row1.Columns[3].GetString_(),
				Status: row1.Columns[4].GetString_(),
				DateTime: row1.Columns[5].GetString_(),
				Details: row1.Columns[6].GetString_(),
			}				
			Batches = append(Batches, bacthStruct)
		}
	}
	
	batchBytes, err := json.Marshal(Batches)
	if err != nil {
		myLogger.Errorf("batch transaction marshaling error %v", err)
	}
	myLogger.Debug("batchStruct: ",Batches)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	myLogger.Debug("Rows Fetched: ", RowsToFetch)
	return batchBytes, nil
}

func (t *ReconChaincode) GetBatchByStatus_Old(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	var columns []shim.Column
	
	rowChannel, err := stub.GetRows("Batch", columns)
	myLogger.Debug("Rows: ",rowChannel)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}
	var Batches []BatchStruct
	var rows []shim.Row
	for {
		select {	
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				if row.Columns[4].GetString_() == args[0] {
					rows = append(rows, row)
					
					batch := BatchStruct{
					BatchID: row.Columns[0].GetString_(),
					BillingCompany: row.Columns[1].GetString_(),
					Issuer: row.Columns[2].GetString_(),
					Amount: row.Columns[3].GetString_(),
					Status: row.Columns[4].GetString_(),
					DateTime: row.Columns[5].GetString_(),
					Details: row.Columns[6].GetString_(),
					}
					Batches = append(Batches, batch)				
				}
			}
		}
		if rowChannel == nil {
			break
		}
	}
	
	batchBytes, err := json.Marshal(Batches)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	//myLogger.Debug("reconStruct: ",Batches)
	//myLogger.Debug("reconBytes: ",batchBytes)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return batchBytes, nil
}

func (t *ReconChaincode) GetBatchByStatus(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	batchIds := t.GetUniqueIds(stub, RowsToFetch, "Batch")
	myLogger.Debug("batchIds: ", batchIds)

	var Batches []BatchStruct

	for bi, bv := range batchIds {
		
		var columns []shim.Column
		col1Val := bv
		myLogger.Debug("bi: ", bi)
		myLogger.Debug("col1Val: ", col1Val)
		col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
		columns = append(columns, col1)	
		row1, err := stub.GetRow("Batch", columns)
		myLogger.Debug("len(row1.Columns): ", len(row1.Columns))
		if err != nil {
		 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
		}
		if len(row1.Columns) > 1 {
			if row1.Columns[4].GetString_() == args[0] {
				bacthStruct := BatchStruct {
					BatchID: row1.Columns[0].GetString_(),
					BillingCompany: row1.Columns[1].GetString_(),
					Issuer: row1.Columns[2].GetString_(),
					Amount: row1.Columns[3].GetString_(),
					Status: row1.Columns[4].GetString_(),
					DateTime: row1.Columns[5].GetString_(),
					Details: row1.Columns[6].GetString_(),
				}				
				Batches = append(Batches, bacthStruct)
			}
		}
	}
	
	batchBytes, err := json.Marshal(Batches)
	if err != nil {
		myLogger.Errorf("batch transaction marshaling error %v", err)
	}
	myLogger.Debug("batchStruct: ",Batches)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	myLogger.Debug("Rows Fetched: ", RowsToFetch)
	return batchBytes, nil
}

func (t *ReconChaincode) GetCounts(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	var cols1 []shim.Column	
	totRow, err := stub.GetRows("StatusCounts", cols1)		
	
	// zero index is table ID
	Total := 0
	Initiated := 0
	Recieved := 0
	Authorized := 0
	AuthRecieved := 0
	Reconciled := 0
	BatchInitiated := 0
	SettlementInitiated := 0
	Settled := 0
	Rejected := 0
	myLogger.Debug("Total number of transactions: ",Total)
	for {
		select {
		case row, ok := <-totRow:
			if !ok {
				totRow = nil
			} else {
				Total, err = strconv.Atoi(row.Columns[1].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Initiated, err = strconv.Atoi(row.Columns[2].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Recieved, err = strconv.Atoi(row.Columns[3].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Authorized, err = strconv.Atoi(row.Columns[4].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				AuthRecieved, err = strconv.Atoi(row.Columns[5].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Reconciled, err = strconv.Atoi(row.Columns[6].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				BatchInitiated, err = strconv.Atoi(row.Columns[7].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				SettlementInitiated, err = strconv.Atoi(row.Columns[8].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Settled, err = strconv.Atoi(row.Columns[9].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Rejected, err = strconv.Atoi(row.Columns[10].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
			}
		}
		if totRow == nil {
			break
		}
	}
	
	
	
	myLogger.Debug("Total number of transactions: ",Total)
	myLogger.Debug("Number of initiated transactions: ",Initiated)
	myLogger.Debug("Number of recieved transactions: ",Recieved)
	myLogger.Debug("Number of authorized transactions: ",Authorized)
	myLogger.Debug("Number of authRecieved transactions: ",AuthRecieved)
	myLogger.Debug("Number of reconciled transactions: ",Reconciled)
	myLogger.Debug("Number of batchInitiated transactions: ",BatchInitiated)
	myLogger.Debug("Number of settlementInitiated transactions: ",SettlementInitiated)
	myLogger.Debug("Number of settled transactions: ",Settled)
	myLogger.Debug("Number of rejected transactions: ",Rejected)
	
	tranC := TranCounts{
		Total: Total,
		Initiated: Initiated,
		Recieved: Recieved,
		Authorized: Authorized,
		AuthRecieved: AuthRecieved,		
		Reconciled: Reconciled,
		BatchInitiated: BatchInitiated,
		SettlementInitiated: SettlementInitiated,
		Settled: Settled,
		Rejected: Rejected,
	}
	
	tranBytes, err := json.Marshal(tranC)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}

	return tranBytes, nil
}

func (t *ReconChaincode) GetCompanyCounts(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	var cols1 []shim.Column	
	totRow, err := stub.GetRows("CompanyCounts", cols1)		
	
	// zero index is table ID
	RTA := 0
	Dewa := 0
	DU := 0
	Etisalat := 0
	DubaiCustoms := 0
	Others := 0
	
	for {
		select {
		case row, ok := <-totRow:
			if !ok {
				totRow = nil
			} else {
				RTA, err = strconv.Atoi(row.Columns[1].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Dewa, err = strconv.Atoi(row.Columns[2].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				DU, err = strconv.Atoi(row.Columns[3].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Etisalat, err = strconv.Atoi(row.Columns[4].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				DubaiCustoms, err = strconv.Atoi(row.Columns[5].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Others, err = strconv.Atoi(row.Columns[6].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
			}
		}
		if totRow == nil {
			break
		}
	}

	myLogger.Debug("Number of RTA transactions: : ",RTA)
	myLogger.Debug("Number of Dewa transactions: ",Dewa)
	myLogger.Debug("Number of DU transactions: ",DU)
	myLogger.Debug("Number of Etisalat transactions: ",Etisalat)
	myLogger.Debug("Number of DubaiCustoms transactions: ",DubaiCustoms)
	myLogger.Debug("Number of Others transactions: ",Others)
	
	tranC := TranCountsEntity{
		RTA: RTA,
		DEWA: Dewa,
		Etisalat: Etisalat,
		DU: DU,
		DubaiCustoms: DubaiCustoms,		
		Others: Others,
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
	st = append(st, "BatchInitiated")
	st = append(st, "SettlementInitiated")
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
	//myLogger.Debug("reconStruct: ",reconStruct)
	//myLogger.Debug("reconBytes: ",reconBytes)
	//myLogger.Debug("before marshal Rows: ",row)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return reconBytes, nil
}

func (t *ReconChaincode) GetBatchByBatchID(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}
	
	var row shim.Row
	
	var columns []shim.Column
	col1Val := args[0]
	myLogger.Debug("col1Val: ", col1Val)
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	columns = append(columns, col1)	
	row, err := stub.GetRow("Batch", columns)
	myLogger.Debug("len(row1.Columns): ", len(row.Columns))
	if err != nil {
	 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	if (len(row.Columns) < 1){
		return nil, nil
	}
	
	bacthStruct := BatchStruct{
		BatchID: row.Columns[0].GetString_(),
		BillingCompany: row.Columns[1].GetString_(),
		Issuer: row.Columns[2].GetString_(),
		Amount: row.Columns[3].GetString_(),
		Status: row.Columns[4].GetString_(),
		DateTime: row.Columns[5].GetString_(),
		Details: row.Columns[6].GetString_(),
	}
	batchBytes, err := json.Marshal(bacthStruct)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	//myLogger.Debug("bacthStruct: ",bacthStruct)
	//myLogger.Debug("batchBytes: ",batchBytes)
	//myLogger.Debug("before marshal Rows: ",row)

	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return batchBytes, nil
}

func (t *ReconChaincode) GetUniqueBillCompany(stub shim.ChaincodeStubInterface) ([]string){
	
	var bcArray []string
	
	var columns []shim.Column
	col1Val := "Reconciled"
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	columns = append(columns, col1)	
	rowChannel, err := stub.GetRows("Reconciliation", columns)
	myLogger.Debug("Rows: ",rowChannel)
	if err != nil {
		return nil
	}

	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				bcArray = append(bcArray, row.Columns[5].GetString_())
			}
		}
		if rowChannel == nil {
			break
		}
	}
	
	encountered := map[string]bool{}
    // Create a map of all unique elements.
    for v:= range bcArray {
        encountered[bcArray[v]] = true
    }
    // Place all keys from the map into a slice.
    result := []string{}
    for key, _ := range encountered {
        result = append(result, key)
    }

	myLogger.Debug("result: ",result)

	return result
}

func (t *ReconChaincode) GetTranByBatchID(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
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
				if (row.Columns[8].GetString_() == args[0]) {
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
	//myLogger.Debug("reconBytes: ",reconBytes)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return reconBytes, nil
}

func (t *ReconChaincode) GetExceptions(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	var columns []shim.Column
	
	// 0 = All
	// 1 = Entity
	// 2 = Epay
	// 3 = Issuer
	myLogger.Debug("Argument recieved: ",args[0])
	
	if args[0] == "0" {
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
				if (row.Columns[0].GetString_() == "Initiated") || (row.Columns[0].GetString_() == "Recieved") || (row.Columns[0].GetString_() == "Authorized")|| (row.Columns[0].GetString_() == "AuthRecieved") {
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
			}
			if rowChannel == nil {
				break
			}
		}
		
		reconBytes, err := json.Marshal(Transactions)
		if err != nil {
			myLogger.Errorf("reconciliation transaction marshaling error %v", err)
		}
		//myLogger.Debug("reconStruct: ",Transactions)
		//myLogger.Debug("reconBytes: ",reconBytes)
		//jsonRows, err := json.Marshal(rows)
		if err != nil {
			return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
		}

		return reconBytes, nil
	}
	
	if args[0] == "1" {
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
					if row.Columns[0].GetString_() == "AuthRecieved" {
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
			}
			if rowChannel == nil {
				break
			}
		}
		
		reconBytes, err := json.Marshal(Transactions)
		if err != nil {
			myLogger.Errorf("reconciliation transaction marshaling error %v", err)
		}
		//myLogger.Debug("reconStruct: ",Transactions)
		//myLogger.Debug("reconBytes: ",reconBytes)
		//jsonRows, err := json.Marshal(rows)
		if err != nil {
			return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
		}

		return reconBytes, nil
	}
	
	if args[0] == "2" {
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
					if (row.Columns[0].GetString_() == "Initiated") || (row.Columns[0].GetString_() == "Authorized") {
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
			}
			if rowChannel == nil {
				break
			}
		}
		
		reconBytes, err := json.Marshal(Transactions)
		if err != nil {
			myLogger.Errorf("reconciliation transaction marshaling error %v", err)
		}
		//myLogger.Debug("reconStruct: ",Transactions)
		//myLogger.Debug("reconBytes: ",reconBytes)
		//jsonRows, err := json.Marshal(rows)
		if err != nil {
			return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
		}

		return reconBytes, nil
	}
	
	if args[0] == "3" {
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
					if row.Columns[0].GetString_() == "Recieved" {
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
			}
			if rowChannel == nil {
				break
			}
		}
		
		reconBytes, err := json.Marshal(Transactions)
		if err != nil {
			myLogger.Errorf("reconciliation transaction marshaling error %v", err)
		}
		//myLogger.Debug("reconStruct: ",Transactions)
		//myLogger.Debug("reconBytes: ",reconBytes)
		//jsonRows, err := json.Marshal(rows)
		if err != nil {
			return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
		}

		return reconBytes, nil
	}
	
	return nil, nil
}

func (t *ReconChaincode) GetUnsettledBatch(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	var columns []shim.Column
	
	rowChannel, err := stub.GetRows("Batch", columns)
	myLogger.Debug("Rows: ",rowChannel)
	if err != nil {
		return nil, fmt.Errorf("operation failed. %s", err)
	}
	var Batches []BatchStruct
	var rows []shim.Row
	for {
		select {
		case row, ok := <-rowChannel:
			if !ok {
				rowChannel = nil
			} else {
				if row.Columns[4].GetString_() == "SettlementInitiated" {
					rows = append(rows, row)
					
					batch := BatchStruct{
					BatchID: row.Columns[0].GetString_(),
					BillingCompany: row.Columns[1].GetString_(),
					Issuer: row.Columns[2].GetString_(),
					Amount: row.Columns[3].GetString_(),
					Status: row.Columns[4].GetString_(),
					DateTime: row.Columns[5].GetString_(),
					Details: row.Columns[6].GetString_(),
					}
					Batches = append(Batches, batch)				
				}
			}
		}
		if rowChannel == nil {
			break
		}
	}
	
	batchBytes, err := json.Marshal(Batches)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	//myLogger.Debug("reconStruct: ",Batches)
	//myLogger.Debug("reconBytes: ",batchBytes)
	//jsonRows, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return batchBytes, nil
}

func (t *ReconChaincode) GetAmounts(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	var cols1 []shim.Column	
	totRow, err := stub.GetRows("TranAmounts", cols1)		
	
	// zero index is table ID
	Total := 0
	Initiated := 0
	Recieved := 0
	Authorized := 0
	AuthRecieved := 0
	Reconciled := 0
	BatchInitiated := 0
	SettlementInitiated := 0
	Settled := 0
	Rejected := 0
	myLogger.Debug("Total number of transactions: ",Total)
	for {
		select {
		case row, ok := <-totRow:
			if !ok {
				totRow = nil
			} else {
				Total, err = strconv.Atoi(row.Columns[1].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Initiated, err = strconv.Atoi(row.Columns[2].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Recieved, err = strconv.Atoi(row.Columns[3].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Authorized, err = strconv.Atoi(row.Columns[4].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				AuthRecieved, err = strconv.Atoi(row.Columns[5].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Reconciled, err = strconv.Atoi(row.Columns[6].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				BatchInitiated, err = strconv.Atoi(row.Columns[7].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				SettlementInitiated, err = strconv.Atoi(row.Columns[8].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Settled, err = strconv.Atoi(row.Columns[9].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
				Rejected, err = strconv.Atoi(row.Columns[10].GetString_())
				if err != nil { 
					return nil, fmt.Errorf("Error encountered.")
				}
			}
		}
		if totRow == nil {
			break
		}
	}
	
	
	
	myLogger.Debug("Total amount of transactions: ",Total)
	myLogger.Debug("Amount of initiated transactions: ",Initiated)
	myLogger.Debug("Amount of recieved transactions: ",Recieved)
	myLogger.Debug("Amount of authorized transactions: ",Authorized)
	myLogger.Debug("Amount of authRecieved transactions: ",AuthRecieved)
	myLogger.Debug("Amount of reconciled transactions: ",Reconciled)
	myLogger.Debug("Amount of batchInitiated transactions: ",BatchInitiated)
	myLogger.Debug("Amount of settlementInitiated transactions: ",SettlementInitiated)
	myLogger.Debug("Amount of settled transactions: ",Settled)
	myLogger.Debug("Amount of rejected transactions: ",Rejected)
	
	tranC := TranCounts{
		Total: Total,
		Initiated: Initiated,
		Recieved: Recieved,
		Authorized: Authorized,
		AuthRecieved: AuthRecieved,		
		Reconciled: Reconciled,
		BatchInitiated: BatchInitiated,
		SettlementInitiated: SettlementInitiated,
		Settled: Settled,
		Rejected: Rejected,
	}
	
	tranBytes, err := json.Marshal(tranC)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}

	return tranBytes, nil
}

func (t *ReconChaincode) GetRequestStatusById(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){

	var row shim.Row
	
	var columns []shim.Column
	col1Val := args[0]
	myLogger.Debug("col1Val: ", col1Val)
	col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
	columns = append(columns, col1)	
	row, err := stub.GetRow("TranStatus", columns)
	myLogger.Debug("len(row1.Columns): ", len(row.Columns))
	if err != nil {
	 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
	}
	if (len(row.Columns) < 1){
		return nil, nil
	}

	data := RequestStatus{
		Id: row.Columns[0].GetString_(),
		Details: row.Columns[1].GetString_(),
		Status: row.Columns[2].GetString_(),
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	myLogger.Debug("data: ",data)
	myLogger.Debug("jsonData: ",jsonData)
	myLogger.Debug("before marshal Rows: ",row)

	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	return jsonData, nil
}

func (t *ReconChaincode) GetAllTranTest(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	epayIds := t.GetUniqueIds(stub, RowsToFetch, "Tran")
	myLogger.Debug("epayIds: ", epayIds)
	var Transactions []ReconciliationStruct
	
	//var rows []shim.Row
	var st []string
	
	st = append(st, "Initiated")
	st = append(st, "Recieved")
	st = append(st, "Authorized")
	st = append(st, "AuthRecieved")
	st = append(st, "Reconciled")
	st = append(st, "BatchInitiated")
	st = append(st, "SettlementInitiated")
	st = append(st, "Settled")

	for ei, ev := range epayIds {
		for si, sv := range st {
			var columns []shim.Column
			col1Val := sv
			col2Val := ev
			myLogger.Debug("ei: ", ei)
			myLogger.Debug("si: ", si)
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
				//rows = append(rows, row1)
				
				reconStruct := ReconciliationStruct{
					Status: row1.Columns[0].GetString_(),
					EpayRefNum: row1.Columns[1].GetString_(),
					EntityRefNum: row1.Columns[2].GetString_(),
					IssuerRefNum: row1.Columns[3].GetString_(),
					BillNumber: row1.Columns[4].GetString_(),
					BillingCompany: row1.Columns[5].GetString_(),
					Issuer: row1.Columns[7].GetString_(),
					Amount: row1.Columns[6].GetString_(),
					BatchID: row1.Columns[8].GetString_(),
					DateTime: row1.Columns[9].GetString_(),
					Details: row1.Columns[10].GetString_(),
				}
				Transactions = append(Transactions, reconStruct)
			}
		}
	}
	
	reconBytes, err := json.Marshal(Transactions)
	if err != nil {
		myLogger.Errorf("reconciliation transaction marshaling error %v", err)
	}
	myLogger.Debug("reconStruct: ",Transactions)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	myLogger.Debug("Rows Fetched: ", RowsToFetch)
	return reconBytes, nil
}

func (t *ReconChaincode) GetAllBatchTest(stub shim.ChaincodeStubInterface, args []string) ([]byte, error){
	
	batchIds := t.GetUniqueIds(stub, RowsToFetch, "Batch")
	myLogger.Debug("batchIds: ", batchIds)

	var Batches []BatchStruct

	for bi, bv := range batchIds {
		
		var columns []shim.Column
		col1Val := bv
		myLogger.Debug("bi: ", bi)
		myLogger.Debug("col1Val: ", col1Val)
		col1 := shim.Column{Value: &shim.Column_String_{String_: col1Val}}
		columns = append(columns, col1)	
		row1, err := stub.GetRow("Batch", columns)
		myLogger.Debug("len(row1.Columns): ", len(row1.Columns))
		if err != nil {
		 return nil, fmt.Errorf("getRowTableOne operation failed. %s", err)
		}
		if len(row1.Columns) > 1 {
			//rows = append(rows, row1)
			
			bacthStruct := BatchStruct{
				BatchID: row1.Columns[0].GetString_(),
				BillingCompany: row1.Columns[1].GetString_(),
				Issuer: row1.Columns[2].GetString_(),
				Amount: row1.Columns[3].GetString_(),
				Status: row1.Columns[4].GetString_(),
				DateTime: row1.Columns[5].GetString_(),
				Details: row1.Columns[6].GetString_(),
			}				

			Batches = append(Batches, bacthStruct)
		}
	}
	
	batchBytes, err := json.Marshal(Batches)
	if err != nil {
		myLogger.Errorf("batch transaction marshaling error %v", err)
	}
	myLogger.Debug("batchStruct: ",Batches)
	if err != nil {
		return nil, fmt.Errorf("Operation failed. Error marshaling JSON: %s", err)
	}

	myLogger.Debug("Rows Fetched: ", RowsToFetch)
	return batchBytes, nil
}