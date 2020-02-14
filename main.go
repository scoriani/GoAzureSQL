package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	sqlsdk "github.com/Azure/azure-sdk-for-go/services/preview/sql/mgmt/2015-05-01-preview/sql"
	"github.com/Azure/azure-sdk-for-go/services/resources/mgmt/2017-05-10/resources"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/Azure/go-autorest/autorest/to"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
)

var db *sql.DB

var server = "my_server_name"
var port = 1433
var user = "username"
var password = "password"
var database = "databasename"
var subid = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
var resgroup = "goazuresql"
var location = "my_azure_location"

func main() {

	// Deploy a new server using Azure SDK for GO
	deployserver()
	// Create a new database using Azure SDK for GO
	createdatabase()
	// Verify database connectivity
	databaseconnect()
	// Insert a new record using mssql driver for GO
	insertrecord()
	// Query database using mssql driver for GO
	querydatabase()
	// Invoke a Stored Procedure using mssql driver for GO
	invokestoredproc()
	// Interact with database using Gorm (ORM for GO)
	usegorm()
	// Cleanup resource group
	cleanup()
}

func databaseconnect() {
	connString := fmt.Sprintf("server=%s.database.windows.net;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)

	var err error

	// Open a connection with Azure SQL
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	ctx := context.Background()
	// Verify that connection is open
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Printf("Connected!\n")
}

func deployserver() {

	var err error
	// Create auth token from env variables (see here for details https://github.com/Azure/azure-sdk-for-go)
	authorizer, err := auth.NewAuthorizerFromEnvironment()
	if err == nil {
		// Create resource group SDK client
		groupsClient := resources.NewGroupsClient(subid)
		groupsClient.Authorizer = authorizer
		// Create AzureSQL SDK client
		sqlclient := sqlsdk.NewServersClient(subid)
		sqlclient.Authorizer = authorizer

		ctx := context.Background()

		// Create new resource group
		_, err := groupsClient.CreateOrUpdate(ctx, resgroup, resources.Group{Location: to.StringPtr(location)})
		if err != nil {
			PrintAndLog(err.Error())
		}

		var future sqlsdk.ServersCreateOrUpdateFuture

		// Create new Azure SQL virtual server
		future, err = sqlclient.CreateOrUpdate(
			ctx,
			resgroup, server,
			sqlsdk.Server{
				Location: to.StringPtr(location),
				ServerProperties: &sqlsdk.ServerProperties{
					AdministratorLogin:         to.StringPtr(user),
					AdministratorLoginPassword: to.StringPtr(password),
					Version:                    to.StringPtr("12.0"),
				},
			})
		if err != nil {
			PrintAndLog(fmt.Sprintf("cannot create sql server: %v", err))
		}
		// Wait for async operation completion
		err = future.WaitForCompletionRef(ctx, sqlclient.Client)
		if err != nil {
			PrintAndLog(fmt.Sprintf("Cannot create logical SQL Server. Create or update future response: %v", err))
		}
		// Create new firewall rule client
		fwrClient := sqlsdk.NewFirewallRulesClient(subid)
		fwrClient.Authorizer = authorizer
		// Create new firewal rule
		_, err = fwrClient.CreateOrUpdate(
			ctx,
			resgroup,
			server,
			"my_client_address",
			sqlsdk.FirewallRule{
				FirewallRuleProperties: &sqlsdk.FirewallRuleProperties{
					StartIPAddress: to.StringPtr("XXX.XXX.XXX.XXX"),
					EndIPAddress:   to.StringPtr("XXX.XXX.XXX.XXX"),
				},
			},
		)
	}
}

func createdatabase() {

	var err error

	// Create auth token from env variables (see here for details https://github.com/Azure/azure-sdk-for-go)
	authorizer, err := auth.NewAuthorizerFromEnvironment()
	if err == nil {
		// Create SDK database client
		dbclient := sqlsdk.NewDatabasesClient(subid)
		dbclient.Authorizer = authorizer

		ctx := context.Background()
		// Create new database from AdventureWorksLT sample
		future, err := dbclient.CreateOrUpdate(
			ctx,
			resgroup,
			server,
			database,
			sqlsdk.Database{
				Location: to.StringPtr(location),
				DatabaseProperties: &sqlsdk.DatabaseProperties{
					Edition:                       "GeneralPurpose",
					SampleName:                    "AdventureWorksLT",
					RequestedServiceObjectiveName: "GP_Gen4_1",
				},
			})
		if err != nil {
			PrintAndLog(fmt.Sprintf("cannot create sql database: %v", err))
		}
		// Wait for async operation completion
		err = future.WaitForCompletionRef(ctx, dbclient.Client)
		if err != nil {
			PrintAndLog(fmt.Sprintf("cannot get the sql database create or update future response: %v", err))
		}

	}
}

func querydatabase() {

	connString := fmt.Sprintf("server=%s.database.windows.net;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)

	var err error

	// Open database connection
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}

	if err != nil {
		log.Fatal(err.Error())
	}
	// Database command
	tsql := fmt.Sprintf(`SELECT 
			SOH.SalesOrderNumber,
			SOH.OrderDate,
			SOH.CustomerID,
			SOD.ProductID,
			SOD.OrderQty,
			SOD.UnitPrice
		FROM SalesLT.SalesOrderHeader SOH 
			INNER JOIN SalesLT.SalesOrderDetail SOD ON SOH.SalesOrderID = SOD.SalesOrderID
		WHERE SOH.SalesOrderID = @orderid;`)

	ctx := context.Background()

	orderid := 71797

	// Execute query
	rows, err := db.QueryContext(ctx, tsql, sql.Named("orderid", orderid))

	defer rows.Close()

	var (
		ordernumber   string
		orderdate     string
		customerid    string
		productid     string
		orderquantity string
		unitprice     string
	)
	// Scan all rows
	for rows.Next() {
		if err := rows.Scan(&ordernumber, &orderdate, &customerid, &productid, &orderquantity, &unitprice); err != nil {
			log.Fatal(err)
		}
		PrintAndLog(fmt.Sprintf("SalesOrderNumber: %s - OrderDate: %s - CustomerID: %s - ProductID: %s - OrderQty: %s - UnitPrice: %s",
			ordernumber, orderdate, customerid, productid, orderquantity, unitprice))
	}
	// Cleanup resources
	rerr := rows.Close()
	db.Close()

	if rerr != nil {
		log.Fatal(err)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func insertrecord() {

	connString := fmt.Sprintf("server=%s.database.windows.net;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)

	var err error

	// Open database connection
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	ctx := context.Background()
	if err != nil {
		log.Fatal(err.Error())
	}

	var parentcategoryid int = 1
	var name string = uuid.New().String()
	// T-SQL command
	tsql := fmt.Sprintf(`INSERT INTO [SalesLT].[ProductCategory]
							([ParentProductCategoryID]
							,[Name])
							VALUES (@parentid,@name);
							SELECT CONVERT(bigint, SCOPE_IDENTITY());
							`)
	// Prepare statement
	stmt, err := db.Prepare(tsql)
	if err != nil {
		return
	}
	defer stmt.Close()
	// Execute command
	row := stmt.QueryRowContext(
		ctx,
		sql.Named("parentid", parentcategoryid),
		sql.Named("name", name))
	var newID int64
	// Read identity value
	err = row.Scan(&newID)
	if err != nil {
		return
	}

	PrintAndLog(fmt.Sprintf("Insert new Product Category with ID: %d \n",
		newID))
	// Cleanup resources
	db.Close()
}

func invokestoredproc() {

	connString := fmt.Sprintf("server=%s.database.windows.net;user id=%s;password=%s;port=%d;database=%s;",
		server, user, password, port, database)

	var err error

	// Open database connection
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}

	if err != nil {
		log.Fatal(err.Error())
	}

	// Dropping the stored procedure if exists
	var tsql string = `
	DECLARE @mycmd VARCHAR (4000);
	SET @mycmd = 'IF EXISTS (SELECT name FROM sys.objects WHERE name =''spGetOrder'')
	BEGIN  
		DROP PROC spGetOrder
	END';
	EXEC (@mycmd);
	`
	// Execute DDL command
	_, err = db.Exec(tsql)
	if err != nil {
		log.Println(err)
		return
	}

	// Creating the stored procedure
	tsql = `
	DECLARE @mycmd VARCHAR (4000);
	SET @mycmd = 'CREATE PROC spGetOrder @orderid INT
	AS
	BEGIN
		SELECT 
				SOH.SalesOrderNumber,
				SOH.OrderDate,
				SOH.CustomerID,
				SOD.ProductID,
				SOD.OrderQty,
				SOD.UnitPrice
			FROM SalesLT.SalesOrderHeader SOH 
				INNER JOIN SalesLT.SalesOrderDetail SOD ON SOH.SalesOrderID = SOD.SalesOrderID
			WHERE SOH.SalesOrderID = @orderid;
	END';
	EXEC (@mycmd);
	`
	//  Execute DDL command
	_, err = db.Exec(tsql)
	if err != nil {
		log.Println(err)
		return
	}

	var orderid int = 71797
	// Execute the stored procedure
	rows, err := db.Query("EXEC spGetOrder @orderid",
		sql.Named("orderid", orderid),
	)
	if err != nil {
		log.Println(err)
		return
	}

	var (
		ordernumber   string
		orderdate     string
		customerid    string
		productid     string
		orderquantity string
		unitprice     string
	)

	for rows.Next() {
		if err := rows.Scan(&ordernumber, &orderdate, &customerid, &productid, &orderquantity, &unitprice); err != nil {
			log.Fatal(err)
		}
		PrintAndLog(fmt.Sprintf("SalesOrderNumber: %s - OrderDate: %s - CustomerID: %s - ProductID: %s - OrderQty: %s - UnitPrice: %s",
			ordernumber, orderdate, customerid, productid, orderquantity, unitprice))
	}

	// Cleanup resources
	rerr := rows.Close()
	db.Close()

	if rerr != nil {
		log.Fatal(err)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

}

// Type definitions for Gorm

// OrderHeader struct
type OrderHeader struct {
	OrderNumber string        `gorm:"column:SalesOrderID;primary_key"`
	OrderDate   string        `gorm:"column:OrderDate"`
	CustomerID  string        `gorm:"column:CustomerID"`
	Details     []OrderDetail `gorm:"foreignkey:OrderNumber;association_foreignkey:Refer"`
}

// OrderDetail struct
type OrderDetail struct {
	OrderNumber string `gorm:"column:SalesOrderID"`
	ProductID   string `gorm:"column:ProductID"`
	OrderQty    string `gorm:"column:OrderQty"`
	UnitPrice   string `gorm:"column:UnitPrice"`
}

// ProductCategory struct
type ProductCategory struct {
	ID       uint32 `gorm:"column:ProductCategoryID;AUTO_INCREMENT;PRIMARY_KEY"`
	ParentID uint32 `gorm:"column:ParentProductCategoryID"`
	Name     string `gorm:"column:Name"`
}

// TableName alias for SalesOrderHeader
func (OrderHeader) TableName() string {
	return "SalesLT.SalesOrderHeader"
}

// TableName alias for SalesOrderDetail
func (OrderDetail) TableName() string {
	return "SalesLT.SalesOrderDetail"
}

// TableName alias for ProductCategory
func (ProductCategory) TableName() string {
	return "SalesLT.ProductCategory"
}

func usegorm() {

	connString := fmt.Sprintf("sqlserver://%s:%s@%s.database.windows.net:%d?database=%s",
		user, password, server, port, database)
	// Open database connection
	db, err := gorm.Open("mssql", connString)
	// Enable logging to see database commands generated
	db.LogMode(true)

	if err != nil {
		panic("failed to connect database")
	}

	var order OrderHeader
	// Simple database query
	db.Where(&OrderHeader{OrderNumber: "71797"}).First(&order)
	PrintAndLog(fmt.Sprintf("Order Number: %s", order.OrderNumber))

	// Array for storing projected resultset from JOIN operation
	var OrderAndDetails []struct {
		OrderNumber uint32     `gorm:"type:integer;column:SalesOrderID"`
		OrderDate   *time.Time `gorm:"type:datetime;column:OrderDate"`
		CustomerID  uint32     `gorm:"type:integer;column:CustomerID"`
		ProductID   uint32     `gorm:"type:integer;column:ProductID"`
		OrderQty    uint32     `gorm:"type:integer;column:OrderQty"`
		UnitPrice   float64    `gorm:"type:float;column:UnitPrice"`
	}
	// Define select list
	selectList := "SalesLT.SalesOrderHeader.SalesOrderID,SalesLT.SalesOrderHeader.OrderDate,SalesLT.SalesOrderHeader.CustomerID,SalesLT.SalesOrderDetail.ProductID,SalesLT.SalesOrderDetail.OrderQty,SalesLT.SalesOrderDetail.UnitPrice"
	// Query joining SalesOrderHeader and SalesOrderDetail for a specific order
	db.Table("SalesLT.SalesOrderHeader").Select(selectList).Joins("left join SalesLT.SalesOrderDetail on SalesLT.SalesOrderHeader.SalesOrderID = SalesLT.SalesOrderDetail.SalesOrderID").Where(&OrderHeader{OrderNumber: "71797"}).Scan(&OrderAndDetails)
	// Iterate on results
	for _, ord := range OrderAndDetails {
		PrintAndLog(fmt.Sprintf("SalesOrderNumber: %d - OrderDate: %s - CustomerID: %d - ProductID: %d - OrderQty: %d - UnitPrice: %.2f",
			ord.OrderNumber, ord.OrderDate, ord.CustomerID, ord.ProductID, ord.OrderQty, ord.UnitPrice))
	}

	// Create a new Product Category
	prod := ProductCategory{ParentID: 1, Name: uuid.New().String()}
	db.Create(&prod)
	PrintAndLog(fmt.Sprintf("New Product Category ID: %d", prod.ID))

	// Update Product Category created
	db.Model(&prod).Update("Name", uuid.New())
	PrintAndLog(fmt.Sprintf("New Product Category name: %s", prod.Name))

	// Delete Product Category
	db.Delete(&prod)
	PrintAndLog(fmt.Sprintf("Deleted Product Category ID: %d", prod.ID))

	// Cleanup resources
	db.Close()
}

// PrintAndLog method
func PrintAndLog(message string) {
	log.Println(message)
	fmt.Println(message)
}

func cleanup() {

	var err error

	// Create auth token from env variables (see here for details https://github.com/Azure/azure-sdk-for-go)
	authorizer, err := auth.NewAuthorizerFromEnvironment()
	if err == nil {
		// Create resource group SDK client
		groupsClient := resources.NewGroupsClient(subid)
		groupsClient.Authorizer = authorizer

		ctx := context.Background()

		// Delete resource group
		future, err := groupsClient.Delete(ctx, resgroup)
		if err != nil {
			PrintAndLog(err.Error())
		}
		// Wait for async operation completion
		err = future.WaitForCompletionRef(ctx, groupsClient.Client)
		if err != nil {
			PrintAndLog(fmt.Sprintf("Cannot delete resource group. Resource group delete future response: %v", err))
		}
	}
}
