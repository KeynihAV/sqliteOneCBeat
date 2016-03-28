package beater

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	//"strconv"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
	_ "github.com/mattn/go-sqlite3"
)

type Sqliteonecbeat struct {
	SbConfig   ConfigSettings
	events     publisher.Client
	path       string
	path_since string
	filesState map[string]int64
	batch      int
	done       chan struct{}
}

func New() *Sqliteonecbeat {
	return &Sqliteonecbeat{}
}

func (sb *Sqliteonecbeat) Config(b *beat.Beat) error {
	err := cfgfile.Read(&sb.SbConfig, "")
	if err != nil {
		logp.Err("Error reading configuration file: %v", err)
		return err
	}
	if sb.SbConfig.Input.Path != nil {
		sb.path = *sb.SbConfig.Input.Path
	} else {
		sb.path = ""
	}

	if sb.SbConfig.Input.Path_since != nil {
		sb.path_since = *sb.SbConfig.Input.Path_since
	} else {
		sb.path_since = ""
	}

	if sb.SbConfig.Input.Batch != nil {
		sb.batch = *sb.SbConfig.Input.Batch
	} else {
		sb.batch = 100
	}

	logp.Debug("sqliteonecbeat", "Init sqliteonecbeat")

	return nil
}

func (sb *Sqliteonecbeat) Setup(b *beat.Beat) error {
	sb.events = b.Events
	sb.done = make(chan struct{})
	return nil
}

func (sb *Sqliteonecbeat) Run(b *beat.Beat) error {
	var err error
	sb.filesState = make(map[string]int64)
	sb.GetLastPlace()
	db, err := sql.Open("sqlite3", sb.path)
	checkErr(err)

	var rows = ReadNRowsFromSqlite(db, sb.filesState[sb.path], sb.batch)

	for rows.Next() {
		var mytimestamp int64
		var connectID int
		var session int
		var transactionStatus int
		var transactionID int
		var severity int
		var UserName string
		var Client string
		var transactionDate int64
		var ApplicationName string
		var Event string
		var Comment string
		var metaData sql.NullString
		var dataType sql.NullString
		var data sql.NullString
		var rowID int
		var dataPresentation sql.NullString
		var Server sql.NullString
		var PrimaryPort sql.NullInt64
		var SecondaryPort sql.NullInt64

		err = rows.Scan(&mytimestamp, &connectID, &session, &transactionStatus, &transactionID, &severity, &UserName, &Client, &transactionDate,
			&ApplicationName, &Event, &Comment, &metaData, &dataType, &data, &rowID, &dataPresentation, &Server, &PrimaryPort, &SecondaryPort)
		checkErr(err)

		OneSLog := common.MapStr{
			"connectID":         connectID,
			"session":           session,
			"transactionStatus": transactionStatus,
			"transactionID":     transactionID,
			"severity":          severity,
			"UserName":          UserName,
			"Client":            Client,
			"transactionDate":   transactionDate,
			"ApplicationName":   ApplicationName,
			"Event":             Event,
			"Comment":           Comment,
			"metaData":          metaData,
			"dataType":          dataType,
			"data":              data,
			"rowID":             rowID,
			"dataPresentation":  dataPresentation,
			"Server":            Server,
			"PrimaryPort":       PrimaryPort,
			"SecondaryPort":     SecondaryPort,
		}
		//OneSLog := common.MapStr{
		//		"message": "message",
		//}
		event := common.MapStr{
			"@timestamp": "2015-08-06T20:20:34.089Z",
			"type":       "OneClog",
			"OneSLog":    OneSLog,
		}
		fmt.Println("Дата: (%s)", event)
		sb.events.PublishEvent(event)
		sb.filesState[sb.path] = mytimestamp
		break
	}

	sb.SaveLastPlace()

	db.Close()
	return err
}

// Cleanup removes any temporary files, data, or other items that were created by the Beat.
func (sb *Sqliteonecbeat) Cleanup(b *beat.Beat) error {
	return nil
}

// Stop is called on exit to stop the crawling, spooling and registration processes.
func (sb *Sqliteonecbeat) Stop() {

	logp.Info("Stopping sqliteonecbeat")

	close(sb.done)
}

func (sb *Sqliteonecbeat) GetLastPlace() {
	if existing, e := os.Open(sb.path_since); e == nil {
		defer existing.Close()
		decoder := json.NewDecoder(existing)
		decoder.Decode(&sb.filesState)
	}
}

func (sb *Sqliteonecbeat) SaveLastPlace() error {
	file, e := os.Create(sb.path_since + ".new")
	if e != nil {
		logp.Err("Failed to create tempfile (%s) for writing: %s", sb.path_since, e)
		return e
	}
	encoder := json.NewEncoder(file)
	encoder.Encode(sb.filesState)

	// Directly close file because of windows
	file.Close()

	return SafeFileRotate(sb.path_since, sb.path_since+".new")
}

func ReadNRowsFromSqlite(db *sql.DB, lastPlace int64, batch int) *sql.Rows {
	rows, err := db.Query(`SELECT
	(EventLog.[date] - 62135596800 * 10000) / 10000 as mytimestamp	
     ,EventLog.[connectID] as connectID
	  , EventLog.[session] as session
		, EventLog.[transactionStatus] as transactionStatus		
		, EventLog.[transactionID] as transactionID
		, EventLog.[severity] as severity		
		, UserCodes.[name] as UserName
		, ComputerCodes.[name] as Client				
		, CASE WHEN EventLog.[transactionDate] = 0 THEN 0 ELSE (EventLog.[transactionDate] - 62135596800 * 10000) / 10000 END as transactionDate
		, AppCodes.[name] as ApplicationName
		, EventCodes.[name] as Event
		, EventLog.[comment] as Comment
		, metadataCodes.name as metaData
		, EventLog.[dataType] as dataType
		, EventLog.[data] as data
		, EventLog.[date] as rowID
		, EventLog.[dataPresentation] as dataPresentation
		, WorkServerCodes.[name] as Server
		, primaryPortCodes.[name] as PrimaryPort
		, secondaryPortCodes.[name] as SecondaryPort
		FROM [EventLog] as EventLog
		LEFT JOIN [UserCodes] as UserCodes
		ON 
		EventLog.userCode = UserCodes.[code]
		LEFT JOIN [ComputerCodes] as ComputerCodes
		ON 
		EventLog.ComputerCode = ComputerCodes.[code]
		LEFT JOIN [AppCodes] as AppCodes
		ON 
		EventLog.appCode = AppCodes.[code]
		LEFT JOIN [EventCodes] as EventCodes
		ON 
		EventLog.eventCode = EventCodes.[code]
		LEFT JOIN [MetadataCodes] as metadataCodes
		ON 
		EventLog.metadataCodes = metadataCodes.[code]
		LEFT JOIN [WorkServerCodes] as WorkServerCodes
		ON 
		EventLog.WorkServerCode = WorkServerCodes.[code]
		LEFT JOIN [PrimaryPortCodes] as primaryPortCodes
		ON 
		EventLog.primaryPortCode = primaryPortCodes.[code]
		LEFT JOIN [SecondaryPortCodes] as secondaryPortCodes
		ON 
		EventLog.secondaryPortCode = secondaryPortCodes.[code]				
		WHERE (EventLog.[date] > ?) ORDER BY EventLog.[date] LIMIT ?`, lastPlace, batch)

	checkErr(err)
	return rows
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func SafeFileRotate(path, tempfile string) error {
	old := path + ".old"
	var e error

	// In Windows, one cannot rename a file if the destination already exists, at least
	// not with using the os.Rename function that Golang offers.
	// This tries to move the existing file into an old file first and only do the
	// move after that.
	if e = os.Remove(old); e != nil {
		logp.Debug("filecompare", "delete old: %v", e)
		// ignore error in case old doesn't exit yet
	}
	if e = os.Rename(path, old); e != nil {
		logp.Debug("filecompare", "rotate to old: %v", e)
		// ignore error in case path doesn't exist
	}

	if e = os.Rename(tempfile, path); e != nil {
		logp.Err("rotate: %v", e)
		return e
	}
	return nil
}
