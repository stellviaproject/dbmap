package pgutil

import (
	"testing"

	"github.com/stellviaproject/dbmap/database"
)

func TestGetDataBase(t *testing.T) {
	cfg := database.DataBase{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "postgres",
		Name:     "sigies",
		SSLMode:  "disable",
	}
	db, err := cfg.Connect()
	if err != nil {
		t.Log("fallo al conectar con postgres o con la base de datos")
		t.FailNow()
	}
	info, err := GetDataBaseInfo(db)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	t.Log(info.String())
}

func TestGetDefinition(t *testing.T) {
	cfg := database.DataBase{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "postgres",
		Name:     "sigies",
		SSLMode:  "disable",
	}
	db, err := cfg.Connect()
	if err != nil {
		t.Log("fallo al conectar con postgres o con la base de datos")
		t.FailNow()
	}
	query, err := GetCreateTableQuery(db, "pkt_organization", "tb_student")
	if err != nil {
		t.Log("fallo al obtener la query de la tabla")
		t.FailNow()
	}
	t.Log(query)
}
