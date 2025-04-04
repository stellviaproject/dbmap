package main

import (
	"encoding/json"
	"os"
)

type Config struct {
	SourceDB  DataBase
	DestinyDB DataBase
	Tables    []string
}

func (cfg *Config) Load(fileName string) error {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, cfg)
}

func (cfg *Config) Save(fileName string) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(fileName, data, os.ModeDevice)
}

func (cfg *Config) Example() {
	*cfg = Config{
		SourceDB: DataBase{
			Host:     "localhost",
			Port:     5432,
			User:     "postgres",
			Password: "postgres",
			Name:     "database1",
			SSLMode:  "disable",
		},
		DestinyDB: DataBase{
			Host:     "localhost",
			Port:     5432,
			User:     "postgres",
			Password: "postgres",
			Name:     "database2",
			SSLMode:  "disable",
		},
		Tables: []string{"table1", "table2"},
	}
}
