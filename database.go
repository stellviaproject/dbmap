package main

import (
	"database/sql"
	"fmt"
)

type DataBase struct {
	Host     string `json:"host"`
	Port     int    `json:"Port"`
	User     string `json:"user"`
	Password string `json:"Password"`
	Name     string `json:"Name"`
	SSLMode  string `json:"sslmode"`
}

func (db *DataBase) DSN() string {
	if db.SSLMode == "" {
		return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d", db.Host, db.User, db.Password, db.Name, db.Port)
	}
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=%s", db.Host, db.User, db.Password, db.Name, db.Port, db.SSLMode)
}

// Conectar a la base de datos utilizando database/sql
func (db *DataBase) Connect() (*sql.DB, error) {
	connectionString := db.DSN()
	sqlDB, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("error al abrir la conexión con la base de datos: %v", err)
	}

	// Verificar que la conexión sea válida
	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("error al verificar la conexión con la base de datos: %v", err)
	}

	return sqlDB, nil
}

func (db *DataBase) PgDSN() string {
	if db.SSLMode == "" {
		return fmt.Sprintf("host=%s user=%s password=%s dbname=postgres port=%d", db.Host, db.User, db.Password, db.Port)
	}
	return fmt.Sprintf("host=%s user=%s password=%s dbname=postgres port=%d sslmode=%s", db.Host, db.User, db.Password, db.Port, db.SSLMode)
}

func (db *DataBase) PgConnect() (*sql.DB, error) {
	// Construir el DSN para la base de datos `postgres`
	connectionString := db.PgDSN()
	sqlDB, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("error al abrir la conexión con la base de datos postgres: %v", err)
	}

	// Verificar que la conexión sea válida
	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("error al verificar la conexión con la base de datos postgres: %v", err)
	}

	return sqlDB, nil
}

func (db *DataBase) HasDB(pgDB *sql.DB) bool {
	var exists bool
	query := "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)"

	// Ejecutar consulta para verificar si la base de datos existe
	err := pgDB.QueryRow(query, db.Name).Scan(&exists)
	if err != nil {
		fmt.Printf("Error al verificar la base de datos: %v\n", err)
		return false
	}

	return exists
}

func (db *DataBase) CreateDB(pgDB *sql.DB) error {
	// Verificar si la base de datos ya existe
	if db.HasDB(pgDB) {
		fmt.Printf("La base de datos '%s' ya existe.\n", db.Name)
		return nil
	}

	// Crear la base de datos
	query := fmt.Sprintf("CREATE DATABASE %s", db.Name)
	_, err := pgDB.Exec(query)
	if err != nil {
		return fmt.Errorf("error al crear la base de datos '%s': %v", db.Name, err)
	}

	fmt.Printf("La base de datos '%s' se creó exitosamente.\n", db.Name)
	return nil
}
