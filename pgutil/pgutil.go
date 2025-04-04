package pgutil

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // Importa el driver para PostgreSQL
)

// GetCreateTableQuery obtiene la definición CREATE TABLE de una tabla específica usando PostgreSQL.
// GetCreateTableQuery obtiene la definición CREATE TABLE de una tabla específica usando PostgreSQL.
func GetCreateTableQuery(db *sql.DB, schemaName, tableName string) (string, error) {
	// Construye la consulta para obtener la definición CREATE TABLE
	query := `
        SELECT 
            'CREATE TABLE ' || quote_ident(table_schema) || '.' || quote_ident(table_name) || ' (' ||
            string_agg(column_def, ', ' ORDER BY ordinal_position) || ');' AS table_definition
        FROM (
            SELECT 
                table_schema,
                table_name,
                quote_ident(column_name) || ' ' || 
                udt_name || 
                CASE 
                    WHEN character_maximum_length IS NOT NULL 
                    THEN '(' || character_maximum_length || ')' 
                    ELSE '' 
                END || 
                CASE 
                    WHEN is_nullable = 'NO' THEN ' NOT NULL' 
                    ELSE '' 
                END || 
                CASE 
                    WHEN column_default IS NOT NULL 
                    THEN ' DEFAULT ' || column_default 
                    ELSE '' 
                END AS column_def,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
        ) AS table_columns
        GROUP BY table_schema, table_name;`

	var createTableSQL string
	err := db.QueryRow(query, schemaName, tableName).Scan(&createTableSQL)
	if err != nil {
		return "", fmt.Errorf("error obteniendo la definición CREATE TABLE para la tabla %s.%s: %v", schemaName, tableName, err)
	}
	return createTableSQL, nil
}
