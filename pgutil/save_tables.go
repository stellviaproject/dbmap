package pgutil

import (
	"database/sql"
	"fmt"
	"strings"
)

func MakeSave(db *sql.DB) error {
	// Obtener los nombres de todas las tablas en todos los esquemas, excluyendo los del sistema
	rows, err := db.Query(`
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_type = 'BASE TABLE';
    `)
	if err != nil {
		return fmt.Errorf("error al obtener los nombres de las tablas: %v", err)
	}
	defer rows.Close()

	// Iterar sobre cada tabla y renombrarla
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return fmt.Errorf("error al escanear el nombre de la tabla: %v", err)
		}

		//Si la tabla no tiene sufijo _save
		if !strings.HasSuffix(tableName, "_save") {
			// Crear el nuevo nombre de la tabla
			newTableName := fmt.Sprintf("%s_save", tableName)

			// Renombrar la tabla
			_, err := db.Exec(fmt.Sprintf(`ALTER TABLE "%s"."%s" RENAME TO "%s";`, schemaName, tableName, newTableName))
			if err != nil {
				return fmt.Errorf("error al renombrar la tabla %s.%s a %s: %v", schemaName, tableName, newTableName, err)
			}

			fmt.Printf("Tabla renombrada: %s.%s -> %s\n", schemaName, tableName, newTableName)
		}
	}

	// Verificar errores en la iteración de las filas
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error durante la iteración de las tablas: %v", err)
	}

	return nil
}

func UnMakeSave(db *sql.DB) error {
	// Obtener los nombres de todas las tablas en la base de datos que terminan con _save
	rows, err := db.Query(`
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_type = 'BASE TABLE'
        AND table_name LIKE '%_save';
    `)
	if err != nil {
		return fmt.Errorf("error al obtener los nombres de las tablas: %v", err)
	}
	defer rows.Close()

	// Iterar sobre cada tabla y renombrarla quitando el sufijo _save
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return fmt.Errorf("error al escanear el nombre de la tabla: %v", err)
		}

		// Verificar y quitar el sufijo _save del nombre de la tabla
		if strings.HasSuffix(tableName, "_save") {
			newTableName := strings.TrimSuffix(tableName, "_save")

			// Renombrar la tabla
			_, err := db.Exec(fmt.Sprintf(`ALTER TABLE "%s"."%s" RENAME TO "%s";`, schemaName, tableName, newTableName))
			if err != nil {
				return fmt.Errorf("error al renombrar la tabla %s.%s a %s: %v", schemaName, tableName, newTableName, err)
			}

			fmt.Printf("Tabla renombrada: %s.%s -> %s\n", schemaName, tableName, newTableName)
		}
	}

	// Verificar errores en la iteración de las filas
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error durante la iteración de las tablas: %v", err)
	}

	return nil
}

func GetSaveTables(db *sql.DB) ([]string, error) {
	// Consulta para obtener los nombres de las tablas con sufijo "_save"
	query := `
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_type = 'BASE TABLE'
        AND table_name LIKE '%_save';
    `

	// Ejecutar la consulta
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error ejecutando consulta: %v", err)
	}
	defer rows.Close()

	// Slice para almacenar los nombres de las tablas
	var tables []string

	// Iterar sobre los resultados
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("error escaneando resultados: %v", err)
		}

		// Formatear el nombre completo como <scheme>.<nombre>
		fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
		tables = append(tables, fullTableName)
	}

	// Verificar errores en la iteración
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterando sobre los resultados: %v", err)
	}

	return tables, nil
}

func DropNotSave(db *sql.DB) error {
	// Consulta para obtener los nombres de las tablas sin el sufijo "_save"
	query := `
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%_save';
    `

	// Ejecutar la consulta
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error al ejecutar la consulta: %v", err)
	}
	defer rows.Close()

	// Slice para almacenar las tablas que se van a eliminar
	var tablesToDelete []string

	// Iterar sobre los resultados
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return fmt.Errorf("error al escanear los resultados: %v", err)
		}

		// Formatear el nombre completo como <scheme>.<table>
		fullTableName := fmt.Sprintf(`"%s"."%s"`, schemaName, tableName)
		tablesToDelete = append(tablesToDelete, fullTableName)
	}

	// Verificar errores durante la iteración
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error durante la iteración de las tablas: %v", err)
	}

	// Eliminar las tablas
	for _, table := range tablesToDelete {
		deleteQuery := fmt.Sprintf("DROP TABLE %s CASCADE;", table)
		fmt.Printf("Eliminando tabla: %s\n", table)
		if _, err := db.Exec(deleteQuery); err != nil {
			return fmt.Errorf("error eliminando la tabla %s: %v", table, err)
		}
	}

	fmt.Println("Todas las tablas sin el sufijo '_save' han sido eliminadas.")
	return nil
}

func CleanTables(db *sql.DB) error {
	// Consulta para obtener los nombres de las tablas sin el sufijo "_save"
	query := `
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE '%_save';
    `

	// Ejecutar la consulta
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error al ejecutar la consulta: %v", err)
	}
	defer rows.Close()

	// Iterar sobre las tablas y limpiar los datos
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return fmt.Errorf("error al escanear los resultados: %v", err)
		}

		// Construir la consulta DELETE para limpiar la tabla
		fullTableName := fmt.Sprintf(`"%s"."%s"`, schemaName, tableName)
		deleteQuery := fmt.Sprintf("DELETE FROM %s;", fullTableName)

		fmt.Printf("Limpiando datos de la tabla: %s\n", fullTableName)
		if _, err := db.Exec(deleteQuery); err != nil {
			return fmt.Errorf("error limpiando la tabla %s: %v", fullTableName, err)
		}
	}

	// Verificar errores durante la iteración
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error durante la iteración de las tablas: %v", err)
	}

	fmt.Println("Todas las tablas sin sufijo '_save' han sido limpiadas.")
	return nil
}
