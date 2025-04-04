package pgsync

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/stellviaproject/dbmap/pgutil"
)

func SyncTables(src, dst *sql.DB, tables []string, info *pgutil.DataBaseInfo) error {
	//Mapear las tablas por sus esquemas
	tableMap := map[string]*pgutil.TableInfo{}
	for _, table := range info.Tables {
		tableMap[fmt.Sprintf("%s.%s", table.Scheme, table.Name)] = table
	}
	//Mapear las tablas de la consulta y preparar una lista
	localMap := map[string]*pgutil.TableInfo{}
	for _, tableName := range tables {
		table := tableMap[tableName]
		localMap[tableName] = table
	}
	//Tablas sin dependencias de otras en la consulta
	withoutDeps := []*pgutil.TableInfo{}
	//Tablas con referencias a otras en la consulta
	references := map[*pgutil.TableInfo][]*pgutil.TableInfo{}
	queue := []*pgutil.TableInfo{}
	//Llenar las tablas con referencias y las tablas sin dependencias
	for _, table := range localMap {
		refs := GetReferences(table, localMap) //Obtener referencias locales
		if len(refs) > 0 {
			//Agregar a la lista de referencias
			references[table] = append(references[table], refs...)
			queue = append(queue, table)
		} else {
			//Agregar a la lista de sin dependencias
			withoutDeps = append(withoutDeps, table)
		}
	}
	for _, table := range withoutDeps {
		if err := CheckConstraints(table, tableMap); err != nil {
			log.Fatalln(err)
		}
		if err := SyncTable(src, dst, table); err != nil {
			return err
		}
	}
	syncedTables := map[string]*pgutil.TableInfo{}
	processed := map[string]*pgutil.TableInfo{}
	for len(queue) > 0 {
		current := queue[0]
		log.Printf("trying to sync table %s.%s", current.Scheme, current.Name)
		if err := CheckConstraints(current, tableMap); err != nil {
			log.Fatalln(err)
		}
		if CanSync(current, syncedTables) {
			syncedTables[fmt.Sprintf("%s.%s", current.Scheme, current.Name)] = current
			log.Printf("sync table %s.%s", current.Scheme, current.Name)
			if err := SyncTable(src, dst, current); err != nil {
				return err
			}
		} else {
			if _, ok := processed[current.Name]; ok {
				return fmt.Errorf("infinite loop detected in table dependecies")
			}
			queue = append(queue, current)
			log.Printf("table will sync later %s.%s", current.Scheme, current.Name)
			processed[current.Name] = current
		}
		queue = queue[1:]
	}
	return nil
}

func GetReferences(table *pgutil.TableInfo, tableMap map[string]*pgutil.TableInfo) []*pgutil.TableInfo {
	references := []*pgutil.TableInfo{}
	for _, fk := range table.Constraints {
		if ref, ok := tableMap[fk.ReferencedTable]; ok {
			references = append(references, ref)
		}
	}
	return references
}

type SyncTableData struct {
	Table *pgutil.TableInfo
	Times int
}

func CanSync(table *pgutil.TableInfo, syncedTables map[string]*pgutil.TableInfo) bool {
	for _, constraint := range table.Constraints {
		column := table.GetColumn(constraint.Local)
		_, ok := syncedTables[constraint.ReferencedTable]
		if column.IsNullable {
			return true
		}
		if !ok {
			return false
		}
	}
	return true
}

func CheckConstraints(table *pgutil.TableInfo, tablesMap map[string]*pgutil.TableInfo) error {
	for _, constraint := range table.Constraints {
		//Chequear si puede ser null
		for _, column := range table.Columns {
			if column.Name == constraint.Local && !column.IsNullable {
				if _, ok := tablesMap[constraint.ReferencedTable]; !ok {
					return fmt.Errorf("la columna %s en la tabla %s es una clave foranea y no puede ser nula, incluya la tabla %s como objetivo de copia para solucionar el error", constraint.Local, table.Name, constraint.ReferencedTable)
				}
			}
		}
	}
	return nil
}

func SyncTable(src, dst *sql.DB, table *pgutil.TableInfo) error {
	const batchSize = 1000 // Número de filas por lote
	var offset int = 0     // Inicialización del offset para la consulta
	totalRows := 0         // Variable para realizar seguimiento de las filas copiadas

	// Obtener el número total de filas en la tabla fuente
	var rowCount int
	err := src.QueryRow(table.CountQuery()).Scan(&rowCount)
	if err != nil {
		return fmt.Errorf("error fetching row count: %w", err)
	}

	fmt.Printf("Total rows to process in table %s.%s: %d\n", table.Scheme, table.Name, rowCount)

	// Copiar datos por partes
	for offset < rowCount {
		// Obtener el siguiente lote de datos desde la base de datos fuente
		rows, err := src.Query(table.SelectWithBatchQuery(batchSize, offset))
		if err != nil {
			return fmt.Errorf("error fetching rows: %w", err)
		}
		defer rows.Close()

		// Preparar inserciones y actualizaciones en la base de datos destino
		insertQuery := table.InsertQuery()
		updateQuery := table.UpdateQuery()
		tx, err := dst.Begin() // Inicia una transacción para mejorar el rendimiento
		if err != nil {
			return fmt.Errorf("error beginning transaction: %w", err)
		}

		insertStmt, err := tx.Prepare(insertQuery)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error preparing insert statement: %w", err)
		}
		defer insertStmt.Close()

		updateStmt, err := tx.Prepare(updateQuery)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error preparing update statement: %w", err)
		}
		defer updateStmt.Close()

		// Copiar filas al destino
		for rows.Next() {
			// Lee cada fila y los valores de las columnas
			columns, err := rows.Columns()
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("error fetching columns: %w", err)
			}

			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				tx.Rollback()
				return fmt.Errorf("error scanning row: %w", err)
			}

			// Verificar las claves foráneas y actualizar valores inexistentes a NULL
			for _, fk := range table.Constraints {
				refExists := checkFKExists(dst, fk.ReferencedTable, fk.Referenced, values[findColumnIndex(columns, fk.Local)])
				if !refExists {
					values[findColumnIndex(columns, fk.Local)] = nil // Establecer a NULL si no existe
				}
			}

			// Verificar si el registro ya existe en la base de datos destino
			existsQuery := table.SelectExistsQuery()
			existsStmt, err := dst.Prepare(existsQuery)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("error preparing exists statement: %w", err)
			}
			defer existsStmt.Close()

			existsValues := getPrimaryKeyValues(table.Columns, values) // Extraer valores de claves primarias
			var exists bool
			if err := existsStmt.QueryRow(existsValues...).Scan(&exists); err != nil {
				tx.Rollback()
				return fmt.Errorf("error checking record existence: %w", err)
			}

			// Si existe, actualiza; de lo contrario, inserta
			if exists {
				updateValues := append(values, existsValues...) // Combinar valores para la cláusula WHERE
				if _, err := updateStmt.Exec(updateValues...); err != nil {
					tx.Rollback()
					return fmt.Errorf("error updating record: %w", err)
				}
			} else {
				if _, err := insertStmt.Exec(values...); err != nil {
					tx.Rollback()
					return fmt.Errorf("error inserting record: %w", err)
				}
			}

			totalRows++
		}

		// Confirma la transacción
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("error committing transaction: %w", err)
		}

		fmt.Printf("Processed %d rows from %s.%s\n", totalRows, table.Scheme, table.Name)

		// Incrementa el offset para procesar el siguiente lote
		offset += batchSize
	}

	fmt.Printf("Completed processing table %s.%s\n", table.Scheme, table.Name)
	return nil
}

// Obtiene los valores de las claves primarias desde los valores de la fila
func getPrimaryKeyValues(columns []pgutil.ColumnInfo, values []interface{}) []interface{} {
	var pkValues []interface{}
	for i, column := range columns {
		if column.IsPrimaryKey {
			pkValues = append(pkValues, values[i])
		}
	}
	return pkValues
}

// Verifica si una clave foránea existe en la base de datos de destino
func checkFKExists(db *sql.DB, referencedTable string, referencedColumn string, value interface{}) bool {
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE %s = $1)", referencedTable, referencedColumn)
	var exists bool
	err := db.QueryRow(query, value).Scan(&exists)
	if err != nil {
		fmt.Printf("Error checking foreign key existence: %v\n", err)
		return false
	}
	return exists
}

// Encuentra el índice de una columna en el arreglo de columnas
func findColumnIndex(columns []string, columnName string) int {
	for i, col := range columns {
		if col == columnName {
			return i
		}
	}
	return -1
}
