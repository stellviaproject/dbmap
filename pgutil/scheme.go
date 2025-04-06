package pgutil

import (
	"database/sql"
	"fmt"
	"strings"
)

type DataBaseInfo struct {
	Tables []*TableInfo
}

// Método String() para DataBase
func (db *DataBaseInfo) String() string {
	var sb strings.Builder
	sb.WriteString("Database:\n")
	for _, table := range db.Tables {
		sb.WriteString(table.String())
		sb.WriteString("\n")
	}
	return sb.String()
}

type TableInfo struct {
	Scheme             string       //Esquema de la tabla
	Name               string       //Nombre de la tabla
	Columns            []ColumnInfo //Columnas de la tabla
	Constraints        []FKConstraintInfo
	selectQuery        string
	insertQuery        string
	selectExistsQuery  string
	updateQuery        string
	selectBatchColumns string
}

func (tb *TableInfo) TableName() string {
	return fmt.Sprintf("%s.%s", tb.Scheme, tb.Name)
}

func (tb *TableInfo) GetColumn(columnName string) *ColumnInfo {
	for _, column := range tb.Columns {
		if column.Name == columnName {
			return &column
		}
	}
	return nil
}

func (tb *TableInfo) CountQuery() string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", tb.Scheme, tb.Name)
}

func (tb *TableInfo) SelectExistsQuery() string {
	if tb.selectExistsQuery == "" {
		// Generar las condiciones para el WHERE basándose en las columnas de clave primaria
		whereClauses := []string{}
		for _, column := range tb.Columns {
			if column.IsPrimaryKey {
				whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", column.Name, len(whereClauses)+1))
			}
		}
		whereClause := strings.Join(whereClauses, " AND ")

		// Construir la consulta SELECT EXISTS
		tb.selectExistsQuery = fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s.%s WHERE %s)", tb.Scheme, tb.Name, whereClause)
	}
	return tb.selectExistsQuery
}

// Retorna una query SELECT column1, column2, colum3,... FROM table
func (tb *TableInfo) SelectQuery() string {
	if tb.selectQuery == "" {
		columnNames := []string{}
		for _, column := range tb.Columns {
			columnNames = append(columnNames, column.Name)
		}
		columns := strings.Join(columnNames, ", ")
		tb.selectQuery = fmt.Sprintf("SELECT %s FROM %s.%s", columns, tb.Scheme, tb.Name)
	}
	return tb.selectQuery
}

// Retorna una query SELECT column1, column2, column3,... FROM table con soporte para batch (LIMIT y OFFSET)
func (tb *TableInfo) SelectWithBatchQuery(limit, offset int) string {
	if tb.selectBatchColumns == "" {
		columnNames := []string{}
		for _, column := range tb.Columns {
			columnNames = append(columnNames, column.Name)
		}
		tb.selectBatchColumns = strings.Join(columnNames, ", ")
	}
	return fmt.Sprintf("SELECT %s FROM %s.%s LIMIT %d OFFSET %d", tb.selectBatchColumns, tb.Scheme, tb.Name, limit, offset)
}

// Retorna INSERT INTO %s.%s VALUES ($1,$2,...)
// Los values con la misma cantidad que el numero de columnas
func (tb *TableInfo) InsertQuery() string {
	if tb.insertQuery == "" {
		valuePlaceholders := []string{}
		for i := range tb.Columns {
			valuePlaceholders = append(valuePlaceholders, fmt.Sprintf("$%d", i+1))
		}
		values := strings.Join(valuePlaceholders, ", ")
		tb.insertQuery = fmt.Sprintf("INSERT INTO %s.%s VALUES (%s)", tb.Scheme, tb.Name, values)
	}
	return tb.insertQuery
}

// Retorna una query update con el mismo orden que los nombres de los campos en la consulta SELECT
func (tb *TableInfo) UpdateQuery() string {
	if tb.updateQuery == "" {
		// Generar las asignaciones de columnas para el SET
		setClauses := []string{}
		for i, column := range tb.Columns {
			setClauses = append(setClauses, fmt.Sprintf("%s = $%d", column.Name, i+1))
		}
		setClause := strings.Join(setClauses, ", ")

		// Generar las condiciones del WHERE usando las claves primarias
		whereClauses := []string{}
		offset := len(tb.Columns) + 1 // Los placeholders del WHERE comienzan después de los SET
		for _, column := range tb.Columns {
			if column.IsPrimaryKey {
				whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", column.Name, offset))
				offset++
			}
		}
		whereClause := strings.Join(whereClauses, " AND ")

		// Construir la query completa
		tb.updateQuery = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", tb.Scheme, tb.Name, setClause, whereClause)
	}
	return tb.updateQuery
}

func (tb *TableInfo) UpSertQuery(destinyTable string) string {
	// Obtener los nombres de las columnas
	columnNames := []string{}
	for _, column := range tb.Columns {
		columnNames = append(columnNames, column.Name)
	}

	// Crear la lista de columnas
	columns := strings.Join(columnNames, ", ")

	// Determinar la clave primaria y construir la cláusula ON CONFLICT
	primaryKeys := []string{}
	for _, column := range tb.Columns {
		if column.IsPrimaryKey {
			primaryKeys = append(primaryKeys, column.Name)
		}
	}
	if len(primaryKeys) == 0 {
		return fmt.Sprintf("Error: La tabla %s.%s no tiene claves primarias definidas.", tb.Scheme, tb.Name)
	}
	onConflictClause := fmt.Sprintf("ON CONFLICT (%s)", strings.Join(primaryKeys, ", "))

	// Construir la cláusula DO UPDATE SET
	setClauses := []string{}
	for _, column := range tb.Columns {
		setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", column.Name, column.Name))
	}
	setClause := strings.Join(setClauses, ", ")

	// Generar la subconsulta SELECT desde la tabla fuente
	subQuery := fmt.Sprintf("SELECT %s FROM %s", columns, tb.TableName())

	// Generar la consulta completa
	query := fmt.Sprintf(
		"INSERT INTO %s (%s) %s %s DO UPDATE SET %s;",
		destinyTable, columns, subQuery, onConflictClause, setClause,
	)

	return query
}

// Método String() para TableInfo
func (tb *TableInfo) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Table: %s.%s\n", tb.Scheme, tb.Name))
	sb.WriteString("Columns:\n")
	for _, column := range tb.Columns {
		sb.WriteString(fmt.Sprintf("  %s\n", column.String()))
	}
	sb.WriteString("Constraints:\n")
	for _, constraint := range tb.Constraints {
		sb.WriteString(fmt.Sprintf("  %s\n", constraint.String()))
	}
	return sb.String()
}

type ColumnInfo struct {
	Name            string //Nombre de la columna
	DataType        string //Tipo de dato de la columna
	LengthPrecision int    //Longitud o precision del tipo de dato de la columna
	IsPrimaryKey    bool   //Si la columna es clave primaria
	IsNullable      bool   //Si la columna acepta valores nulos
}

// Método String() para ColumnInfo
func (ci *ColumnInfo) String() string {
	primaryKeyText := ""
	if ci.IsPrimaryKey {
		primaryKeyText = " (Primary Key)"
	}
	notNullText := ""
	if ci.IsNullable {
		notNullText = "Not Null"
	}
	return fmt.Sprintf("Column: %s, Type: %s(%d)%s %s", ci.Name, ci.DataType, ci.LengthPrecision, primaryKeyText, notNullText)
}

type FKConstraintInfo struct {
	Name                 string //Nombre de la restriccion
	UniqueConstraintName string //Nombre unico de la restriccion
	Local                string //Nombre de la columna en la tabla local
	Referenced           string //Nombre de la columna referenciada en otra tabla
	ReferencedTable      string //Nombre de la tabla referenciada en la forma scheme.table
	OnUpdate             Action //Accion al actualizar
	OnDelete             Action //Accion al eliminar
}

// Método String() para ConstraintInfo
func (ci *FKConstraintInfo) String() string {
	return fmt.Sprintf(
		"Constraint: %s, Local Column: %s, Referenced Column: %s, Referenced Table: %s, On Update: %s, On Delete: %s",
		ci.Name, ci.Local, ci.Referenced, ci.ReferencedTable, ci.OnUpdate, ci.OnDelete,
	)
}

type Action string //Acciones a realizar a actualizar o eliminar

const (
	NO_ACTION Action = "NO ACTION"
	CASCADE   Action = "CASCADE"
	SET_NULL  Action = "SET NULL"
	RESTRICT  Action = "RESTRICT"
)

// Recibe por parámetro la base de datos (postgres) y el nombre de la tabla en la forma scheme.table y devuelve la información de la tabla
func GetTableInfo(db *sql.DB, tableName string) (*TableInfo, error) {
	var tableInfo TableInfo
	query := `
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema || '.' || table_name = $1
    `
	err := db.QueryRow(query, tableName).Scan(&tableInfo.Scheme, &tableInfo.Name)
	if err != nil {
		return nil, fmt.Errorf("error fetching table info: %w", err)
	}

	// Obtener columnas
	columnsQuery := `
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema || '.' || table_name = $1
    `
	rows, err := db.Query(columnsQuery, tableName)
	if err != nil {
		return nil, fmt.Errorf("error fetching columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var column ColumnInfo
		var lengthPrecision sql.NullInt64
		err := rows.Scan(&column.Name, &column.DataType, &lengthPrecision)
		if err != nil {
			return nil, fmt.Errorf("error scanning columns: %w", err)
		}
		column.LengthPrecision = int(lengthPrecision.Int64)
		GetIsPrimaryKey(db, tableName, &column)
		tableInfo.Columns = append(tableInfo.Columns, column)
	}

	// Obtener restricciones FK
	constraintsQuery := `
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema || '.' || table_name = $1
		AND constraint_name LIKE 'fk_%'
    `
	rows, err = db.Query(constraintsQuery, tableName)
	if err != nil {
		return nil, fmt.Errorf("error fetching constraints: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var constraintName string
		err := rows.Scan(&constraintName)
		if err != nil {
			return nil, fmt.Errorf("error scanning constraints: %w", err)
		}
		constraintInfo, err := GetFKConstraintInfo(db, tableName, constraintName)
		if err != nil {
			return nil, fmt.Errorf("error fetching constraint info: %w", err)
		}
		tableInfo.Constraints = append(tableInfo.Constraints, *constraintInfo)
	}

	return &tableInfo, nil
}

// Recibe por parámetro la base de datos (postgres) y el nombre de la tabla en la forma scheme.table, el nombre de la columna y devuelve la información de la columna
func GetColumnInfo(db *sql.DB, tableName string, columnName string) (*ColumnInfo, error) {
	var column ColumnInfo
	query := `
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema || '.' || table_name = $1
        AND column_name = $2
    `
	var lengthPrecision sql.NullInt64
	err := db.QueryRow(query, tableName, columnName).Scan(&column.Name, &column.DataType, &lengthPrecision)
	if err != nil {
		return nil, fmt.Errorf("error fetching column info: %w", err)
	}
	column.LengthPrecision = int(lengthPrecision.Int64)
	GetIsPrimaryKey(db, tableName, &column)
	return &column, nil
}

// Recibe por parámetro la base de datos (postgres) y el nombre de la tabla en la forma scheme.table, el nombre de la restricción y devuelve la información de la restricción
func GetFKConstraintInfo(db *sql.DB, tableName string, constraintName string) (*FKConstraintInfo, error) {
	var constraint FKConstraintInfo
	query := `
        SELECT 
            kcu.constraint_name AS constraint_name,
            kcu.column_name AS local_column,
            rc.unique_constraint_name AS unique_constraint_name,
            ccu.column_name AS referenced_column,
            ccu.table_schema || '.' || ccu.table_name AS referenced_table,
            rc.update_rule AS update_rule,
            rc.delete_rule AS delete_rule
        FROM 
            information_schema.key_column_usage AS kcu
        JOIN 
            information_schema.referential_constraints AS rc
            ON kcu.constraint_name = rc.constraint_name
        JOIN 
            information_schema.constraint_column_usage AS ccu
            ON rc.unique_constraint_name = ccu.constraint_name
        WHERE 
            kcu.table_schema || '.' || kcu.table_name = $1
            AND kcu.constraint_name = $2;
    `
	err := db.QueryRow(query, tableName, constraintName).Scan(
		&constraint.Name,
		&constraint.Local,
		&constraint.UniqueConstraintName,
		&constraint.Referenced,
		&constraint.ReferencedTable,
		&constraint.OnUpdate,
		&constraint.OnDelete,
	)
	if err != nil {
		return nil, fmt.Errorf("error fetching constraint info: %w", err)
	}
	return &constraint, nil
}

// Establece en la columna si es o no una clave primaria
func GetIsPrimaryKey(db *sql.DB, tableName string, column *ColumnInfo) {
	query := `
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_schema || '.' || table_name = $1
        AND column_name = $2
        AND constraint_name LIKE '%pkey'
    `
	var primaryColumnName string
	err := db.QueryRow(query, tableName, column.Name).Scan(&primaryColumnName)
	column.IsPrimaryKey = err == nil
}

func GetIsNotNull(db *sql.DB, tableName string, column *ColumnInfo) {
	query := `SELECT 
    column_name 
FROM 
    information_schema.columns
WHERE 
    table_schema || '.' || table_name = $1
    AND is_nullable = 'NO'
	AND column_name = $2`
	var isNullableColumnName string
	err := db.QueryRow(query, tableName, column.Name).Scan(&isNullableColumnName)
	column.IsNullable = err != nil
}

// Obtiene las tablas de la base de datos
func GetDataBaseInfo(db *sql.DB) (*DataBaseInfo, error) {
	database := new(DataBaseInfo)
	query := `
        SELECT table_schema || '.' || table_name
        FROM information_schema.tables
    `
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error fetching database tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("error scanning tables: %w", err)
		}
		tableInfo, err := GetTableInfo(db, tableName)
		if err != nil {
			return nil, fmt.Errorf("error fetching table info: %w", err)
		}
		database.Tables = append(database.Tables, tableInfo)
	}

	return database, nil
}
