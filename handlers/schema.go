package handlers

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
)

// TableInfo represents basic table information
type TableInfo struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ColumnInfo represents column information
type ColumnInfo struct {
	Name             string  `json:"name"`
	DataType         string  `json:"data_type"`
	IsNullable       string  `json:"is_nullable"`
	Default          *string `json:"default"`
	MaxLength        *int    `json:"max_length"`
	NumericPrecision *int    `json:"numeric_precision"`
	NumericScale     *int    `json:"numeric_scale"`
}

// ForeignKeyInfo represents foreign key information
type ForeignKeyInfo struct {
	Column        string `json:"column"`
	ForeignTable  string `json:"foreign_table"`
	ForeignColumn string `json:"foreign_column"`
}

// TableSchema represents complete table schema
type TableSchema struct {
	Name        string           `json:"name"`
	Columns     []ColumnInfo     `json:"columns"`
	PrimaryKeys []string         `json:"primary_keys"`
	ForeignKeys []ForeignKeyInfo `json:"foreign_keys"`
}

func (h *Handler) GetDatabases(c *gin.Context) {
	rows, err := h.db.Query(`
		SELECT datname 
		FROM pg_database 
		WHERE datistemplate = false 
		ORDER BY datname
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		databases = append(databases, dbName)
	}

	c.JSON(http.StatusOK, gin.H{"databases": databases})
}

func (h *Handler) GetTables(c *gin.Context) {
	rows, err := h.db.Query(`
		SELECT table_name, table_type 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		ORDER BY table_name
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var table TableInfo
		if err := rows.Scan(&table.Name, &table.Type); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		tables = append(tables, table)
	}

	c.JSON(http.StatusOK, gin.H{"tables": tables})
}

func (h *Handler) GetTableColumns(c *gin.Context) {
	tableName := c.Param("name")

	rows, err := h.db.Query(`
		SELECT 
			column_name,
			data_type,
			is_nullable,
			column_default,
			character_maximum_length,
			numeric_precision,
			numeric_scale
		FROM information_schema.columns 
		WHERE table_schema = 'public' AND table_name = $1 
		ORDER BY ordinal_position
	`, tableName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var maxLen, precision, scale sql.NullInt64
		var def sql.NullString

		if err := rows.Scan(
			&col.Name, &col.DataType, &col.IsNullable, &def,
			&maxLen, &precision, &scale,
		); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		if def.Valid {
			col.Default = &def.String
		}
		if maxLen.Valid {
			val := int(maxLen.Int64)
			col.MaxLength = &val
		}
		if precision.Valid {
			val := int(precision.Int64)
			col.NumericPrecision = &val
		}
		if scale.Valid {
			val := int(scale.Int64)
			col.NumericScale = &val
		}

		columns = append(columns, col)
	}

	c.JSON(http.StatusOK, gin.H{
		"table_name": tableName,
		"columns":    columns,
	})
}

func (h *Handler) GetTablePrimaryKeys(c *gin.Context) {
	tableName := c.Param("name")

	rows, err := h.db.Query(`
		SELECT 
			column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = 'public' 
			AND table_name = $1 
			AND constraint_name IN (
				SELECT constraint_name 
				FROM information_schema.table_constraints 
				WHERE constraint_type = 'PRIMARY KEY'
			)
		ORDER BY ordinal_position
	`, tableName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		primaryKeys = append(primaryKeys, colName)
	}

	c.JSON(http.StatusOK, gin.H{
		"table_name":   tableName,
		"primary_keys": primaryKeys,
	})
}

func (h *Handler) GetTableForeignKeys(c *gin.Context) {
	tableName := c.Param("name")

	rows, err := h.db.Query(`
		SELECT
			kcu.column_name,
			ccu.table_name AS foreign_table_name,
			ccu.column_name AS foreign_column_name
		FROM information_schema.key_column_usage kcu
		JOIN information_schema.referential_constraints rc
			ON kcu.constraint_name = rc.constraint_name
		JOIN information_schema.constraint_column_usage ccu
			ON rc.unique_constraint_name = ccu.constraint_name
		WHERE kcu.table_schema = 'public' 
			AND kcu.table_name = $1
		ORDER BY kcu.column_name
	`, tableName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var foreignKeys []ForeignKeyInfo
	for rows.Next() {
		var fk ForeignKeyInfo
		if err := rows.Scan(&fk.Column, &fk.ForeignTable, &fk.ForeignColumn); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		foreignKeys = append(foreignKeys, fk)
	}

	c.JSON(http.StatusOK, gin.H{
		"table_name":   tableName,
		"foreign_keys": foreignKeys,
	})
}

func (h *Handler) GetFullSchema(c *gin.Context) {
	// Get all tables
	tableRows, err := h.db.Query(`
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public' 
		ORDER BY table_name
	`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer tableRows.Close()

	var tables []string
	for tableRows.Next() {
		var tableName string
		if err := tableRows.Scan(&tableName); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		tables = append(tables, tableName)
	}

	var schema []TableSchema
	for _, table := range tables {
		tableSchema, err := h.getTableSchema(table)
		if err != nil {
			continue // Skip tables that can't be read
		}
		schema = append(schema, tableSchema)
	}

	c.JSON(http.StatusOK, gin.H{"schema": schema})
}

func (h *Handler) getTableSchema(tableName string) (TableSchema, error) {
	var schema TableSchema
	schema.Name = tableName

	// Get columns
	colRows, err := h.db.Query(`
		SELECT 
			column_name,
			data_type,
			is_nullable,
			column_default
		FROM information_schema.columns 
		WHERE table_schema = 'public' AND table_name = $1 
		ORDER BY ordinal_position
	`, tableName)
	if err != nil {
		return schema, err
	}
	defer colRows.Close()

	for colRows.Next() {
		var col ColumnInfo
		var def sql.NullString

		colRows.Scan(&col.Name, &col.DataType, &col.IsNullable, &def)

		if def.Valid {
			col.Default = &def.String
		}
		schema.Columns = append(schema.Columns, col)
	}

	// Get primary keys
	pkRows, err := h.db.Query(`
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = 'public' 
			AND table_name = $1 
			AND constraint_name IN (
				SELECT constraint_name 
				FROM information_schema.table_constraints 
				WHERE constraint_type = 'PRIMARY KEY'
			)
	`, tableName)
	if err == nil {
		defer pkRows.Close()
		for pkRows.Next() {
			var colName string
			pkRows.Scan(&colName)
			schema.PrimaryKeys = append(schema.PrimaryKeys, colName)
		}
	}

	// Get foreign keys
	fkRows, err := h.db.Query(`
		SELECT
			kcu.column_name,
			ccu.table_name AS foreign_table_name,
			ccu.column_name AS foreign_column_name
		FROM information_schema.key_column_usage kcu
		JOIN information_schema.referential_constraints rc
			ON kcu.constraint_name = rc.constraint_name
		JOIN information_schema.constraint_column_usage ccu
			ON rc.unique_constraint_name = ccu.constraint_name
		WHERE kcu.table_schema = 'public' 
			AND kcu.table_name = $1
	`, tableName)
	if err == nil {
		defer fkRows.Close()
		for fkRows.Next() {
			var fk ForeignKeyInfo
			fkRows.Scan(&fk.Column, &fk.ForeignTable, &fk.ForeignColumn)
			schema.ForeignKeys = append(schema.ForeignKeys, fk)
		}
	}

	return schema, nil
}
