package handlers

import (
	"net/http"
	"strings"

	sqlparser "github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/gin-gonic/gin"
)

type QueryRequest struct {
	SQL string `json:"sql"`
}

func (h *Handler) RunQuery(c *gin.Context) {
	var req QueryRequest

	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}

	sqlText := strings.TrimSpace(req.SQL)
	if sqlText == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "SQL cannot be empty"})
		return
	}

	// Parse SQL syntax
	stmt, err := sqlparser.Parse(sqlText)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "SQL syntax error: " + err.Error()})
		return
	}

	// Only allow SELECT
	switch stmt.(type) {
	case *sqlparser.Select:
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "Only SELECT statements are allowed"})
		return
	}

	// Add LIMIT to protect DB
	if !strings.Contains(strings.ToUpper(sqlText), "LIMIT") {
		sqlText += " LIMIT 100"
	}

	// Execute query
	rows, err := h.db.Query(sqlText)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Execution failed: " + err.Error()})
		return
	}
	defer rows.Close()

	// Get column names
	cols, err := rows.Columns()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get columns: " + err.Error()})
		return
	}

	// Process rows
	result := []map[string]interface{}{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Row scan failed: " + err.Error()})
			return
		}

		rowMap := map[string]interface{}{}
		for i, col := range cols {
			rowMap[col] = vals[i]
		}
		result = append(result, rowMap)
	}

	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Row iteration error: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"columns": cols,
		"rows":    result,
	})
}
