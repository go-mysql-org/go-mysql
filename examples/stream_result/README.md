# StreamResult Example

Demonstrates streaming MySQL query results row by row - useful for large result sets.

## Quick Start

```bash
go run main.go                           # Start server
mysql -h 127.0.0.1 -P 4000 -u root       # Connect
mysql> SELECT * FROM users;              # Stream 100 rows
```

## Usage

```go
// 1. Create StreamResult
fields := []*mysql.Field{
    {Name: []byte("id"), Type: mysql.MYSQL_TYPE_LONG},
    {Name: []byte("name"), Type: mysql.MYSQL_TYPE_VAR_STRING},
}
sr := mysql.NewStreamResult(fields, 10)  // buffer size = 10

// 2. Write rows in goroutine
go func() {
    defer sr.Close()  // Always close!
    ctx := context.Background()
    
    for _, data := range source {
        if !sr.WriteRow(ctx, []any{data.ID, data.Name}) {
            return  // Stream closed, stop writing
        }
    }
}()

// 3. Return result
return sr.AsResult(), nil
```

## Key Points

| Rule | Why |
|------|-----|
| Always `defer sr.Close()` | Prevents consumer from waiting forever |
| Check `WriteRow()` return | Returns `false` if stream closed - stop writing to avoid goroutine leak |
| Use context for timeout | `WriteRow(ctx, row)` respects context cancellation |
| Use `SetError(err)` for errors | Consumer checks via `sr.Err()` after reading |
