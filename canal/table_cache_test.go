package canal

import (
	"container/list"
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/require"
)

func TestTableCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c := NewDefaultConfig()
	c.TableCacheCapacity = 2
	can := &Canal{
		cfg:                c,
		tables:             make(map[string]*schema.Table),
		tableCacheCapacity: 2,
		tableCacheOrder:    list.New(),
		tableCacheNodes:    make(map[string]*list.Element),
	}

	can.SetTableCache([]byte("db"), []byte("a"), &schema.Table{Name: "a"})
	can.SetTableCache([]byte("db"), []byte("b"), &schema.Table{Name: "b"})
	can.SetTableCache([]byte("db"), []byte("c"), &schema.Table{Name: "c"})

	require.NotContains(t, can.tables, "db.a")
	require.Contains(t, can.tables, "db.b")
	require.Contains(t, can.tables, "db.c")

	can.SetTableCache([]byte("db"), []byte("b"), &schema.Table{Name: "b"})
	can.SetTableCache([]byte("db"), []byte("d"), &schema.Table{Name: "d"})

	require.Contains(t, can.tables, "db.b")
	require.Contains(t, can.tables, "db.d")
	require.NotContains(t, can.tables, "db.c")
}

func TestClearTableCacheRemovesLRUTracking(t *testing.T) {
	c := NewDefaultConfig()
	c.TableCacheCapacity = 2
	can := &Canal{
		cfg:                c,
		tables:             make(map[string]*schema.Table),
		tableCacheCapacity: 2,
		tableCacheOrder:    list.New(),
		tableCacheNodes:    make(map[string]*list.Element),
	}

	can.SetTableCache([]byte("db"), []byte("a"), &schema.Table{Name: "a"})
	can.SetTableCache([]byte("db"), []byte("b"), &schema.Table{Name: "b"})
	can.ClearTableCache([]byte("db"), []byte("a"))
	can.SetTableCache([]byte("db"), []byte("c"), &schema.Table{Name: "c"})

	require.NotContains(t, can.tables, "db.a")
	require.Contains(t, can.tables, "db.b")
	require.Contains(t, can.tables, "db.c")
}
