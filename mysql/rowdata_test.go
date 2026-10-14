package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowDataParseNullAfterString(t *testing.T) {
	for _, tc := range []struct {
		name   string
		binary bool
		str    RowData
		null   RowData
		empty  RowData
	}{
		{
			name:  "text",
			str:   RowData{2, 'i', 'd'},
			null:  RowData{0xfb},
			empty: RowData{0},
		},
		{
			name:   "binary",
			binary: true,
			str:    RowData{0, 0, 2, 'i', 'd'},
			null:   RowData{0, 4}, // The first column uses bit 2 of the NULL bitmap.
			empty:  RowData{0, 0, 0},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := []*Field{{Type: MYSQL_TYPE_VAR_STRING}}
			row, err := tc.str.Parse(fields, tc.binary, nil)
			require.NoError(t, err)
			require.Equal(t, []byte("id"), row[0].AsString())

			// Reuse the destination as the client does for pooled result sets.
			row, err = tc.null.Parse(fields, tc.binary, row)
			require.NoError(t, err)
			require.EqualValues(t, FieldValueTypeNull, row[0].Type)
			require.Nil(t, row[0].Value())
			require.Nil(t, row[0].AsString())

			row, err = tc.empty.Parse(fields, tc.binary, row)
			require.NoError(t, err)
			require.EqualValues(t, FieldValueTypeString, row[0].Type)
			require.Equal(t, []byte{}, row[0].AsString())

			row, err = tc.str.Parse(fields, tc.binary, row)
			require.NoError(t, err)
			require.Equal(t, []byte("id"), row[0].AsString())
		})
	}
}
