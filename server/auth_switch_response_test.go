package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckSha2CacheCredentials_EmptyPassword(t *testing.T) {
	tests := []struct {
		name           string
		clientAuthData []byte
		serverPassword string
		wantErr        error
	}{
		{
			name:           "empty client auth, empty server password",
			clientAuthData: []byte{},
			serverPassword: "",
			wantErr:        nil,
		},
		{
			name:           "empty client auth, non-empty server password",
			clientAuthData: []byte{},
			serverPassword: "secret",
			wantErr:        ErrAccessDeniedNoPassword,
		},
		{
			name:           "null byte client auth, empty server password",
			clientAuthData: []byte{0x00},
			serverPassword: "",
			wantErr:        nil,
		},
		{
			name:           "null byte client auth, non-empty server password",
			clientAuthData: []byte{0x00},
			serverPassword: "secret",
			wantErr:        ErrAccessDeniedNoPassword,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Conn{
				credential: Credential{Passwords: []string{tt.serverPassword}},
			}
			err := c.checkSha2CacheCredentials(tt.clientAuthData, c.credential)
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}
