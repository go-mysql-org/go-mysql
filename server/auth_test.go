package server

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrAccessDenied(t *testing.T) {
	require.True(t, errors.Is(ErrAccessDenied, ErrAccessDenied))
	require.True(t, errors.Is(ErrAccessDeniedNoPassword, ErrAccessDenied))
	require.False(t, errors.Is(ErrAccessDenied, ErrAccessDeniedNoPassword))
}

func TestCompareNativePasswordAuthData_EmptyPassword(t *testing.T) {
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
			err := c.compareNativePasswordAuthData(tt.clientAuthData, c.credential)
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestCompareSha256PasswordAuthData_EmptyPassword(t *testing.T) {
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
			err := c.compareSha256PasswordAuthData(tt.clientAuthData, c.credential)
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestCompareCacheSha2PasswordAuthData_EmptyPassword(t *testing.T) {
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
			err := c.compareCacheSha2PasswordAuthData(tt.clientAuthData)
			if tt.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}
