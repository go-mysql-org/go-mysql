package main

import (
	"net"
	"github.com/siddontang/go-mysql/server"
	"github.com/siddontang/go-mysql/mysql"
	"crypto/tls"
	"time"
)

type RemoteThrottleProvider struct {
	*server.InMemoryProvider
	delay int // in milliseconds
}

func (m *RemoteThrottleProvider) GetCredential(username string) (password string, found bool, err error) {
	time.Sleep(time.Millisecond * time.Duration(m.delay))
	return m.InMemoryProvider.GetCredential(username)
}

func main() {
	l, _ := net.Listen("tcp", "127.0.0.1:3306")
	// user either the in-memory credential provider or the remote credential provider (you can implement your own)
	//inMemProvider := server.NewInMemoryProvider()
	//inMemProvider.AddUser("root", "123")
	remoteProvider := &RemoteThrottleProvider{server.NewInMemoryProvider(), 10 + 50}
	remoteProvider.AddUser("root", "123")
	var tlsConf = server.NewServerTLSConfig(caPem, certPem, keyPem, tls.VerifyClientCertIfGiven)
	for {
		c, _ := l.Accept()
		go func() {
			// Create a connection with user root and an empty password.
			// You can use your own handler to handle command here.
			svr := server.NewServer("8.0.12", mysql.DEFAULT_COLLATION_ID, server.CACHING_SHA2_PASSWORD, pubPem, tlsConf)
			conn, err := server.NewCustomizedConn(c, svr, remoteProvider, server.EmptyHandler{})

			if err != nil {
				panic(err)
			}

			for {
				conn.HandleCommand()
			}
		}()
	}
}

var pubPem = []byte(`-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAsraCori69OXEkA07Ykp2
Ju+aHz33PqgKj0qbSbPm6ePh2mer2GWOxC4q1wdRwzgddwTTqSdonhM4XuVyyNqq
gM7uv9JoWCONcKo28cPRK7gH7up7nYFllNFXUAA0/XQ+95tqtdITNplQLIceFIXz
5Bvi9fThcpf9M6qKdNUa2Wd24rM/n6qxoUG2ksDDVXQC30RAHkGCdNi10iya8Pj/
ZaEG86NXFpvvnLHRHiih/gXe7nby1sR6BxaEG2bLZd0cjdL5MuWOPeQ450H6mCtV
SX4poNq9YrdP4XW9M0N7nocRU0p5aUvLWxy6XrUTSP0iRkC7ppEPG0p2Xtsq7QGT
MwIDAQAB
-----END PUBLIC KEY-----`)

var certPem = []byte(`-----BEGIN CERTIFICATE-----
MIIDBjCCAe4CCQDg06wCf7hcuDANBgkqhkiG9w0BAQUFADBFMQswCQYDVQQGEwJB
VTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0
cyBQdHkgTHRkMB4XDTE4MDgxOTA4NDUyNVoXDTI4MDgxNjA4NDUyNVowRTELMAkG
A1UEBhMCQVUxEzARBgNVBAgTClNvbWUtU3RhdGUxITAfBgNVBAoTGEludGVybmV0
IFdpZGdpdHMgUHR5IEx0ZDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
ALK2gqK4uvTlxJANO2JKdibvmh899z6oCo9Km0mz5unj4dpnq9hljsQuKtcHUcM4
HXcE06knaJ4TOF7lcsjaqoDO7r/SaFgjjXCqNvHD0Su4B+7qe52BZZTRV1AANP10
PvebarXSEzaZUCyHHhSF8+Qb4vX04XKX/TOqinTVGtlnduKzP5+qsaFBtpLAw1V0
At9EQB5BgnTYtdIsmvD4/2WhBvOjVxab75yx0R4oof4F3u528tbEegcWhBtmy2Xd
HI3S+TLljj3kOOdB+pgrVUl+KaDavWK3T+F1vTNDe56HEVNKeWlLy1scul61E0j9
IkZAu6aRDxtKdl7bKu0BkzMCAwEAATANBgkqhkiG9w0BAQUFAAOCAQEAma3yFqR7
xkeaZBg4/1I3jSlaNe5+2JB4iybAkMOu77fG5zytLomTbzdhewsuBwpTVMJdga8T
IdPeIFCin1U+5SkbjSMlpKf+krE+5CyrNJ5jAgO9ATIqx66oCTYXfGlNapGRLfSE
sa0iMqCe/dr4GPU+flW2DZFWiyJVDSF1JjReQnfrWY+SD2SpP/lmlgltnY8MJngd
xBLG5nsZCpUXGB713Q8ZyIm2ThVAMiskcxBleIZDDghLuhGvY/9eFJhZpvOkjWa6
XGEi4E1G/SA+zVKFl41nHKCdqXdmIOnpcLlFBUVloQok5a95Kqc1TYw3f+WbdFff
99dAgk3gWwWZQA==
-----END CERTIFICATE-----`)

var keyPem = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAsraCori69OXEkA07Ykp2Ju+aHz33PqgKj0qbSbPm6ePh2mer
2GWOxC4q1wdRwzgddwTTqSdonhM4XuVyyNqqgM7uv9JoWCONcKo28cPRK7gH7up7
nYFllNFXUAA0/XQ+95tqtdITNplQLIceFIXz5Bvi9fThcpf9M6qKdNUa2Wd24rM/
n6qxoUG2ksDDVXQC30RAHkGCdNi10iya8Pj/ZaEG86NXFpvvnLHRHiih/gXe7nby
1sR6BxaEG2bLZd0cjdL5MuWOPeQ450H6mCtVSX4poNq9YrdP4XW9M0N7nocRU0p5
aUvLWxy6XrUTSP0iRkC7ppEPG0p2Xtsq7QGTMwIDAQABAoIBAGh1m8hHWCg7gXh9
838RbRx3IswuKS27hWiaQEiFWmzOIb7KqDy1qAxtu+ayRY1paHegH6QY/+Kd824s
ibpzbgQacJ04/HrAVTVMmQ8Z2VLHoAN7lcPL1bd14aZGaLLZVtDeTDJ413grhxxv
4ho27gcgcbo4Z+rWgk7H2WRPCAGYqWYAycm3yF5vy9QaO6edU+T588YsEQOos5iy
5pVFSGDGZkcUp1ukL3BJYR+jvygn6WPCobQ/LScUdi+ucitaI9i+UdlLokZARVRG
M/msqcTM73thR8yVRcexU6NUDxRBfZ/f7moSAEbBmGDXuxDcIyH9KGMQ2rMtN1X3
lK8UNwkCgYEA2STJq/IUQHjdqd3Dqh/Q7Zm8/pMWFqLJSkqpnFtFsXPyUOx9zDOy
KqkIfGeyKwvsj9X9BcZ0FUKj9zoct1/WpPY+h7i7+z0MIujBh4AMjAcDrt4o76yK
UHuVmG2xKTdJoAbqOdToQeX6E82Ioal5pbB2W7AbCQScNBPZ52jxgtcCgYEA0rE7
2dFiRm0YmuszFYxft2+GP6NgP3R2TQNEooi1uCXG2xgwObie1YCHzpZ5CfSqJIxP
XB7DXpIWi7PxJoeai2F83LnmdFz6F1BPRobwDoSFNdaSKLg4Yf856zpgYNKhL1fE
OoOXj4VBWBZh1XDfZV44fgwlMIf7edOF1XOagwUCgYAw953O+7FbdKYwF0V3iOM5
oZDAK/UwN5eC/GFRVDfcM5RycVJRCVtlSWcTfuLr2C2Jpiz/72fgH34QU3eEVsV1
v94MBznFB1hESw7ReqvZq/9FoO3EVrl+OtBaZmosLD6bKtQJJJ0Xtz/01UW5hxla
pveZ55XBK9v51nwuNjk4UwKBgHD8fJUllSchUCWb5cwzeAz98Kdl7LJ6uQo5q2/i
EllLYOWThiEeIYdrIuklholRPIDXAaPsF2c6vn5yo+q+o6EFSZlw0+YpCjDAb5Lp
wAh5BprFk6HkkM/0t9Guf4rMyYWC8odSlE9x7YXYkuSMYDCTI4Zs6vCoq7I8PbQn
B4AlAoGAZ6Ee5m/ph5UVp/3+cR6jCY7aHBUU/M3pbJSkVjBW+ymEBVJ6sUdz8k3P
x8BiPEQggNN7faWBqRWP7KXPnDYHh6shYUgPJwI5HX6NE/ZDnnXjeysHRyf0oCo5
S6tHXwHNKB5HS1c/KDyyNGjP2oi/MF4o/MGWNWEcK6TJA3RGOYM=
-----END RSA PRIVATE KEY-----`)

var caPem = []byte(`-----BEGIN CERTIFICATE-----
MIIDtTCCAp2gAwIBAgIJANeS1FOzWXlZMA0GCSqGSIb3DQEBBQUAMEUxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQwHhcNMTgwODE2MTUxNDE5WhcNMjEwNjA1MTUxNDE5WjBF
MQswCQYDVQQGEwJBVTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50
ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEAsV6xlhFxMn14Pn7XBRGLt8/HXmhVVu20IKFgIOyX7gAZr0QLsuT1fGf5
zH9HrlgOMkfdhV847U03KPfUnBsi9lS6/xOxnH/OzTYM0WW0eNMGF7eoxrS64GSb
PVX4pLi5+uwrrZT5HmDgZi49ANmuX6UYmH/eRRvSIoYUTV6t0aYsLyKvlpEAtRAe
4AlKB236j5ggmJ36QUhTFTbeNbeOOgloTEdPK8Y/kgpnhiqzMdPqqIc7IeXUc456
yX8MJUgniTM2qCNTFdEw+C2Ok0RbU6TI2SuEgVF4jtCcVEKxZ8kYbioONaePQKFR
/EhdXO+/ag1IEdXElH9knLOfB+zCgwIDAQABo4GnMIGkMB0GA1UdDgQWBBQgHiwD
00upIbCOunlK4HRw89DhjjB1BgNVHSMEbjBsgBQgHiwD00upIbCOunlK4HRw89Dh
jqFJpEcwRTELMAkGA1UEBhMCQVUxEzARBgNVBAgTClNvbWUtU3RhdGUxITAfBgNV
BAoTGEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZIIJANeS1FOzWXlZMAwGA1UdEwQF
MAMBAf8wDQYJKoZIhvcNAQEFBQADggEBAFMZFQTFKU5tWIpWh8BbVZeVZcng0Kiq
qwbhVwaTkqtfmbqw8/w+faOWylmLncQEMmgvnUltGMQlQKBwQM2byzPkz9phal3g
uI0JWJYqtcMyIQUB9QbbhrDNC9kdt/ji/x6rrIqzaMRuiBXqH5LQ9h856yXzArqd
cAQGzzYpbUCIv7ciSB93cKkU73fQLZVy5ZBy1+oAa1V9U4cb4G/20/PDmT+G3Gxz
pEjeDKtz8XINoWgA2cSdfAhNZt5vqJaCIZ8qN0z6C7SUKwUBderERUMLUXdhUldC
KTVHyEPvd0aULd5S5vEpKCnHcQmFcLdoN8t9k9pR9ZgwqXbyJHlxWFo=
-----END CERTIFICATE-----`)