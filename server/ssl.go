package server

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"

	"github.com/go-mysql-org/go-mysql/utils"
)

// NewServerTLSConfig: generate TLS config for server side
// controlling the security level by authType
func NewServerTLSConfig(caPem, certPem, keyPem []byte, authType tls.ClientAuthType) *tls.Config {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPem) {
		panic("failed to add ca PEM")
	}

	cert, err := tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		panic(err)
	}

	config := &tls.Config{
		ClientAuth:   authType,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
	}
	return config
}

// rsaPublicKeyBytes returns the PEM-encoded public key for an RSA private key
func rsaPublicKeyBytes(privKey *rsa.PrivateKey) []byte {
	if privKey == nil {
		return nil
	}
	pubKeyBytes, err := x509.MarshalPKIXPublicKey(&privKey.PublicKey)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal RSA public key: %v", err))
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubKeyBytes})
}

// getRSAKeyPairFromPEM extracts the RSA private key and PEM-encoded public key from a PEM-encoded private key
func getRSAKeyPairFromPEM(keyPem []byte) (*rsa.PrivateKey, []byte) {
	block, _ := pem.Decode(keyPem)
	if block == nil {
		panic("failed to decode PEM block")
	}
	privKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		panic(err)
	}
	return privKey, rsaPublicKeyBytes(privKey)
}

// generate and sign RSA certificates with given CA
// see: https://fale.io/blog/2017/06/05/create-a-pki-in-golang/
func generateAndSignRSACerts(caPem, caKey []byte) ([]byte, []byte) {
	// Load CA
	catls, err := tls.X509KeyPair(caPem, caKey)
	if err != nil {
		panic(err)
	}
	ca, err := x509.ParseCertificate(catls.Certificate[0])
	if err != nil {
		panic(err)
	}

	// use the CA to sign certificates
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		panic(err)
	}
	cert := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:  []string{"ORGANIZATION_NAME"},
			Country:       []string{"COUNTRY_CODE"},
			Province:      []string{"PROVINCE"},
			Locality:      []string{"CITY"},
			StreetAddress: []string{"ADDRESS"},
			PostalCode:    []string{"POSTAL_CODE"},
		},
		NotBefore:    utils.Now(),
		NotAfter:     utils.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)

	// sign the certificate
	cert_b, err := x509.CreateCertificate(rand.Reader, ca, cert, &priv.PublicKey, catls.PrivateKey)
	if err != nil {
		panic(err)
	}
	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert_b})
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return certPem, keyPem
}

// generate CA in PEM
// see: https://github.com/golang/go/blob/master/src/crypto/tls/generate_cert.go
func generateCA() ([]byte, []byte) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		panic(err)
	}
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization:  []string{"ORGANIZATION_NAME"},
			Country:       []string{"COUNTRY_CODE"},
			Province:      []string{"PROVINCE"},
			Locality:      []string{"CITY"},
			StreetAddress: []string{"ADDRESS"},
			PostalCode:    []string{"POSTAL_CODE"},
		},
		NotBefore:             utils.Now(),
		NotAfter:              utils.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign | x509.KeyUsageKeyEncipherment,
		BasicConstraintsValid: true,
	}

	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		panic(err)
	}

	caPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	caKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	return caPem, caKey
}
