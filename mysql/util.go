package mysql

import (
	"bytes"
	"cmp"
	"compress/zlib"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	mrand "math/rand"
	"runtime"
	"strconv"
	"strings"
	"time"

	"filippo.io/edwards25519"
	"github.com/go-mysql-org/go-mysql/utils"
	"github.com/pingcap/errors"
)

func Pstack() string {
	buf := make([]byte, 1024)
	n := runtime.Stack(buf, false)
	return string(buf[0:n])
}

func CalcNativePassword(scramble, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// stage2Hash = SHA1(stage1Hash)
	crypt.Reset()
	crypt.Write(stage1)
	stage2 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + stage2Hash)
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(stage2)
	scrambleHash := crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	return Xor(scrambleHash, stage1)
}

// Xor modifies hash1 in-place with XOR against hash2
func Xor(hash1 []byte, hash2 []byte) []byte {
	l := min(len(hash1), len(hash2))
	for i := range l {
		hash1[i] ^= hash2[i]
	}
	return hash1
}

// hash_stage1 = xor(reply, sha1(public_seed, hash_stage2))
func stage1FromReply(scramble []byte, seed []byte, stage2 []byte) []byte {
	crypt := sha1.New()
	crypt.Write(seed)
	crypt.Write(stage2)
	seededHash := crypt.Sum(nil)

	return Xor(scramble, seededHash)
}

// DecodePasswordHex decodes the standard format used by MySQL
// Password hashes in the 4.1 format always begin with a * character
// see https://dev.mysql.com/doc/mysql-security-excerpt/5.7/en/password-hashing.html
// ref vitess.io/vitess/go/mysql/auth_server.go
func DecodePasswordHex(hexEncodedPassword string) ([]byte, error) {
	if hexEncodedPassword[0] == '*' {
		hexEncodedPassword = hexEncodedPassword[1:]
	}
	return hex.DecodeString(hexEncodedPassword)
}

// EncodePasswordHex encodes to the standard format used by MySQL
// adds the optionally leading * to the hashed password
func EncodePasswordHex(passwordHash []byte) string {
	hexstr := strings.ToUpper(hex.EncodeToString(passwordHash))
	return "*" + hexstr
}

// NativePasswordHash = sha1(sha1(password))
func NativePasswordHash(password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write(password)
	stage1 := crypt.Sum(nil)

	// stage2Hash = SHA1(stage1Hash)
	crypt.Reset()
	crypt.Write(stage1)
	return crypt.Sum(stage1[:0])
}

func CompareNativePassword(reply []byte, stored []byte, seed []byte) bool {
	if len(stored) == 0 {
		return false
	}

	// hash_stage1 = xor(reply, sha1(public_seed, hash_stage2))
	stage1 := stage1FromReply(reply, seed, stored)
	// andidate_hash2 = sha1(hash_stage1)
	stage2 := sha1.Sum(stage1)

	// check(candidate_hash2 == hash_stage2)
	// use ConstantTimeCompare to mitigate timing based attacks
	return subtle.ConstantTimeCompare(stage2[:], stored) == 1
}

// CalcCachingSha2Password: Hash password using MySQL 8+ method (SHA256)
func CalcCachingSha2Password(scramble []byte, password []byte) []byte {
	if len(password) == 0 {
		return nil
	}

	// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))

	crypt := sha256.New()
	crypt.Write(password)
	message1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1)
	message1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1Hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	return Xor(message1, message2)
}

// Taken from https://github.com/go-sql-driver/mysql/pull/1518
func CalcEd25519Password(scramble []byte, password string) ([]byte, error) {
	// Derived from https://github.com/MariaDB/server/blob/d8e6bb00888b1f82c031938f4c8ac5d97f6874c3/plugin/auth_ed25519/ref10/sign.c
	// Code style is from https://cs.opensource.google/go/go/+/refs/tags/go1.21.5:src/crypto/ed25519/ed25519.go;l=207
	h := sha512.Sum512([]byte(password))

	s, err := edwards25519.NewScalar().SetBytesWithClamping(h[:32])
	if err != nil {
		return nil, err
	}
	A := (&edwards25519.Point{}).ScalarBaseMult(s)

	mh := sha512.New()
	mh.Write(h[32:])
	mh.Write(scramble)
	messageDigest := mh.Sum(nil)
	r, err := edwards25519.NewScalar().SetUniformBytes(messageDigest)
	if err != nil {
		return nil, err
	}

	R := (&edwards25519.Point{}).ScalarBaseMult(r)

	kh := sha512.New()
	kh.Write(R.Bytes())
	kh.Write(A.Bytes())
	kh.Write(scramble)
	hramDigest := kh.Sum(nil)
	k, err := edwards25519.NewScalar().SetUniformBytes(hramDigest)
	if err != nil {
		return nil, err
	}

	S := k.MultiplyAdd(k, s, r)

	return append(R.Bytes(), S.Bytes()...), nil
}

func EncryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}
	sha1v := sha1.New()
	return rsa.EncryptOAEP(sha1v, rand.Reader, pub, plain, nil)
}

const (
	SALT_LENGTH                = 16
	ITERATION_MULTIPLIER       = 1000
	SHA256_PASSWORD_ITERATIONS = 5
)

// generateUserSalt generate salt of given length for sha256_password hash
func generateUserSalt(length int) ([]byte, error) {
	// Generate a random salt of the given length
	// Implement this function for your project
	salt := make([]byte, length)
	_, err := rand.Read(salt)
	if err != nil {
		return []byte(""), err
	}

	// Restrict to 7-bit to avoid multi-byte UTF-8
	for i := range salt {
		salt[i] = salt[i] &^ 128
		for salt[i] == 36 || salt[i] == 0 { // '$' or NUL
			newval := make([]byte, 1)
			_, err := rand.Read(newval)
			if err != nil {
				return []byte(""), err
			}
			salt[i] = newval[0] &^ 128
		}
	}
	return salt, nil
}

// hashCrypt256 salt and hash a password the given number of iterations
func hashCrypt256(source, salt string, iterations uint64) (string, error) {
	actualIterations := iterations * ITERATION_MULTIPLIER
	hashInput := []byte(source + salt)
	var hash [32]byte
	for i := uint64(0); i < actualIterations; i++ {
		hash = sha256.Sum256(hashInput)
		hashInput = hash[:]
	}

	hashHex := hex.EncodeToString(hash[:])
	digest := fmt.Sprintf("$%d$%s$%s", iterations, salt, hashHex)
	return digest, nil
}

// Check256HashingPassword compares a password to a hash for sha256_password
// rather than trying to recreate just the hash we recreate the full hash
// and use that for comparison
func Check256HashingPassword(pwhash []byte, password string) (bool, error) {
	pwHashParts := bytes.Split(pwhash, []byte("$"))
	if len(pwHashParts) != 4 {
		return false, errors.New("failed to decode hash parts")
	}

	iterationsPart := pwHashParts[1]
	if len(iterationsPart) == 0 {
		return false, errors.New("iterations part is empty")
	}

	iterations, err := strconv.ParseUint(string(iterationsPart), 10, 64)
	if err != nil {
		return false, errors.New("failed to decode iterations")
	}
	salt := pwHashParts[2][:SALT_LENGTH]

	newHash, err := hashCrypt256(password, string(salt), iterations)
	if err != nil {
		return false, err
	}

	return subtle.ConstantTimeCompare(pwhash, []byte(newHash)) == 1, nil
}

// NewSha256PasswordHash creates a new password hash for sha256_password
func NewSha256PasswordHash(pwd string) (string, error) {
	salt, err := generateUserSalt(SALT_LENGTH)
	if err != nil {
		return "", err
	}
	return hashCrypt256(pwd, string(salt), SHA256_PASSWORD_ITERATIONS)
}

func DecompressMariadbData(data []byte) ([]byte, error) {
	// algorithm always 0=zlib
	// algorithm := (data[pos] & 0x07) >> 4
	headerSize := int(data[0] & 0x07)
	uncompressedDataSize := BFixedLengthInt(data[1 : 1+headerSize])
	uncompressedData := make([]byte, uncompressedDataSize)
	r, err := zlib.NewReader(bytes.NewReader(data[1+headerSize:]))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	_, err = io.ReadFull(r, uncompressedData)
	if err != nil {
		return nil, err
	}
	return uncompressedData, nil
}

// AppendLengthEncodedInteger: encodes a uint64 value and appends it to the given bytes slice
func AppendLengthEncodedInteger(b []byte, n uint64) []byte {
	switch {
	case n <= 250:
		return append(b, byte(n))

	case n <= 0xffff:
		return append(b, 0xfc, byte(n), byte(n>>8))

	case n <= 0xffffff:
		return append(b, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	}
	return append(b, 0xfe, byte(n), byte(n>>8), byte(n>>16), byte(n>>24),
		byte(n>>32), byte(n>>40), byte(n>>48), byte(n>>56))
}

func RandomBuf(size int) []byte {
	buf := make([]byte, size)
	// When this project supports golang 1.20 as a minimum, then this mrand.New(...)
	// line can be eliminated and the random number can be generated by simply
	// calling mrand.Intn()
	random := mrand.New(mrand.NewSource(time.Now().UTC().UnixNano()))
	min, max := 30, 127
	for i := 0; i < size; i++ {
		buf[i] = byte(min + random.Intn(max-min))
	}
	return buf
}

// FixedLengthInt: little endian
func FixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(i) * 8)
	}
	return num
}

// BFixedLengthInt: big endian
func BFixedLengthInt(buf []byte) uint64 {
	var num uint64 = 0
	for i, b := range buf {
		num |= uint64(b) << (uint(len(buf)-i-1) * 8)
	}
	return num
}

func LengthEncodedInt(b []byte) (num uint64, isNull bool, n int) {
	if len(b) == 0 {
		return 0, true, 0
	}

	switch b[0] {
	// 251: NULL
	case 0xfb:
		return 0, true, 1

		// 252: value of following 2
	case 0xfc:
		return uint64(b[1]) | uint64(b[2])<<8, false, 3

		// 253: value of following 3
	case 0xfd:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, false, 4

		// 254: value of following 8
	case 0xfe:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 |
				uint64(b[4])<<24 | uint64(b[5])<<32 | uint64(b[6])<<40 |
				uint64(b[7])<<48 | uint64(b[8])<<56,
			false, 9
	}

	// 0-250: value of first byte
	return uint64(b[0]), false, 1
}

func PutLengthEncodedInt(n uint64) []byte {
	switch {
	case n <= 250:
		return []byte{byte(n)}

	case n <= 0xffff:
		return []byte{0xfc, byte(n), byte(n >> 8)}

	case n <= 0xffffff:
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}

	default:
		// handles case n <= 0xffffffffffffffff
		// using 'default' instead of 'case' to avoid static analysis error
		// SA4003: every value of type uint64 is <= math.MaxUint64
		return []byte{
			0xfe, byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
			byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56),
		}
	}
}

// LengthEncodedString returns the string read as a bytes slice, whether the value is NULL,
// the number of bytes read and an error, in case the string is longer than
// the input slice
func LengthEncodedString(b []byte) ([]byte, bool, int, error) {
	// Get length
	num, isNull, n := LengthEncodedInt(b)
	if num < 1 {
		return b[n:n], isNull, n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return b[n-int(num) : n : n], false, n, nil
	}
	return nil, false, n, io.EOF
}

func SkipLengthEncodedString(b []byte) (int, error) {
	// Get length
	num, _, n := LengthEncodedInt(b)
	if num < 1 {
		return n, nil
	}

	n += int(num)

	// Check data length
	if len(b) >= n {
		return n, nil
	}
	return n, io.EOF
}

func PutLengthEncodedString(b []byte) []byte {
	data := make([]byte, 0, len(b)+9)
	data = append(data, PutLengthEncodedInt(uint64(len(b)))...)
	data = append(data, b...)
	return data
}

func Uint16ToBytes(n uint16) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
	}
}

func Uint32ToBytes(n uint32) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
	}
}

func Uint64ToBytes(n uint64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

func FormatBinaryDate(n int, data []byte) ([]byte, error) {
	switch n {
	case 0:
		return []byte("0000-00-00"), nil
	case 4:
		return []byte(fmt.Sprintf("%04d-%02d-%02d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3])), nil
	default:
		return nil, errors.Errorf("invalid date packet length %d", n)
	}
}

func FormatBinaryDateTime(n int, data []byte) ([]byte, error) {
	switch n {
	case 0:
		return []byte("0000-00-00 00:00:00"), nil
	case 4:
		return []byte(fmt.Sprintf("%04d-%02d-%02d 00:00:00",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3])), nil
	case 7:
		return []byte(fmt.Sprintf(
			"%04d-%02d-%02d %02d:%02d:%02d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3],
			data[4],
			data[5],
			data[6])), nil
	case 11:
		return []byte(fmt.Sprintf(
			"%04d-%02d-%02d %02d:%02d:%02d.%06d",
			binary.LittleEndian.Uint16(data[:2]),
			data[2],
			data[3],
			data[4],
			data[5],
			data[6],
			binary.LittleEndian.Uint32(data[7:11]))), nil
	default:
		return nil, errors.Errorf("invalid datetime packet length %d", n)
	}
}

func FormatBinaryTime(n int, data []byte) ([]byte, error) {
	if n == 0 {
		return []byte("00:00:00"), nil
	}

	var sign byte
	if data[0] == 1 {
		sign = byte('-')
	}

	var bytes []byte
	switch n {
	case 8:
		bytes = []byte(fmt.Sprintf(
			"%c%02d:%02d:%02d",
			sign,
			uint16(data[1])*24+uint16(data[5]),
			data[6],
			data[7],
		))
	case 12:
		bytes = []byte(fmt.Sprintf(
			"%c%02d:%02d:%02d.%06d",
			sign,
			uint16(data[1])*24+uint16(data[5]),
			data[6],
			data[7],
			binary.LittleEndian.Uint32(data[8:12]),
		))
	default:
		return nil, errors.Errorf("invalid time packet length %d", n)
	}
	if bytes[0] == 0 {
		return bytes[1:], nil
	}
	return bytes, nil
}

var (
	DONTESCAPE = byte(255)

	EncodeMap [256]byte
)

// Escape: only support utf-8
func Escape(sql string) string {
	dest := make([]byte, 0, 2*len(sql))

	for _, w := range utils.StringToByteSlice(sql) {
		if c := EncodeMap[w]; c == DONTESCAPE {
			dest = append(dest, w)
		} else {
			dest = append(dest, '\\', c)
		}
	}

	return string(dest)
}

func GetNetProto(addr string) string {
	if strings.Contains(addr, "/") {
		return "unix"
	} else {
		return "tcp"
	}
}

// ErrorEqual returns a boolean indicating whether err1 is equal to err2.
func ErrorEqual(err1, err2 error) bool {
	e1 := errors.Cause(err1)
	e2 := errors.Cause(err2)

	if e1 == e2 {
		return true
	}

	if e1 == nil || e2 == nil {
		return e1 == e2
	}

	return e1.Error() == e2.Error()
}

func compareSubVersion(typ, a, b, aFull, bFull string) (int, error) {
	if a == "" || b == "" {
		return 0, nil
	}

	var aNum, bNum int
	var err error

	if aNum, err = strconv.Atoi(a); err != nil {
		return 0, fmt.Errorf("cannot parse %s version %s of %s", typ, a, aFull)
	}
	if bNum, err = strconv.Atoi(b); err != nil {
		return 0, fmt.Errorf("cannot parse %s version %s of %s", typ, b, bFull)
	}

	return cmp.Compare(aNum, bNum), nil
}

// Compares version triplet strings, ignoring anything past `-` in version.
// A version string like 8.0 will compare as if third triplet were a wildcard.
// A version string like 8 will compare as if second & third triplets were wildcards.
func CompareServerVersions(a, b string) (int, error) {
	aNumbers, _, _ := strings.Cut(a, "-")
	bNumbers, _, _ := strings.Cut(b, "-")

	aMajor, aRest, _ := strings.Cut(aNumbers, ".")
	bMajor, bRest, _ := strings.Cut(bNumbers, ".")

	if majorCompare, err := compareSubVersion("major", aMajor, bMajor, a, b); err != nil || majorCompare != 0 {
		return majorCompare, err
	}

	aMinor, aPatch, _ := strings.Cut(aRest, ".")
	bMinor, bPatch, _ := strings.Cut(bRest, ".")

	if minorCompare, err := compareSubVersion("minor", aMinor, bMinor, a, b); err != nil || minorCompare != 0 {
		return minorCompare, err
	}

	return compareSubVersion("patch", aPatch, bPatch, a, b)
}

var encodeRef = map[byte]byte{
	'\x00': '0',
	'\'':   '\'',
	'"':    '"',
	'\b':   'b',
	'\n':   'n',
	'\r':   'r',
	'\t':   't',
	26:     'Z', // ctl-Z
	'\\':   '\\',
}

func init() {
	for i := range EncodeMap {
		EncodeMap[i] = DONTESCAPE
	}
	for k, v := range encodeRef {
		EncodeMap[k] = v
	}
}
