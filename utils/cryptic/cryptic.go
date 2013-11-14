// Package cryptic implements encryption and decryption functions using AES256
// and SHA256.
package cryptic

import (
    "io"
    "errors"
    "bytes"
    "crypto/rand"
    "crypto/sha256"
    "crypto/aes"
    "crypto/cipher"
    "crypto/hmac"
    "encoding/base64"
)

const MacSize = sha256.Size

func PassphraseToKey(passphrase string) []byte {
    h := sha256.New()
    h.Write([]byte(passphrase))
    return h.Sum(nil)
}

func Encrypt(plaintext []byte, passphrase string) (ciphertext, mac []byte, e error) {
    buf := new(bytes.Buffer)
    hashMac, err := EncryptIO(bytes.NewReader(plaintext), buf, passphrase)
    if err != nil { return nil, nil, err }
    return buf.Bytes(), hashMac, nil
}

func EncryptString(plaintext, passphrase string) (ciphertext string, mac []byte, e error) {
    ct, hashMac, err := Encrypt([]byte(plaintext), passphrase)
    if err != nil { return "", nil, err }
    return base64.StdEncoding.EncodeToString(ct), hashMac, nil
}

func EncryptIO(reader io.Reader, writer io.Writer, passphrase string) (mac []byte, e error) {
    key := PassphraseToKey(passphrase)
    hashMac := hmac.New(sha256.New, key)
    block, err := aes.NewCipher(key)
    if err != nil { return nil, err }
    iv := make([]byte, aes.BlockSize)
    if _, err = io.ReadFull(rand.Reader, iv); err != nil { return nil, err }
    stream := cipher.NewOFB(block, iv)
    _, err = writer.Write(iv)
    if err != nil { return nil, err }
    cipherWriter := &cipher.StreamWriter{S: stream, W: writer}
    _, err = cipherWriter.Write([]byte(passphrase))
    if err != nil { return nil, err }
    mw := io.MultiWriter(cipherWriter, hashMac)
    if _, err = io.Copy(mw, reader); err != nil { return nil, err }
    return hashMac.Sum(nil), nil
}

func Decrypt(ciphertext []byte, passphrase string) (plaintext, mac []byte, e error) {
    buf := new(bytes.Buffer)
    hashMac, err := DecryptIO(bytes.NewReader(ciphertext), buf, passphrase)
    if err != nil { return nil, nil, err }
    return buf.Bytes(), hashMac, nil
}

func DecryptString(ciphertext, passphrase string) (plaintext string, mac []byte, e error) {
    var pt []byte
    ct, err := base64.StdEncoding.DecodeString(ciphertext)
    if err != nil { return "", nil, err }
    pt, mac, err = Decrypt(ct, passphrase)
    if err != nil { return "", nil, err }
    return string(pt), mac, nil
}

func DecryptIO(reader io.Reader, writer io.Writer, passphrase string) (mac []byte, e error) {
    key := PassphraseToKey(passphrase)
    hashMac := hmac.New(sha256.New, key)
    block, err := aes.NewCipher(key)
    if err != nil { return nil, err }
    iv := make([]byte, aes.BlockSize)
    if _, err = io.ReadFull(reader, iv); err != nil { return nil, err }
    stream := cipher.NewOFB(block, iv)
    cipherReader := &cipher.StreamReader{S: stream, R: reader}
    pp := make([]byte, len([]byte(passphrase)))
    if _, err = io.ReadFull(cipherReader, pp); err != nil { return nil, err }
    if passphrase != string(pp) {
        return nil, errors.New("Incorrect passphrase")
    }
    mw := io.MultiWriter(writer, hashMac)
    if _, err = io.Copy(mw, cipherReader); err != nil { return nil, err }
    return hashMac.Sum(nil), nil
}
