package cryptic

import (
    "os"
    "errors"
    "fmt"
    "crypto/hmac"
)

type CryptoFunc func(*os.File, *os.File) error

func processFile(inPath, outPath string, cryptoFunc CryptoFunc) error {
    inFile, err := os.Open(inPath)
    if err != nil { return err }
    defer inFile.Close()
    outFile, err2 := os.Create(outPath)
    if err2 != nil { return err }
    defer outFile.Close()
    return cryptoFunc(inFile, outFile)
}

func EncryptFile(inPath, outPath, passphrase string) error {
    err := processFile(inPath, outPath, func(inFile, outFile *os.File) error {
        var mac []byte
        _, err := outFile.WriteString("CrYpTiC")
        if err != nil { return err }
        _, err = outFile.Write(make([]byte, MacSize))
        if err != nil { return err }
        mac, err = EncryptIO(inFile, outFile, passphrase)
        if err != nil { return err }
        _, err = outFile.Seek(7, 0)
        if err != nil { return err }
        _, err = outFile.Write(mac)
        if err != nil { return err }
        return nil
    })
    if err != nil { os.Remove(outPath) }
    return err
}

func IsEncryptedFile(path string) (bool, error) {
    signature := make([]byte, 7)
    file, err := os.Open(path)
    if err != nil { return false, err }
    defer file.Close()
    _, err = file.Read(signature)
    if err != nil { return false, err }
    return string(signature) == "CrYpTiC", nil
}

func DecryptFile(inPath, outPath, passphrase string) error {
    err := processFile(inPath, outPath, func(inFile, outFile *os.File) error {
        var mac []byte
        signature := make([]byte, 7)
        _, err := inFile.Read(signature)
        if err != nil { return err }
        if string(signature) != "CrYpTiC" {
            return errors.New(fmt.Sprintf("Not encrypted %s", inPath))
        }
        expectedMac := make([]byte, MacSize)
        _, err = inFile.Read(expectedMac)
        if err != nil { return err }
        mac, err = DecryptIO(inFile, outFile, passphrase)
        if err != nil { return err }
        if !hmac.Equal(mac, expectedMac) {
            return errors.New("Authenticity verification failed")
        }
        return nil
    })
    if err != nil { os.Remove(outPath) }
    return err
}
