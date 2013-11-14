package cryptic

import (
    "testing"
)

func TestStringEncryptionDecryption(t *testing.T) {
    plaintext := "This is a nice test"
    ciphertext, _, err := EncryptString(plaintext,"//lio123")
    if err != nil { t.Error(err); return }
    plaintext2, _, err2 := DecryptString(ciphertext,"//lio123")
    if err2 != nil { t.Error(err2); return }
    if plaintext != plaintext2 {
        t.Errorf("%s != %s", plaintext, plaintext2)
    }
}
