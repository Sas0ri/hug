package utils

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"net/smtp"
	"path/filepath"
	"strings"
)

// Email represents a single message, which may contain
// attachments.
type Email struct {
	Subject     string
	From        string
	Password    string
	To          []string
	emailBodies []emailBody
	Attachments map[string][]byte
	EnableTLS   bool
	AuthHost    string
	SmtpServer  string
}

type emailBody struct {
	mimeType, bodyText string
}

// Compose begins a new email, filling the subject and body,
// and allocating memory for the list of recipients and the
// attachments.
func ComposeMail(Subject, authHost, smtpServer string) *Email {
	out := new(Email)
	out.EnableTLS = true
	out.Subject = Subject
	out.To = make([]string, 0, 1)
	out.Attachments = make(map[string][]byte)
	out.AuthHost = authHost
	out.SmtpServer = smtpServer
	return out
}

// Attach takes a filename and adds this to the message.
// Note that since only the filename is stored (and not
// its path, for privacy reasons), multiple files in
// different directories but with the same filename and
// extension cannot be sent.
func (e *Email) Attach(Filename string) error {
	b, err := ioutil.ReadFile(Filename)
	if err != nil {
		return err
	}

	_, fname := filepath.Split(Filename)
	e.Attachments[fname] = b
	return nil
}

// AddRecipient adds a single recipient.
func (e *Email) AddRecipient(Recipient string) {
	e.To = append(e.To, Recipient)
}

// AddRecipients adds one or more recipients.
func (e *Email) AddRecipients(Recipients ...string) {
	e.To = append(e.To, Recipients...)
}

// Add a body of any mimetype to the email.
func (e *Email) AddBody(mimeType, body string) {
	e.emailBodies = append(e.emailBodies, emailBody{mimeType, body})
}

// Adds an HTML body to the email, using the utf-8 charset.
func (e *Email) AddHtmlBody(body string) {
	e.AddBody("text/html; charset=utf-8", body)
}

// Adds a plaintext body to the email, using the utf-8 charset.
func (e *Email) AddTextBody(body string) {
	e.AddBody("text/plain; charset=utf-8", body)
}

// Send sends the email, returning any error encountered.
func (e *Email) Send() error {
	if e.From == "" {
		return errors.New("Error: No sender specified. Please set the Email.From field.")
	}
	if e.To == nil || len(e.To) == 0 {
		return errors.New("Error: No recipient specified. Please set the Email.To field.")
	}
	if e.Password == "" {
		return errors.New("Error: No password specified. Please set the Email.Password field.")
	}

	auth := smtp.PlainAuth(
		"",
		e.From,
		e.Password,
		e.AuthHost,
	)

	conn, err := smtp.Dial(e.SmtpServer)
	if err != nil {
		return err
	}
	if e.EnableTLS {
		err = conn.StartTLS(&tls.Config{})
		if err != nil {
			return err
		}
	}

	err = conn.Auth(auth)
	if err != nil {
		return err
	}

	err = conn.Mail(e.From)
	if err != nil {
		if strings.Contains(err.Error(), "530 5.5.1") {
			return errors.New("Error: Authentication failure. Your username or password is incorrect.")
		}
		return err
	}

	for _, recipient := range e.To {
		err = conn.Rcpt(recipient)
		if err != nil {
			return err
		}
	}

	wc, err := conn.Data()
	if err != nil {
		return err
	}
	defer wc.Close()
	_, err = wc.Write(e.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (e *Email) Bytes() []byte {
	buf := bytes.NewBuffer(nil)

	buf.WriteString("Subject: " + e.Subject + "\n")
	buf.WriteString("MIME-Version: 1.0\n")

	// Boundary is used by MIME to separate files.
	boundary := "f46d043c813270fc6b04c2d223da"

	if len(e.Attachments) > 0 {
		buf.WriteString("Content-Type: multipart/mixed; boundary=" + boundary + "\n")
		buf.WriteString("--" + boundary + "\n")
	}

	// buf.WriteString("Content-Type: text/plain; charset=utf-8\n")
	// buf.WriteString(e.Body)

	for _, body := range e.emailBodies {
		buf.WriteString(fmt.Sprintf("Content-Type: %s\n", body.mimeType))
		buf.WriteString(body.bodyText)
		buf.WriteString("\r\n")
	}

	if len(e.Attachments) > 0 {
		for k, v := range e.Attachments {
			buf.WriteString("\n\n--" + boundary + "\n")
			buf.WriteString("Content-Type: application/octet-stream\n")
			buf.WriteString("Content-Transfer-Encoding: base64\n")
			buf.WriteString("Content-Disposition: attachment; filename=\"" + k + "\"\n\n")

			b := make([]byte, base64.StdEncoding.EncodedLen(len(v)))
			base64.StdEncoding.Encode(b, v)
			buf.Write(b)
			buf.WriteString("\n--" + boundary)
		}

		buf.WriteString("--")
	}

	return buf.Bytes()
}
