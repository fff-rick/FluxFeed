package infraauth

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/mail"
	"net/smtp"
	"strings"

	infraconfig "FluxFeed/internal/infra/config"
)

type SMTPMailer struct{ config infraconfig.SMTPConfig }

func NewSMTPMailer(config infraconfig.SMTPConfig) *SMTPMailer { return &SMTPMailer{config: config} }

func (m *SMTPMailer) Send(ctx context.Context, to, subject, htmlBody string) error {
	from, err := mail.ParseAddress(m.config.From)
	if err != nil {
		return err
	}
	address := net.JoinHostPort(m.config.Host, fmt.Sprintf("%d", m.config.Port))
	tlsConfig := &tls.Config{ServerName: m.config.Host, MinVersion: tls.VersionTLS12}
	var client *smtp.Client
	if strings.EqualFold(m.config.TLSMode, "implicit") {
		connection, dialErr := (&tls.Dialer{Config: tlsConfig}).DialContext(ctx, "tcp", address)
		if dialErr != nil {
			return dialErr
		}
		client, err = smtp.NewClient(connection, m.config.Host)
	} else {
		connection, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp", address)
		if dialErr != nil {
			return dialErr
		}
		client, err = smtp.NewClient(connection, m.config.Host)
		if err == nil {
			err = client.StartTLS(tlsConfig)
		}
	}
	if err != nil {
		return err
	}
	defer client.Close()
	if m.config.Username != "" {
		if err := client.Auth(smtp.PlainAuth("", m.config.Username, m.config.Password, m.config.Host)); err != nil {
			return err
		}
	}
	if err := client.Mail(from.Address); err != nil {
		return err
	}
	if err := client.Rcpt(to); err != nil {
		return err
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	message := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n%s", m.config.From, to, subject, htmlBody)
	if _, err = writer.Write([]byte(message)); err != nil {
		return err
	}
	if err = writer.Close(); err != nil {
		return err
	}
	return client.Quit()
}
