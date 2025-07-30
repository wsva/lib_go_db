package db

import (
	"github.com/wsva/lib_go/crypto"
)

func (d *DB) NeedEncrypt() bool {
	if d.MySQL.DSN != "" && !crypto.IsAES256Text(d.MySQL.DSN) {
		return true
	}
	if d.MySQL.Password != "" && !crypto.IsAES256Text(d.MySQL.Password) {
		return true
	}
	if d.Oracle.Password != "" && !crypto.IsAES256Text(d.Oracle.Password) {
		return true
	}
	if d.PostgreSQL.Password != "" && !crypto.IsAES256Text(d.PostgreSQL.Password) {
		return true
	}
	return false
}

func (d *DB) Encrypt(aeskey, aesiv string) error {
	var err error
	d.MySQL.DSN, err = encrypt(aeskey, aesiv, d.MySQL.DSN)
	if err != nil {
		return err
	}
	d.MySQL.Password, err = encrypt(aeskey, aesiv, d.MySQL.Password)
	if err != nil {
		return err
	}
	d.Oracle.Password, err = encrypt(aeskey, aesiv, d.Oracle.Password)
	if err != nil {
		return err
	}
	d.PostgreSQL.Password, err = encrypt(aeskey, aesiv, d.PostgreSQL.Password)
	if err != nil {
		return err
	}
	return nil
}

func (d *DB) Decrypt(aeskey, aesiv string) error {
	var err error
	d.MySQL.DSN, err = decrypt(aeskey, aesiv, d.MySQL.DSN)
	if err != nil {
		return err
	}
	d.MySQL.Password, err = decrypt(aeskey, aesiv, d.MySQL.Password)
	if err != nil {
		return err
	}
	d.Oracle.Password, err = decrypt(aeskey, aesiv, d.Oracle.Password)
	if err != nil {
		return err
	}
	d.PostgreSQL.Password, err = decrypt(aeskey, aesiv, d.PostgreSQL.Password)
	if err != nil {
		return err
	}
	return nil
}

// 如果有错误，将text原样返回
func encrypt(aeskey, aesiv, text string) (string, error) {
	if text != "" && !crypto.IsAES256Text(text) {
		ctext, err := crypto.AES256Encrypt(aeskey, aesiv, text)
		if err != nil {
			return text, err
		}
		return ctext, nil
	}
	return text, nil
}

// 如果有错误，将ctext原样返回
func decrypt(aeskey, aesiv, ctext string) (string, error) {
	if crypto.IsAES256Text(ctext) {
		text, err := crypto.AES256Decrypt(aeskey, aesiv, ctext)
		if err != nil {
			return ctext, err
		}
		return text, nil
	}
	return ctext, nil
}
