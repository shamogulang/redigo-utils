package redigouitls

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

var pool *redis.Pool

func InitRedis(addr, password string, db int) {
	pool = &redis.Pool{
		MaxIdle:     10,
		MaxActive:   100,
		IdleTimeout: 240 * time.Second,
		Wait:        true,

		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr,
				redis.DialDatabase(db),
				redis.DialPassword(password),
				redis.DialConnectTimeout(5*time.Second),
				redis.DialReadTimeout(5*time.Second),
				redis.DialWriteTimeout(5*time.Second))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, lastUsed time.Time) error {
			if time.Since(lastUsed) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func GetConn() (redis.Conn, error) {
	if pool == nil {
		return nil, errors.New("redis pool not init")
	}
	return pool.Get(), nil
}

func Set(key string, value interface{}) error {
	conn, err := GetConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = conn.Do("SET", key, data)
	return err
}

func SetEx(key string, value interface{}, expireSeconds int) error {
	conn, err := GetConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if expireSeconds > 0 {
		_, err = conn.Do("SETEX", key, expireSeconds, data)
	} else {
		_, err = conn.Do("SET", key, data)
	}
	return err
}

func GetString(key string) (string, error) {
	var rsp string
	err := Get(key, &rsp)
	if err != nil {
		return "", err
	}
	return rsp, nil
}

func GetInt(key string) (int, error) {
	var i int
	err := Get(key, i)
	if err != nil {
		return 0, err
	}
	return i, nil
}

func GetBool(key string) (bool, error) {
	var b bool
	err := Get(key, b)
	if err != nil {
		return false, err
	}
	return b, nil
}

func GetT[T any](key string) (*T, error) {
	var t T
	err := Get(key, &t)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func Get(key string, v interface{}) error {
	conn, err := GetConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	data, err := redis.Bytes(conn.Do("GET", key))
	fmt.Println(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func Del(key string) error {
	conn, err := GetConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Do("DEL", key)
	return err
}

func Exist(key string) (bool, error) {
	conn, err := GetConn()
	if err != nil {
		return false, err
	}
	defer conn.Close()
	return redis.Bool(conn.Do("EXISTS", key))
}

func Hset(key, field string, value interface{}) error {
	return HsetEx(key, field, value, 0)
}

func HsetEx(key, field string, value interface{}, expireSeconds int) error {
	conn, err := GetConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if expireSeconds > 0 {
		var hsetExpireScript = redis.NewScript(1,
			`redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
			 if tonumber(ARGV[3]) > 0 then 
			    redis.call("EXPIRE", KEYS[1], ARGV[3])
			 end 
			 return 1`)
		_, err = hsetExpireScript.Do(conn, key, field, data, expireSeconds)
	} else {
		_, err = conn.Do("HSET", key, field, data)
	}
	return err
}

func Hget(key, field string, v interface{}) error {
	conn, err := GetConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	data, err := redis.Bytes(conn.Do("HGET", key, field))
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func HgetString(key, field string) (string, error) {
	var s string
	err := Hget(key, field, &s)
	return s, err
}

func HgetInt(key, field string, v interface{}) (int, error) {
	var i int
	err := Hget(key, field, &i)
	return i, err
}

func HgetBool(key, field string, v interface{}) (bool, error) {
	var b bool
	err := Hget(key, field, &b)
	return b, err
}
