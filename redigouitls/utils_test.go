package redigouitls

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newTestRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start error: %v", err)
	}
	return s
}

func initPoolFromMiniredis(t *testing.T, s *miniredis.Miniredis) {
	t.Helper()
	InitRedis(s.Addr(), "", 0)
}

func TestSetGetDelExist(t *testing.T) {
	s := newTestRedis(t)
	defer s.Close()

	initPoolFromMiniredis(t, s)

	// Set
	if err := Set("k1", "hello"); err != nil {
		t.Fatalf("set error: %v", err)
	}

	// Get
	v, err := GetString("k1")
	if err != nil {
		t.Fatalf("get error: %v", err)
	}
	if v != "hello" {
		t.Fatalf("expected hello got %s", v)
	}

	// Exist
	ok, err := Exist("k1")
	if err != nil {
		t.Fatalf("exist error: %v", err)
	}
	if !ok {
		t.Fatalf("expected exist")
	}

	// Del
	if err := Del("k1"); err != nil {
		t.Fatalf("del error: %v", err)
	}

	ok, _ = Exist("k1")
	if ok {
		t.Fatalf("expected not exist after del")
	}
}

func TestSetExExpire(t *testing.T) {
	s := newTestRedis(t)
	defer s.Close()

	initPoolFromMiniredis(t, s)

	if err := SetEx("k2", "v2", 1); err != nil {
		t.Fatalf("setex error: %v", err)
	}

	s.FastForward(2 * time.Second)

	_, err := GetString("k2")
	if err == nil {
		t.Fatalf("expected key expired")
	}
}

func TestHsetHget(t *testing.T) {
	s := newTestRedis(t)
	defer s.Close()

	initPoolFromMiniredis(t, s)

	if err := Hset("hash1", "field1", "value1"); err != nil {
		t.Fatalf("hset error: %v", err)
	}

	v, err := HgetString("hash1", "field1")
	if err != nil {
		t.Fatalf("hget error: %v", err)
	}
	if v != "value1" {
		t.Fatalf("expected value1 got %s", v)
	}
}

func TestHsetEx(t *testing.T) {
	s := newTestRedis(t)
	defer s.Close()

	initPoolFromMiniredis(t, s)

	if err := HsetEx("hash1", "field1", "value1", 1); err != nil {
		t.Fatalf("hset error: %v", err)
	}

	s.FastForward(2 * time.Second)

	data, err := HgetString("hash1", "field1")
	if err == nil {
		t.Fatalf("hget expected key expired: %v, data = %v", err, data)
	}
}

func TestGetT(t *testing.T) {
	s := newTestRedis(t)
	defer s.Close()

	initPoolFromMiniredis(t, s)

	type user struct {
		Name string
		Age  int
	}

	u := user{Name: "Tom", Age: 18}
	if err := Set("user", u); err != nil {
		t.Fatalf("set error: %v", err)
	}

	got, err := GetT[user]("user")
	if err != nil {
		t.Fatalf("GetT error: %v", err)
	}
	if got.Name != "Tom" || got.Age != 18 {
		t.Fatalf("unexpected value: %+v", got)
	}
}

func TestGetInt(t *testing.T) {
	s := newTestRedis(t)
	defer s.Close()

	initPoolFromMiniredis(t, s)

	if err := Set("i1", 123); err != nil {
		t.Fatalf("set error: %v", err)
	}

	data, err := GetInt("i1")
	if data != 123 {
		t.Fatalf("expected 123 but get %v, err = %v", data, err)
	}
}

func TestGetBool(t *testing.T) {
	s := newTestRedis(t)
	defer s.Close()

	initPoolFromMiniredis(t, s)

	if err := Set("b1", true); err != nil {
		t.Fatalf("set error: %v", err)
	}

	data, err := GetBool("b1")
	if !data {
		t.Fatalf("expected true but get %v, err=%v", data, err)
	}
}

func TestHGetBool(t *testing.T) {
	s := newTestRedis(t)
	defer s.Close()

	initPoolFromMiniredis(t, s)

	if err := Hset("b1", "sub", false); err != nil {
		t.Fatalf("set error: %v", err)
	}

	data, err := HgetBool("b1", "sub")
	if err != nil {
		t.Fatalf("expected false but get %v, err=%v", data, err)
	}

	if err := Hset("b1", "sub1", true); err != nil {
		t.Fatalf("set error: %v", err)
	}
	data, err = HgetBool("b1", "sub1")
	if err != nil {
		t.Fatalf("expected true but get %v, err=%v", data, err)
	}
}

func TestHGetInt(t *testing.T) {
	s := newTestRedis(t)
	defer s.Close()

	initPoolFromMiniredis(t, s)

	if err := Hset("i1", "sub", 123); err != nil {
		t.Fatalf("set error: %v", err)
	}

	data, err := HgetInt("i1", "sub")
	if data != 123 {
		t.Fatalf("expected 123 but get %v, err = %v", data, err)
	}
}

func TestHGetT(t *testing.T) {
	s := newTestRedis(t)
	defer s.Close()

	initPoolFromMiniredis(t, s)

	type user struct {
		Name string
		Age  int
	}

	u := user{Name: "Tom", Age: 18}
	if err := Hset("user", "sub", u); err != nil {
		t.Fatalf("set error: %v", err)
	}

	got, err := HgetT[user]("user", "sub")
	if err != nil {
		t.Fatalf("GetT error: %v", err)
	}
	if got.Name != "Tom" || got.Age != 18 {
		t.Fatalf("unexpected value: %+v", got)
	}
}
