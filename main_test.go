package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseT0_IntSeconds(t *testing.T) {
	v, err := parseT0("1730000000")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !v.HasUnix || v.UnixSecond != 1730000000 {
		t.Fatalf("unexpected unix: %+v", v)
	}
	if !v.HasTime {
		t.Fatalf("expected HasTime")
	}
}

func TestParseT0_Datetime(t *testing.T) {
	v, err := parseT0("2025-12-25 12:00:00")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !v.HasTime || v.Time.IsZero() {
		t.Fatalf("unexpected time: %+v", v)
	}
}

func TestT0LiteralForType(t *testing.T) {
	v := t0Value{Time: time.Date(2025, 12, 25, 12, 0, 0, 0, time.Local), HasTime: true, UnixSecond: 1735128000, HasUnix: true}
	got, err := t0LiteralForType(v, "datetime")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "'2025-12-25 12:00:00'" {
		t.Fatalf("got %s", got)
	}
	got, err = t0LiteralForType(v, "bigint")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "1735128000" {
		t.Fatalf("got %s", got)
	}
}

func TestValidateSameColumns(t *testing.T) {
	if err := validateSameColumns([]string{"a", "b"}, []string{"a", "b"}); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if err := validateSameColumns([]string{"a", "b"}, []string{"a", "c"}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestBuildUnionViewSQL(t *testing.T) {
	sql := buildUnionViewSQL("business", "asset_log", "asset_log_old", "asset_log_new", []string{"id", "insertTime"}, "insertTime", "'2025-12-25 12:00:00'")
	if !strings.Contains(strings.ToLower(sql), "union all") {
		t.Fatalf("missing union all: %s", sql)
	}
	if !strings.Contains(sql, "where `insertTime` <= '2025-12-25 12:00:00'") {
		t.Fatalf("missing old filter: %s", sql)
	}
	if !strings.Contains(sql, "where `insertTime` > '2025-12-25 12:00:00'") {
		t.Fatalf("missing new filter: %s", sql)
	}
}

func TestSuffixPairingExample(t *testing.T) {
	oldSuffix := "_1"
	newSuffix := "_2"
	oldTable := "dns_log_1"
	newTable := "dns_log_2"

	oldBase := strings.TrimSuffix(oldTable, oldSuffix)
	newBase := strings.TrimSuffix(newTable, newSuffix)
	if oldBase != "dns_log" || newBase != "dns_log" {
		t.Fatalf("base mismatch: oldBase=%s newBase=%s", oldBase, newBase)
	}
}
