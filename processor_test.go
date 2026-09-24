package main

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestParseLogLine(t *testing.T) {
	record := []string{"2024-01-15 10:30:00", "192.168.1.100", "GET", "/api/users", "404", "150"}
	entry, err := parseLogLine(record)
	if err != nil {
		t.Fatalf("parseLogLine failed: %v", err)
	}

	if entry.IP != "192.168.1.100" {
		t.Errorf("Expected IP 192.168.1.100, got %s", entry.IP)
	}
	if entry.StatusCode != 404 {
		t.Errorf("Expected StatusCode 404, got %d", entry.StatusCode)
	}
	if entry.ResponseTime != 150 {
		t.Errorf("Expected ResponseTime 150, got %d", entry.ResponseTime)
	}
}

func TestFilterLogs(t *testing.T) {
	ctx := context.Background()
	input := make(chan LogEntry)
	output := filterLogs(ctx, input, 400)

	go func() {
		input <- LogEntry{StatusCode: 200}
		input <- LogEntry{StatusCode: 404}
		input <- LogEntry{StatusCode: 500}
		input <- LogEntry{StatusCode: 301}
		close(input)
	}()

	var results []LogEntry
	for entry := range output {
		results = append(results, entry)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 filtered entries, got %d", len(results))
	}
}

func TestCalculateStats(t *testing.T) {
	ctx := context.Background()
	input := make(chan LogEntry)

	go func() {
		input <- LogEntry{IP: "1.2.3.4", StatusCode: 200, ResponseTime: 100}
		input <- LogEntry{IP: "1.2.3.4", StatusCode: 404, ResponseTime: 50}
		input <- LogEntry{IP: "5.6.7.8", StatusCode: 500, ResponseTime: 200}
		close(input)
	}()

	stats := calculateStats(ctx, input)

	if stats.TotalRequests != 3 {
		t.Errorf("Expected 3 total requests, got %d", stats.TotalRequests)
	}
	if stats.ErrorCount != 2 {
		t.Errorf("Expected 2 errors, got %d", stats.ErrorCount)
	}
	if stats.RequestsByIP["1.2.3.4"] != 2 {
		t.Errorf("Expected 2 requests from 1.2.3.4, got %d", stats.RequestsByIP["1.2.3.4"])
	}
}

// waitOrFail ждет WaitGroup с таймаутом. Без него ошибка в проводке каналов
// превратилась бы в зависший тест, а не в понятное сообщение о падении.
func waitOrFail(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("test hung: goroutines did not finish in time")
	}
}

// Регрессия на замечание преподавателя: calculateStats обязан считать все
// полученные записи, а не только ошибки. Успешных записей здесь больше, чем
// ошибок, чтобы подмена "все записи" на "только ошибки" была заметна.
func TestCalculateStats_CountsAllEntries(t *testing.T) {
	input := make(chan LogEntry)

	go func() {
		defer close(input)
		input <- LogEntry{IP: "1.1.1.1", StatusCode: 200, ResponseTime: 10}
		input <- LogEntry{IP: "1.1.1.1", StatusCode: 204, ResponseTime: 20}
		input <- LogEntry{IP: "2.2.2.2", StatusCode: 301, ResponseTime: 30}
		input <- LogEntry{IP: "2.2.2.2", StatusCode: 404, ResponseTime: 40}
		input <- LogEntry{IP: "3.3.3.3", StatusCode: 503, ResponseTime: 50}
	}()

	stats := calculateStats(context.Background(), input)

	if stats.TotalRequests != 5 {
		t.Errorf("Expected 5 total requests, got %d", stats.TotalRequests)
	}
	if stats.ErrorCount != 2 {
		t.Errorf("Expected 2 errors, got %d", stats.ErrorCount)
	}
}

func TestStatistics_AverageRespTime(t *testing.T) {
	empty := &Statistics{}
	if got := empty.AverageRespTime(); got != 0 {
		t.Errorf("Expected 0 for empty statistics, got %v", got)
	}

	s := &Statistics{TotalRequests: 4, TotalResponseTime: 10}
	if got := s.AverageRespTime(); got != 2.5 {
		t.Errorf("Expected 2.5, got %v", got)
	}
}

// tee должен отдавать обоим получателям одну и ту же последовательность.
// Оба выхода читаются параллельно: tee отправляет запись в оба канала, и
// последовательное чтение в тесте заблокировало бы его само по себе.
func TestTee(t *testing.T) {
	want := []LogEntry{
		{IP: "1.1.1.1", StatusCode: 200, ResponseTime: 10},
		{IP: "2.2.2.2", StatusCode: 404, ResponseTime: 20},
		{IP: "3.3.3.3", StatusCode: 500, ResponseTime: 30},
		{IP: "4.4.4.4", StatusCode: 301, ResponseTime: 40},
	}

	input := make(chan LogEntry)
	out1, out2 := tee(context.Background(), input)

	go func() {
		defer close(input)
		for _, entry := range want {
			input <- entry
		}
	}()

	var got1, got2 []LogEntry
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for entry := range out1 {
			got1 = append(got1, entry)
		}
	}()
	go func() {
		defer wg.Done()
		for entry := range out2 {
			got2 = append(got2, entry)
		}
	}()
	waitOrFail(t, &wg, 5*time.Second)

	if !reflect.DeepEqual(got1, want) {
		t.Errorf("first output: expected %v, got %v", want, got1)
	}
	if !reflect.DeepEqual(got2, want) {
		t.Errorf("second output: expected %v, got %v", want, got2)
	}
}

// Регрессия на замечание про errCh: в файле три битые строки, а буфер канала
// равен 1. Без параллельного читателя readLogs завис бы на второй ошибке.
func TestReadLogs_DrainsErrCh(t *testing.T) {
	content := "timestamp,ip,method,url,status,response_time\n" +
		"2024-01-15 10:30:00,10.0.0.1,GET,/ok,200,10\n" +
		"2024-01-15 10:30:01,10.0.0.2,GET,/bad-status,abc,20\n" +
		"2024-01-15 10:30:02,10.0.0.3,GET,/short\n" +
		"2024-01-15 10:30:03,10.0.0.4,GET,/bad-time,500,xyz\n" +
		"2024-01-15 10:30:04,10.0.0.5,GET,/ok-too,404,30\n"

	path := filepath.Join(t.TempDir(), "broken.csv")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	entries, errCh := readLogs(context.Background(), path)

	var (
		errs       []error
		validCount int
		wg         sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for err := range errCh {
			errs = append(errs, err)
		}
	}()
	go func() {
		defer wg.Done()
		for range entries {
			validCount++
		}
	}()
	waitOrFail(t, &wg, 5*time.Second)

	if len(errs) != 3 {
		t.Errorf("Expected 3 errors to reach the reader, got %d: %v", len(errs), errs)
	}
	if validCount != 2 {
		t.Errorf("Expected 2 valid entries, got %d", validCount)
	}
}

// Интеграционный тест: проходит через ту же сборку pipeline, что и main.
// В testdata/logs.csv 15 записей, из них 8 со статусом >= 400.
func TestPipeline_TotalRequestsCountsAllEntries(t *testing.T) {
	var (
		stats        *Statistics
		errorEntries []LogEntry
		wg           sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		stats, errorEntries = runPipeline(context.Background(), "testdata/logs.csv", 3)
	}()
	waitOrFail(t, &wg, 5*time.Second)

	if stats.TotalRequests != 15 {
		t.Errorf("Expected 15 total requests, got %d", stats.TotalRequests)
	}
	if stats.ErrorCount != 8 {
		t.Errorf("Expected 8 errors, got %d", stats.ErrorCount)
	}
	// Сумма времен ответа по всем 15 записям: подтверждает, что в статистику
	// дошли и успешные запросы, а не только ошибки
	if stats.TotalResponseTime != 9980 {
		t.Errorf("Expected total response time 9980, got %d", stats.TotalResponseTime)
	}
	if len(errorEntries) != 8 {
		t.Errorf("Expected 8 collected error entries, got %d", len(errorEntries))
	}
	for _, entry := range errorEntries {
		if entry.StatusCode < 400 {
			t.Errorf("Non-error entry leaked into filtered stream: %+v", entry)
		}
	}
}
