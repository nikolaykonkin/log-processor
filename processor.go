package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// LogEntry представляет одну запись лога
type LogEntry struct {
	Timestamp    string // время в формате "2024-01-15 10:30:00"
	IP           string // IP адрес клиента
	Method       string // HTTP метод (GET, POST и т.д.)
	URL          string // путь запроса
	StatusCode   int    // HTTP статус код
	ResponseTime int    // время ответа в миллисекундах
}

// Statistics хранит агрегированную статистику
type Statistics struct {
	mu                sync.Mutex
	TotalRequests     int            // общее количество запросов (все записи, не только ошибки)
	ErrorCount        int            // количество ошибок (статус >= 400)
	RequestsByIP      map[string]int // количество запросов с каждого IP
	TotalResponseTime int            // суммарное время ответа (для среднего)
}

// AverageRespTime возвращает среднее время ответа в миллисекундах.
// Среднее вычисляется из накопленных сумм, а не хранится отдельным полем:
// так оно не может разойтись с TotalRequests и TotalResponseTime.
func (s *Statistics) AverageRespTime() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Без этой проверки деление на ноль дало бы NaN на пустом входе
	if s.TotalRequests == 0 {
		return 0
	}
	return float64(s.TotalResponseTime) / float64(s.TotalRequests)
}

// parseLogLine парсит строку CSV в структуру LogEntry
func parseLogLine(record []string) (LogEntry, error) {
	if len(record) < 6 {
		return LogEntry{}, fmt.Errorf("invalid CSV record: expected 6 fields, got %d", len(record))
	}

	statusCode, err := strconv.Atoi(strings.TrimSpace(record[4]))
	if err != nil {
		return LogEntry{}, fmt.Errorf("invalid status code: %v", err)
	}

	responseTime, err := strconv.Atoi(strings.TrimSpace(record[5]))
	if err != nil {
		return LogEntry{}, fmt.Errorf("invalid response time: %v", err)
	}

	return LogEntry{
		Timestamp:    strings.TrimSpace(record[0]),
		IP:           strings.TrimSpace(record[1]),
		Method:       strings.TrimSpace(record[2]),
		URL:          strings.TrimSpace(record[3]),
		StatusCode:   statusCode,
		ResponseTime: responseTime,
	}, nil
}

// readLogs читает CSV файл и отправляет записи в выходной канал.
// Канал ошибок закрывается вместе с выходным, поэтому вызывающая сторона
// обязана читать его до закрытия: иначе readLogs встанет на отправке
// второй ошибки и остановит весь pipeline.
func readLogs(ctx context.Context, filename string) (<-chan LogEntry, <-chan error) {
	out := make(chan LogEntry)
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)

		file, err := os.Open(filename)
		if err != nil {
			errCh <- fmt.Errorf("failed to open file: %v", err)
			return
		}
		defer file.Close()

		reader := csv.NewReader(bufio.NewReader(file))
		reader.FieldsPerRecord = -1 // разрешаем переменное количество полей

		// Пропускаем заголовок
		_, err = reader.Read()
		if err != nil {
			errCh <- fmt.Errorf("failed to read header: %v", err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}

			record, err := reader.Read()
			if err == io.EOF {
				return
			}
			if err != nil {
				errCh <- fmt.Errorf("CSV read error: %v", err)
				continue
			}

			entry, err := parseLogLine(record)
			if err != nil {
				errCh <- fmt.Errorf("parse error: %v", err)
				continue
			}

			select {
			case out <- entry:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return out, errCh
}

// processLogs создает пул воркеров для параллельной обработки
func processLogs(ctx context.Context, input <-chan LogEntry, numWorkers int) <-chan LogEntry {
	out := make(chan LogEntry)
	var wg sync.WaitGroup

	// Запускаем воркеров
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case entry, ok := <-input:
					if !ok {
						return
					}
					// Здесь можно добавить дополнительную обработку
					// Например, нормализация URL или валидация
					select {
					case out <- entry:
					case <-ctx.Done():
						return
					}
				}
			}
		}(i)
	}

	// Закрываем выходной канал после завершения всех воркеров
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// tee раздваивает поток: каждая запись из input попадает в оба выходных канала.
// Нужен потому, что статистике требуются все записи, а фильтру только ошибки.
// Если поставить фильтр перед статистикой, TotalRequests совпадёт с ErrorCount.
func tee(ctx context.Context, input <-chan LogEntry) (<-chan LogEntry, <-chan LogEntry) {
	out1 := make(chan LogEntry)
	out2 := make(chan LogEntry)

	go func() {
		defer close(out1)
		defer close(out2)

		for {
			select {
			case <-ctx.Done():
				return
			case entry, ok := <-input:
				if !ok {
					return
				}

				// Отправляем в оба канала в том порядке, в каком готов получатель.
				// Последовательные out1 <- entry; out2 <- entry заставили бы более
				// медленного читателя тормозить того, кто ждёт первым. После успешной
				// отправки копия канала обнуляется: отправка в nil-канал в select
				// никогда не выбирается, поэтому запись не уйдёт в один канал дважды.
				o1, o2 := out1, out2
				for i := 0; i < 2; i++ {
					select {
					case o1 <- entry:
						o1 = nil
					case o2 <- entry:
						o2 = nil
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return out1, out2
}

// filterLogs фильтрует записи по минимальному статус-коду
func filterLogs(ctx context.Context, input <-chan LogEntry, minStatus int) <-chan LogEntry {
	out := make(chan LogEntry)

	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case entry, ok := <-input:
				if !ok {
					return
				}
				if entry.StatusCode >= minStatus {
					select {
					case out <- entry:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return out
}

// calculateStats подсчитывает статистику из входного канала.
// Считает все записи, которые получила: за фильтрацию отвечает вызывающая
// сторона. Работает в горутине вызывающего: отдельная горутина с немедленным
// Wait ничего бы не распараллелила.
// ВАЖНО: возвращает УКАЗАТЕЛЬ на Statistics, чтобы не копировать мьютекс
func calculateStats(ctx context.Context, input <-chan LogEntry) *Statistics {
	stats := &Statistics{
		RequestsByIP: make(map[string]int),
	}

	for {
		select {
		case <-ctx.Done():
			return stats
		case entry, ok := <-input:
			if !ok {
				return stats
			}
			stats.mu.Lock()
			stats.TotalRequests++
			stats.TotalResponseTime += entry.ResponseTime
			if entry.StatusCode >= 400 {
				stats.ErrorCount++
			}
			stats.RequestsByIP[entry.IP]++
			stats.mu.Unlock()
		}
	}
}

// printTopIPs выводит топ N IP адресов по активности
func printTopIPs(requestsByIP map[string]int, n int) {
	type ipCount struct {
		IP    string
		Count int
	}

	ips := make([]ipCount, 0, len(requestsByIP))
	for ip, count := range requestsByIP {
		ips = append(ips, ipCount{ip, count})
	}

	sort.Slice(ips, func(i, j int) bool {
		return ips[i].Count > ips[j].Count
	})

	limit := min(n, len(ips))

	fmt.Println("\nТоп IP адресов по активности:")
	for i := 0; i < limit; i++ {
		fmt.Printf("  %d. %s — %d запросов\n", i+1, ips[i].IP, ips[i].Count)
	}
}

// printErrorEntries выводит первые N записей с ошибками.
// Воркеры пула отдают записи в произвольном порядке, поэтому перед выводом
// сортируем по времени: иначе вывод менялся бы от запуска к запуску.
// Лимит нужен, чтобы на большом логе не залить консоль тысячами строк.
func printErrorEntries(entries []LogEntry, n int) {
	if len(entries) == 0 {
		return
	}

	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Timestamp < entries[j].Timestamp
	})

	limit := min(n, len(entries))

	fmt.Println("\nЗаписи с ошибками (статус >= 400):")
	for i := 0; i < limit; i++ {
		e := entries[i]
		fmt.Printf("  %s  %s  %s %s  -> %d\n", e.Timestamp, e.IP, e.Method, e.URL, e.StatusCode)
	}
	if len(entries) > limit {
		fmt.Printf("  ... и ещё %d\n", len(entries)-limit)
	}
}
