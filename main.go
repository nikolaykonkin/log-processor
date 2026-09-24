package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	numWorkers     = 3  // размер пула воркеров
	maxErrorsShown = 10 // сколько записей с ошибками выводить в консоль
)

func main() {
	// Проверка аргументов командной строки
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <logfile.csv>\n", os.Args[0])
		fmt.Println("Example: go run main.go processor.go testdata/logs.csv")
		os.Exit(1)
	}

	filename := os.Args[1]

	// Создаем контекст с возможностью отмены
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов прерывания (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n\nПолучен сигнал прерывания. Завершаем работу...")
		cancel()
	}()

	fmt.Println("Начинаем обработку логов...")
	startTime := time.Now()

	stats, errorEntries := runPipeline(ctx, filename, numWorkers)

	// Вывод результатов
	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("СТАТИСТИКА ОБРАБОТКИ ЛОГОВ")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("Общее количество запросов: %d\n", stats.TotalRequests)
	fmt.Printf("Количество ошибок (статус >= 400): %d\n", stats.ErrorCount)
	if stats.TotalRequests > 0 {
		fmt.Printf("Среднее время ответа: %.2f мс\n", stats.AverageRespTime())
	} else {
		fmt.Println("Среднее время ответа: N/A (нет данных)")
	}
	fmt.Printf("Суммарное время обработки: %v\n", time.Since(startTime))

	// Вывод топ IP адресов
	printTopIPs(stats.RequestsByIP, 5)

	// Вывод записей с ошибками
	printErrorEntries(errorEntries, maxErrorsShown)

	fmt.Println("\nОбработка завершена успешно!")
}

// runPipeline собирает и запускает весь pipeline: read -> process -> tee ->
// (filter | aggregate). Вынесен из main, чтобы интеграционный тест проходил
// через ту же сборку, что и боевой запуск: ошибка в проводке каналов (как
// фильтр перед статистикой) не видна модульным тестам отдельных стадий.
// Возвращает статистику по всем записям и список записей с ошибками.
func runPipeline(ctx context.Context, filename string, workers int) (*Statistics, []LogEntry) {
	// 1. Чтение логов из файла
	logEntries, readErrCh := readLogs(ctx, filename)

	// Дренаж канала ошибок стартует сразу после readLogs: пока читателя нет,
	// readLogs блокируется на второй же ошибке парсинга (буфер канала равен 1)
	// и останавливает весь pipeline. Цикл range заканчивается, когда readLogs
	// закрывает канал, поэтому ни одна ошибка не теряется.
	var errWg sync.WaitGroup
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range readErrCh {
			log.Printf("Ошибка при чтении файла: %v", err)
		}
	}()

	// 2. Параллельная обработка через worker pool
	processed := processLogs(ctx, logEntries, workers)

	// 3. Раздваиваем поток: статистика получает все записи, фильтр только
	// ошибки. Иначе TotalRequests равнялся бы ErrorCount.
	allForStats, allForFilter := tee(ctx, processed)

	// 4. Фильтрация ошибок (статус >= 400)
	filtered := filterLogs(ctx, allForFilter, 400)

	// filterLogs пишет в небуферизованный канал: без параллельного читателя
	// tee заблокируется на отправке в allForFilter, и calculateStats никогда
	// не получит записей. Поэтому отфильтрованные записи собираем в отдельной
	// горутине, а calculateStats ниже работает в текущей.
	var (
		collectWg    sync.WaitGroup
		errorEntries []LogEntry
	)
	collectWg.Add(1)
	go func() {
		defer collectWg.Done()
		for entry := range filtered {
			errorEntries = append(errorEntries, entry)
		}
	}()

	// 5. Подсчет статистики по всем записям
	stats := calculateStats(ctx, allForStats)

	// Ждем оба фоновых читателя: без этого срез errorEntries мог бы быть
	// прочитан до завершения сбора, а последние ошибки чтения не дошли бы до лога
	collectWg.Wait()
	errWg.Wait()

	return stats, errorEntries
}
