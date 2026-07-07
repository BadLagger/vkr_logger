package models

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"logger/utils"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"math"
)

// DataPoint структура данных от коллектора
type DataPoint struct {
	Timestamp int64          `json:"timestamp"`
	Values    map[string]any `json:"values"`
}

// CSVLogger основной логгер
type CSVLogger struct {
	log           *utils.Logger
	cfg           *Config
	currentFile   *os.File
	csvWriter     *csv.Writer
	currentCount  int
	fileMutex     sync.Mutex
	running       bool
	runningMutex  sync.RWMutex
	stopChan      chan bool
	collectorConn net.Conn
	controlServer net.Listener
	dataChan      chan DataPoint
	wg            sync.WaitGroup
	mu            sync.Mutex
}

func NewCSVLogger(cfg *Config) (*CSVLogger, error) {

	if err := os.MkdirAll(cfg.LogDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log dir: %v", err)
	}

	return &CSVLogger{
		log:      utils.GlobalLogger(),
		cfg:      cfg,
		running:  false,
		stopChan: make(chan bool),
		dataChan: make(chan DataPoint, 1000),
	}, nil
}

func (l *CSVLogger) getCSVHeaders(point DataPoint) []string {
	headers := make([]string, 0, len(point.Values)+1)
	headers = append(headers, "timestamp")

	// Сортируем ключи для консистентности
	keys := make([]string, 0, len(point.Values))
	for k := range point.Values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	headers = append(headers, keys...)
	return headers
}

func (l *CSVLogger) valueToString(val any) string {
	if val == nil {
		return "NaN"
	}

	switch v:=val.(type) {
	case float64:
		if math.IsNaN(v) {
			return "NaN"
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		return v;
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (l *CSVLogger) writeDataPoint(writer *csv.Writer, point DataPoint, headers []string) error {
	record := make([]string, len(headers))
	record[0] = strconv.FormatInt(point.Timestamp, 10)

	for i, header := range headers[1:] {
		if val, ok := point.Values[header]; ok {
			record[i+1] = l.valueToString(val)
		} else {
			record[i+1] = "NaN"
		}
	}

	return writer.Write(record)
}

func (l *CSVLogger) rotateLog() error {
	// Закрываем текущий файл
	if l.csvWriter != nil {
		l.csvWriter.Flush()
	}
	if l.currentFile != nil {
		l.currentFile.Close()
	}

	// Создаем новый файл с timestamp в имени
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join(l.cfg.LogDir, fmt.Sprintf("metrics_%s.csv", timestamp))

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create log file: %v", err)
	}

	l.currentFile = file
	l.csvWriter = csv.NewWriter(file)

	// Устанавливаем разделитель
	if l.cfg.CSVDelimiter != "" {
		// CSV writer использует только запятую, поэтому для других разделителей
		// нужно использовать альтернативный подход
		if l.cfg.CSVDelimiter != "," {
			// Перенастраиваем writer для использования другого разделителя
			l.csvWriter.Comma = rune(l.cfg.CSVDelimiter[0])
		}
	}

	l.currentCount = 0

	return nil
}

func (l *CSVLogger) writePoint(point DataPoint) error {
	l.fileMutex.Lock()
	defer l.fileMutex.Unlock()

	// Проверяем нужно ли создать новый файл
	if l.currentFile == nil || (l.cfg.Rotation.Enabled && l.currentCount >= l.cfg.Rotation.MaxRecordsPerFile) {
		if err := l.rotateLog(); err != nil {
			return err
		}

		// Записываем заголовки в новый файл
		headers := l.getCSVHeaders(point)
		if err := l.csvWriter.Write(headers); err != nil {
			return err
		}
	}

	// Записываем данные
	headers := l.getCSVHeaders(point)
	if err := l.writeDataPoint(l.csvWriter, point, headers); err != nil {
		return err
	}

	l.csvWriter.Flush()
	l.currentCount++

	// Проверяем ротацию файлов
	l.rotateFilesIfNeeded()

	return nil
}

func (l *CSVLogger) rotateFilesIfNeeded() {
	if !l.cfg.Rotation.Enabled {
		return
	}

	files, err := filepath.Glob(filepath.Join(l.cfg.LogDir, "metrics_*.csv"))
	if err != nil {
		return
	}

	if len(files) <= l.cfg.Rotation.MaxFilesCount {
		return
	}

	// Сортируем файлы по имени (которое содержит timestamp)
	sort.Strings(files)

	// Удаляем самые старые
	toDelete := len(files) - l.cfg.Rotation.MaxFilesCount
	for i := 0; i < toDelete; i++ {
		os.Remove(files[i])
	}
}

func (l *CSVLogger) subscribeToCollector() error {
	l.log.Info("Try to subscribe: %s", l.cfg.CollectorSocket)
	conn, err := net.Dial("unix", l.cfg.CollectorSocket)
	if err != nil {
		return fmt.Errorf("failed to connect to collector: %v", err)
	}

	// Отправляем команду подписки
	_, err = conn.Write([]byte("SUBSCRIBE\n"))
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to subscribe: %v", err)
	}

	l.collectorConn = conn
	return nil
}

func (l *CSVLogger) listenForData() {
	defer l.wg.Done()

	decoder := json.NewDecoder(l.collectorConn)

	for {
		var point DataPoint
		if err := decoder.Decode(&point); err != nil {
			if l.running {
				fmt.Printf("Error reading data: %v\n", err)
			}
			return
		}

		// Отправляем точку в канал для обработки
		select {
		case l.dataChan <- point:
		case <-l.stopChan:
			return
		}
	}
}

func (l *CSVLogger) processData() {
	defer l.wg.Done()

	for {
		select {
		case point := <-l.dataChan:
			if err := l.writePoint(point); err != nil {
				fmt.Printf("Failed to write point: %v\n", err)
			}
		case <-l.stopChan:
			return
		}
	}
}

func (l *CSVLogger) handleControlCommand(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	command := strings.TrimSpace(string(buf[:n]))

	switch command {
	case "ENABLE":
		l.Enable()
		conn.Write([]byte("OK: Logging enabled\n"))

	case "DISABLE":
		l.Disable()
		conn.Write([]byte("OK: Logging disabled\n"))

	case "STATUS":
		status := "disabled"
		if l.IsEnabled() {
			status = "enabled"
		}
		conn.Write([]byte(fmt.Sprintf("Logging is %s\n", status)))

	case "ROTATE":
		l.forceRotate()
		conn.Write([]byte("OK: Manual rotation triggered\n"))

	default:
		conn.Write([]byte("Unknown command. Available: ENABLE, DISABLE, STATUS, ROTATE\n"))
	}
}

func (l *CSVLogger) Enable() {
	l.runningMutex.Lock()
	defer l.runningMutex.Unlock()

	l.log.Debug("Try Enable!")
	if l.running {
		l.log.Debug("Already Enabled!")
		return
	}

	l.running = true
	l.stopChan = make(chan bool)

	// Переподключаемся к коллектору
	if err := l.subscribeToCollector(); err != nil {
		fmt.Printf("Failed to subscribe: %v\n", err)
		l.running = false
		return
	}

	// Запускаем обработчики
	l.wg.Add(2)
	go l.listenForData()
	go l.processData()

}

func (l *CSVLogger) Disable() {
	l.runningMutex.Lock()
	defer l.runningMutex.Unlock()

	if !l.running {
		return
	}

	l.running = false
	close(l.stopChan)

	// Закрываем соединение с коллектором
	if l.collectorConn != nil {
		l.collectorConn.Close()
	}

	// Ждем завершения обработчиков
	l.wg.Wait()

	// Закрываем текущий файл
	l.fileMutex.Lock()
	if l.csvWriter != nil {
		l.csvWriter.Flush()
	}
	if l.currentFile != nil {
		l.currentFile.Close()
		l.currentFile = nil
	}
	l.fileMutex.Unlock()

	fmt.Println("Logging disabled")
}

func (l *CSVLogger) IsEnabled() bool {
	l.runningMutex.RLock()
	defer l.runningMutex.RUnlock()
	return l.running
}

func (l *CSVLogger) forceRotate() {
	l.fileMutex.Lock()
	defer l.fileMutex.Unlock()

	if l.currentFile != nil {
		l.currentCount = l.cfg.Rotation.MaxRecordsPerFile // Триггерит ротацию
	}
}

func (l *CSVLogger) startControlServer() error {
	
	_, err := os.Stat(l.cfg.ControlSocket)
	if err == nil {
		err = os.Remove(l.cfg.ControlSocket)
		if err != nil {
			return fmt.Errorf("can't delete old socket %s (%v)", l.cfg.ControlSocket, err)
		}
	}

	dirpath := filepath.Dir(l.cfg.ControlSocket)
	_, err = os.Stat(dirpath)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(dirpath, 0755)
		if err != nil {
			return fmt.Errorf("can't create dirpath %s (%v)", dirpath, err)
		}
	} 

	listener, err := net.Listen("unix", l.cfg.ControlSocket)
	if err != nil {
		return fmt.Errorf("failed to start control server: %v", err)
	}

	l.controlServer = listener

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go l.handleControlCommand(conn)
		}
	}()

	return nil
}

func (l *CSVLogger) Start() error {
	// Запускаем сервер управления
	if err := l.startControlServer(); err != nil {
		return err
	}

	// Если логирование включено по умолчанию
	if l.cfg.Rotation.Enabled {
		l.log.Debug("Logger aaa enabled!")
		l.Enable()
	}

	fmt.Printf("Logger started. Control socket: %s\n", l.cfg.ControlSocket)

	return nil
}

func (l *CSVLogger) Stop() {
	l.Disable()

	if l.controlServer != nil {
		l.controlServer.Close()
	}

	// Удаляем управляющий сокет
	os.Remove(l.cfg.ControlSocket)
}
