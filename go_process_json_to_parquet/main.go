package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// --- Data Structures (equivalent to your PyArrow Schema and Python dicts) ---

// Inventor struct maps to the nested 'inventor_list' items
type Inventor struct {
	InventorCity      string `json:"inventor_city" parquet:"name=inventor_city,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	InventorCountry   string `json:"inventor_country" parquet:"name=inventor_country,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	InventorNameFirst string `json:"inventor_name_first" parquet:"name=inventor_name_first,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	InventorNameLast  string `json:"inventor_name_last" parquet:"name=inventor_name_last,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	InventorState     string `json:"inventor_state" parquet:"name=inventor_state,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
}

// PatentRecord struct maps directly to your PyArrow schema
// Use `parquet:"name=..."` tags for Parquet column mapping
// Use `json:"..."` tags for JSON unmarshaling
type PatentRecord struct {
	ApplicationNumber  string     `json:"application_number" parquet:"name=application_number,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	PublicationNumber  string     `json:"publication_number" parquet:"name=publication_number,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	Title              string     `json:"title" parquet:"name=title,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	Decision           string     `json:"decision" parquet:"name=decision,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	DateProduced       string     `json:"date_produced" parquet:"name=date_produced,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	DatePublished      string     `json:"date_published" parquet:"name=date_published,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	MainCPCLabel       string     `json:"main_cpc_label" parquet:"name=main_cpc_label,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	CPCLabels          []string   `json:"cpc_labels" parquet:"name=cpc_labels,type=LIST,convertedtype=UTF8,valuetype=BYTE_ARRAY,encoding=PLAIN_DICTIONARY"`
	MainIPCRLabel      string     `json:"main_ipcr_label" parquet:"name=main_ipcr_label,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	IPCRLabels         []string   `json:"ipcr_labels" parquet:"name=ipcr_labels,type=LIST,convertedtype=UTF8,valuetype=BYTE_ARRAY,encoding=PLAIN_DICTIONARY"`
	PatentNumber       string     `json:"patent_number" parquet:"name=patent_number,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	FilingDate         string     `json:"filing_date" parquet:"name=filing_date,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	PatentIssueDate    string     `json:"patent_issue_date" parquet:"name=patent_issue_date,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	AbandonDate        string     `json:"abandon_date" parquet:"name=abandon_date,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	USPCClass          string     `json:"uspc_class" parquet:"name=uspc_class,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	USPCSubclass       string     `json:"uspc_subclass" parquet:"name=uspc_subclass,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	ExaminerID         string     `json:"examiner_id" parquet:"name=examiner_id,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	ExaminerNameLast   string     `json:"examiner_name_last" parquet:"name=examiner_name_last,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	ExaminerNameFirst  string     `json:"examiner_name_first" parquet:"name=examiner_name_first,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	ExaminerNameMiddle string     `json:"examiner_name_middle" parquet:"name=examiner_name_middle,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	InventorList       []Inventor `json:"inventor_list" parquet:"name=inventor_list,type=LIST"`
	Abstract           string     `json:"abstract" parquet:"name=abstract,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	Summary            string     `json:"summary" parquet:"name=summary,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	FullDescription    string     `json:"full_description" parquet:"name=full_description,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
}

// Global counters and mutexes for statistics
var (
	totalFilesScanned        int64
	processedRecordsCount    int64 // Count of records *after* filtering by decision status, before deduplication
	filteredOutCount         int64
	deduplicatedRecordsCount int64          // New counter for records written to Parquet
	wg                       sync.WaitGroup // For waiting on all processing goroutines to finish
	mu                       sync.Mutex     // For protecting shared counters/maps
)

// Allowed decision statuses
var allowedDecisionStatuses = map[string]struct{}{
	"ACCEPTED": {},
	"REJECTED": {},
}

// worker processes a single file, filters it, and sends valid records to the results channel.
func worker(id int, filepaths <-chan string, results chan<- PatentRecord) {
	defer wg.Done()
	// fmt.Printf("Worker %d started.\n", id) // Commented out to reduce console noise
	for filepath := range filepaths {
		// Increment total scanned count
		mu.Lock()
		totalFilesScanned++
		mu.Unlock()

		data, err := ioutil.ReadFile(filepath)
		if err != nil {
			fmt.Printf("Warning: Worker %d could not read %s: %v\n", id, filepath, err)
			mu.Lock()
			filteredOutCount++ // Count as filtered out due to read error
			mu.Unlock()
			continue
		}

		var record map[string]interface{}
		err = json.Unmarshal(data, &record)
		if err != nil {
			fmt.Printf("Warning: Worker %d could not parse JSON %s: %v\n", id, filepath, err)
			mu.Lock()
			filteredOutCount++ // Count as filtered out due to parse error
			mu.Unlock()
			continue
		}

		decisionStatus, ok := record["decision"].(string)
		if !ok || decisionStatus == "" || strings.HasPrefix(decisionStatus, "CONT-") || decisionStatus == "PENDING" {
			mu.Lock()
			filteredOutCount++
			mu.Unlock()
			continue
		}

		_, isAllowed := allowedDecisionStatuses[decisionStatus]
		if !isAllowed {
			mu.Lock()
			filteredOutCount++
			mu.Unlock()
			continue
		}

		// Re-marshal and unmarshal into the strict PatentRecord struct
		// This handles dropping fields not defined in PatentRecord struct.
		processedData, _ := json.Marshal(record)
		var patentRecord PatentRecord
		err = json.Unmarshal(processedData, &patentRecord)
		if err != nil {
			fmt.Printf("Warning: Worker %d could not unmarshal to PatentRecord %s: %v\n", id, filepath, err)
			mu.Lock()
			filteredOutCount++
			mu.Unlock()
			continue
		}

		results <- patentRecord // Send valid record to results channel
		mu.Lock()
		processedRecordsCount++ // This counts records *before* deduplication
		mu.Unlock()
	}
	// fmt.Printf("Worker %d finished.\n", id) // Commented out to reduce console noise
}

// collectFilePaths recursively finds all .json files in a directory.
func collectFilePaths(inputDir string) ([]string, error) {
	var filepaths []string
	err := filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".json") {
			filepaths = append(filepaths, path)
		}
		return nil
	})
	return filepaths, err
}

func main() {
	startTime := time.Now()

	inputDirectory := "hupd_2018_extracted/2018"
	// Ensure the output directory exists
	outputDir := filepath.Dir("hupd_2018_processed.parquet/output.parquet")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("Error creating output directory %s: %v\n", outputDir, err)
		return
	}
	outputParquetFile := "hupd_2018_processed.parquet/output.parquet"

	fmt.Printf("Scanning for JSON files in %s...\n", inputDirectory)
	jsonFilepaths, err := collectFilePaths(inputDirectory)
	if err != nil {
		fmt.Printf("Error scanning directory: %v\n", err)
		return
	}

	if len(jsonFilepaths) == 0 {
		fmt.Printf("No JSON files found in %s. Exiting.\n", inputDirectory)
		return
	}

	fmt.Printf("Found %d JSON files. Starting Go processing...\n", len(jsonFilepaths))

	numWorkers := runtime.NumCPU() // Use number of CPU cores as workers
	if numWorkers == 0 {
		numWorkers = 4 // Fallback if runtime.NumCPU returns 0 (unlikely)
	}
	fmt.Printf("Using %d worker goroutines.\n", numWorkers)

	// Create channels for tasks (filepaths) and results (PatentRecords)
	filepathsChan := make(chan string, numWorkers*2)     // Buffered channel for tasks
	resultsChan := make(chan PatentRecord, numWorkers*2) // Buffered channel for results

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1) // Increment WaitGroup counter for each worker
		go worker(i, filepathsChan, resultsChan)
	}

	// Start a goroutine to feed filepaths into the filepathsChan
	go func() {
		for _, fp := range jsonFilepaths {
			filepathsChan <- fp
		}
		close(filepathsChan) // Close the channel to signal workers no more tasks are coming
	}()

	// Set up Parquet writer
	fw, err := local.NewLocalFileWriter(outputParquetFile)
	if err != nil {
		fmt.Printf("Can't create local file writer: %v\n", err)
		return
	}
	defer fw.Close() // Ensure the file writer is closed when main exits

	// Create Parquet writer for PatentRecord struct
	pw, err := writer.NewParquetWriter(fw, new(PatentRecord), int64(numWorkers))
	if err != nil {
		fmt.Printf("Can't create parquet writer: %v\n", err)
		return
	}

	// Set Parquet properties (e.g., compression)
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Start a goroutine to collect results, deduplicate, and write to Parquet
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()

		seenApplicationNumbers := make(map[string]struct{}) // To track seen application numbers
		duplicatesFound := 0

		for record := range resultsChan {
			if _, seen := seenApplicationNumbers[record.ApplicationNumber]; seen {
				// This application number has already been processed and written
				duplicatesFound++
				continue // Skip writing this duplicate
			}

			// If not seen, mark as seen and write
			seenApplicationNumbers[record.ApplicationNumber] = struct{}{}
			if err = pw.Write(record); err != nil {
				fmt.Printf("Error writing record to parquet: %v\n", err)
			} else {
				mu.Lock()
				deduplicatedRecordsCount++ // Increment count for *actually written* records
				mu.Unlock()
			}
		}
		// Log the duplicates found during writing phase
		if duplicatesFound > 0 {
			fmt.Printf("Deduplication: Skipped %d duplicate records based on 'application_number'.\n", duplicatesFound)
		}

		if err = pw.WriteStop(); err != nil {
			fmt.Printf("Parquet writer stop error: %v\n", err)
		}
	}()

	wg.Wait()          // Wait for all worker goroutines to finish
	close(resultsChan) // Close the results channel to signal the writer goroutine
	writerWg.Wait()    // Wait for the Parquet writer goroutine to finish

	fmt.Println("\n--- Processing Summary ---")
	fmt.Printf("Total JSON files scanned: %d\n", totalFilesScanned)
	fmt.Printf("Total records processed by workers (before deduplication): %d\n", processedRecordsCount)
	fmt.Printf("Total records filtered out by worker logic (decision status, parsing errors): %d\n", filteredOutCount)
	fmt.Printf("Total records written to Parquet (after deduplication): %d\n", deduplicatedRecordsCount)
	// You can infer "duplicates removed" as: processedRecordsCount - deduplicatedRecordsCount

	fmt.Println("Processing complete. Data saved to Parquet.")

	endTime := time.Now()
	totalTime := endTime.Sub(startTime).Seconds()
	fmt.Printf("\nTotal script execution time: %.2f seconds\n", totalTime)
}
