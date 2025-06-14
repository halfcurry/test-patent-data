package main

import (
	"encoding/json"
	"flag" // Import the flag package
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

// --- Data Structures (unchanged) ---

// Inventor struct maps to the nested 'inventor_list' items
type Inventor struct {
	InventorCity      string `json:"inventor_city" parquet:"name=inventor_city,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	InventorCountry   string `json:"inventor_country" parquet:"name=inventor_country,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	InventorNameFirst string `json:"inventor_name_first" parquet:"name=inventor_name_first,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	InventorNameLast  string `json:"inventor_name_last" parquet:"name=inventor_name_last,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
	InventorState     string `json:"inventor_state" parquet:"name=inventor_state,type=BYTE_ARRAY,convertedtype=UTF8,encoding=PLAIN_DICTIONARY"`
}

// PatentRecord struct maps directly to your PyArrow schema
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
	processedRecordsCount    int64
	filteredOutCount         int64
	deduplicatedRecordsCount int64
	wg                       sync.WaitGroup
	mu                       sync.Mutex
)

// Allowed decision statuses
var allowedDecisionStatuses = map[string]struct{}{
	"ACCEPTED": {},
	"REJECTED": {},
}

// worker processes a single file, filters it, and sends valid records to the results channel.
func worker(id int, filepaths <-chan string, results chan<- PatentRecord) {
	defer wg.Done()
	for filepath := range filepaths {
		mu.Lock()
		totalFilesScanned++
		mu.Unlock()

		data, err := ioutil.ReadFile(filepath)
		if err != nil {
			fmt.Printf("Warning: Worker %d could not read %s: %v\n", id, filepath, err)
			mu.Lock()
			filteredOutCount++
			mu.Unlock()
			continue
		}

		var record map[string]interface{}
		err = json.Unmarshal(data, &record)
		if err != nil {
			fmt.Printf("Warning: Worker %d could not parse JSON %s: %v\n", id, filepath, err)
			mu.Lock()
			filteredOutCount++
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

		results <- patentRecord
		mu.Lock()
		processedRecordsCount++
		mu.Unlock()
	}
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

	// --- Command-line argument parsing ---
	var year string
	flag.StringVar(&year, "year", "2018", "The year of the HUPd dataset to process (e.g., 2018, 2019)")
	flag.Parse()

	// Construct input and output paths based on the year
	// Assumes the structure created by the Python downloader: hupd_extracted/<year>/
	inputDirectory := filepath.Join("hupd_extracted", year)
	outputParquetFile := filepath.Join("hupd_processed", fmt.Sprintf("%s.parquet", year), "output.parquet")

	// --- End command-line argument handling ---

	// Ensure the output directory for the single file exists
	outputDir := filepath.Dir(outputParquetFile)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("Error creating output directory %s: %v\n", outputDir, err)
		return
	}

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

	numWorkers := runtime.NumCPU()
	if numWorkers == 0 {
		numWorkers = 4
	}
	fmt.Printf("Using %d worker goroutines.\n", numWorkers)

	filepathsChan := make(chan string, numWorkers*2)
	resultsChan := make(chan PatentRecord, numWorkers*2)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(i, filepathsChan, resultsChan)
	}

	go func() {
		for _, fp := range jsonFilepaths {
			filepathsChan <- fp
		}
		close(filepathsChan)
	}()

	// --- Single Parquet Writer Management ---
	fw, err := local.NewLocalFileWriter(outputParquetFile)
	if err != nil {
		fmt.Printf("Can't create local file writer: %v\n", err)
		return
	}
	defer fw.Close() // Ensure the file writer is closed when main exits

	// Optimize: Set a good row group size for the single output file
	const desiredRowGroupRows int64 = 100000 // A good starting point for row group size in rows

	pw, err := writer.NewParquetWriter(fw, new(PatentRecord), desiredRowGroupRows)
	if err != nil {
		fmt.Printf("Can't create parquet writer: %v\n", err)
		return
	}

	pw.CompressionType = parquet.CompressionCodec_SNAPPY // Keep SNAPPY compression

	// Start a goroutine to collect results, deduplicate, and write to Parquet
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()

		seenApplicationNumbers := make(map[string]struct{})
		duplicatesFound := 0

		for record := range resultsChan {
			if _, seen := seenApplicationNumbers[record.ApplicationNumber]; seen {
				duplicatesFound++
				continue
			}
			seenApplicationNumbers[record.ApplicationNumber] = struct{}{}

			if err = pw.Write(record); err != nil {
				fmt.Printf("Error writing record to parquet: %v\n", err)
			} else {
				mu.Lock()
				deduplicatedRecordsCount++
				mu.Unlock()
			}
		}

		if duplicatesFound > 0 {
			fmt.Printf("Deduplication: Skipped %d duplicate records based on 'application_number'.\n", duplicatesFound)
		}

		if err = pw.WriteStop(); err != nil {
			fmt.Printf("Parquet writer stop error: %v\n", err)
		}
	}()

	wg.Wait()
	close(resultsChan)
	writerWg.Wait()

	fmt.Println("\n--- Processing Summary ---")
	fmt.Printf("Total JSON files scanned: %d\n", totalFilesScanned)
	fmt.Printf("Total records processed by workers (before deduplication): %d\n", processedRecordsCount)
	fmt.Printf("Total records filtered out by worker logic (decision status, parsing errors): %d\n", filteredOutCount)
	fmt.Printf("Total records written to Parquet (after deduplication): %d\n", deduplicatedRecordsCount)

	fmt.Println("Processing complete. Data saved to Parquet.")

	endTime := time.Now()
	totalTime := endTime.Sub(startTime).Seconds()
	fmt.Printf("\nTotal script execution time: %.2f seconds\n", totalTime)
}
