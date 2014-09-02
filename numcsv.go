// package numcsv is for reading numeric csv files. It is more tolerant
// of errors in formatting than the standard go encoding/csv files so it may be
// of help with "from the wild" csv files who don't follow normal csv rules

package numcsv

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/gonum/matrix/mat64"
)

type Reader struct {
	Comma            string // field delimiter (set to ',' by NewReader)
	HeadingComma     string // delimiter for the headings. If "", set to the same value as Comma
	AllowEndingComma bool   // Allows there to be a single comma at the end of the field
	Comment          string // comment character for start of line
	FieldsPerRecord  int    // If preset, the number of expected fields. Set otherwise
	NoHeading        bool
	hasEndingComma   bool
	reader           io.Reader
	scanner          *bufio.Scanner
	lineRead         bool // signifier that some of the
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		Comma:   ",",
		reader:  r,
		scanner: bufio.NewScanner(r),
	}
}

var (
	ErrTrailingComma = errors.New("extra delimeter at end of line")
	ErrFieldCount    = errors.New("wrong number of fields in line")
)

// ReadHeading reads the string fields at the start, ignoring quotations if they are there
func (r *Reader) ReadHeading() (headings []string, err error) {
	// Read until prefix isn't comment
	var line string
	for b := r.scanner.Scan(); b; b = r.scanner.Scan() {
		line = r.scanner.Text()
		if line == "" {
			continue
		}
		if r.Comment != "" && strings.HasPrefix(line, r.Comment) {
			continue
		}
		break
	}
	if err := r.scanner.Err(); err != nil {
		return nil, err
	}
	comma := r.HeadingComma
	if comma == "" {
		comma = r.Comma
	}
	headings = strings.Split(line, r.Comma)

	// See if the last entry is blank
	if headings[len(headings)-1] == "" {
		if !r.AllowEndingComma {
			return nil, ErrTrailingComma
		}
		r.hasEndingComma = true
		headings = headings[:len(headings)-1]
	}
	if r.FieldsPerRecord != 0 && len(headings) != r.FieldsPerRecord {
		return nil, ErrFieldCount
	}
	r.FieldsPerRecord = len(headings)

	// Remove the quotations
	for i, str := range headings {
		str = strings.TrimSuffix(str, "\"")
		str = strings.TrimPrefix(str, "\"")
		headings[i] = str
	}
	r.lineRead = true
	return headings, nil
}

// Read reads a single record from the CSV. ReadHeading must be called first if
// there are headings. Returns nil if EOF reached.
func (r *Reader) Read() ([]float64, error) {
	b := r.scanner.Scan()
	if !b {
		return nil, r.scanner.Err()
	}
	line := r.scanner.Text()
	strs := strings.Split(line, r.Comma)
	if strs[len(strs)-1] == "" {
		strs = strs[:len(strs)-1]
	}

	if !r.lineRead {
		r.lineRead = true
		if r.FieldsPerRecord == 0 {
			r.FieldsPerRecord = len(strs)
		}
	}

	if len(strs) != r.FieldsPerRecord {
		return nil, ErrFieldCount
	}

	// Parse all of the data
	data := make([]float64, r.FieldsPerRecord)
	var err error
	for i, str := range strs {
		data[i], err = strconv.ParseFloat(str, 64)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

// ReadAll reads all of the numeric records from the CSV. ReadHeading must be called first if
// there are headings
func (r *Reader) ReadAll() (*mat64.Dense, error) {
	alldata := make([][]float64, 0)
	for {
		data, err := r.Read()
		if err != nil {
			return nil, err
		}
		if data == nil {
			break
		}
		alldata = append(alldata, data)
	}
	mat := mat64.NewDense(len(alldata), r.FieldsPerRecord, nil)
	for i, record := range alldata {
		for j, v := range record {
			mat.Set(i, j, v)
		}
	}
	return mat, nil
}
