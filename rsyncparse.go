// Package rsyncparse implements a parser that extracts transfer details from
// rsync standard output output.
//
// See also https://github.com/stapelberg/rsyncprom for a wrapper program which
// pushes the extracted transfer details to a Prometheus push gateway.
//
// # Rsync Requirements
//
// Start rsync with --verbose (-v) or --stats to enable printing transfer
// totals.
//
// Do not use the --human-readable (-h) flag in your rsync invocation, otherwise
// rsyncprom cannot parse the output!
//
// Run rsync in the C.UTF-8 locale to prevent rsync from localizing decimal
// separators and fractional points in big numbers.
package rsyncparse

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// Stats contains all data found in rsync output.
type Stats struct {
	Found bool

	TotalWritten int64
	TotalRead    int64
	BytesPerSec  float64
	TotalSize    int64
}

// Speedup calculates the speed-up of using rsync over copying the data as-is.
func (p *Stats) Speedup() float64 {
	return float64(p.TotalSize / (p.TotalWritten + p.TotalRead))
}

var (
	statsTransferRe = regexp.MustCompile(`^sent ([0-9,]+) bytes  received ([0-9,]+) bytes  ([0-9,.]+) bytes/sec$`)

	statsSizeRe = regexp.MustCompile(`^total size is ([0-9,]+)  speedup is ([0-9,.]+)$`)
)

// Parse reads from the specified io.Reader and scans individual lines. rsync
// transfer totals are extracted when found, and returned in the Stats struct.
func Parse(r io.Reader) (*Stats, error) {
	p := &Stats{}
	scan := bufio.NewScanner(r)
	for scan.Scan() {
		line := scan.Text()
		// log.Printf("rsync output line: %q", line)
		if strings.HasPrefix(line, "sent ") {
			// e.g.:
			// sent 1,590 bytes  received 18 bytes  3,216.00 bytes/sec
			// total size is 1,188,046  speedup is 738.83
			matches := statsTransferRe.FindStringSubmatch(line)
			if len(matches) == 0 {
				return nil, fmt.Errorf("could not parse rsync 'sent' line; try starting rsync with LC_ALL=C.UTF-8")
			}

			p.Found = true
			// parse rsync do_big_num(int64 num) output
			// parse 1[,.]192[,.]097 bytes
			var err error
			p.TotalWritten, err = strconv.ParseInt(strings.ReplaceAll(matches[1], ",", ""), 0, 64)
			if err != nil {
				return nil, err
			}
			p.TotalRead, err = strconv.ParseInt(strings.ReplaceAll(matches[2], ",", ""), 0, 64)
			if err != nil {
				return nil, err
			}
			p.BytesPerSec, err = strconv.ParseFloat(strings.ReplaceAll(matches[3], ",", ""), 64)
			if err != nil {
				return nil, err
			}
		} else if strings.HasPrefix(line, "total size is ") {
			matches := statsSizeRe.FindStringSubmatch(line)
			p.Found = true
			var err error
			p.TotalSize, err = strconv.ParseInt(strings.ReplaceAll(matches[1], ",", ""), 0, 64)
			if err != nil {
				return nil, err
			}
		}
	}
	if err := scan.Err(); err != nil {
		if err == io.EOF {
			return p, nil
		}
		return nil, err
	}
	return p, nil
}
