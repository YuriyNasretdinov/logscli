package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const clearLine = "\033[2K\r"

// flags
var (
	// grep parameters
	beforeLines  = flag.Uint("B", 0, "How many lines of context to return before the found line")
	afterLines   = flag.Uint("A", 0, "How many lines of context to return after the found line")
	contextLines = flag.Uint("C", 0, "How many lines of context to return both before and after the found line")
	fixedString  = flag.String("F", "", "Fixed string search")
	regexString  = flag.String("E", "", "Regex string search")
	tailF        = flag.Bool("tailf", false, "Print incoming logs continiously")

	// log filtering parameters
	reverse = flag.Bool("reverse", true, "Whether or not to return results in reverse chronological order")
	before  = flag.String("before", "", "Date and time before which to display results (without milliseconds)")
	after   = flag.String("after", "", "Date and time after which to display results (without milliseconds)")

	// clickhouse parameters
	fields          = flag.String("fields", "review_body", "Comma-separated list of fields to return in the result in addition to timestamp (possible fields: marketplace,customer_id,review_id,product_id,product_parent,product_title,product_category,star_rating,helpful_votes,total_votes,vine,verified_purchase,review_headline,review_body,review_date)")
	textField       = flag.String("text-field", "review_body", "The name of the text field that is being matched")
	additionalWhere = flag.String("where", "", `Additional filters in WHERE (e.g. "vine='Y' AND star_rating>4")`)
	limit           = flag.Uint("limit", 0, "Limit the number of results (0 means no limit)")

	table  = flag.String("table", "amazon", "The name of the table to scan")
	chAddr = flag.String("ch-addr", "localhost:8123", "ClickHouse server address (HTTP endpoint)")

	debug = flag.Bool("debug", false, "Whether or not debug mode is enabled")
)

// Progress describes ClickHouse query progress result.
type Progress struct {
	ReadRows        int64 `json:"read_rows,string"`
	ReadBytes       int64 `json:"read_bytes,string"`
	WrittenRows     int64 `json:"written_rows,string"`
	WrittenBytes    int64 `json:"written_bytes,string"`
	TotalRowsToRead int64 `json:"total_rows_to_read,string"`
}

// Escape escapes string for MySQL. It should work for ClickHouse as well.
func Escape(txt string) string {
	var (
		esc string
		buf bytes.Buffer
	)
	last := 0
	for ii, bb := range txt {
		switch bb {
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\\':
			esc = `\\`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\032':
			esc = `\Z`
		default:
			continue
		}
		io.WriteString(&buf, txt[last:ii])
		io.WriteString(&buf, esc)
		last = ii + 1
	}
	io.WriteString(&buf, txt[last:])
	return buf.String()
}

func makeFilterConds() []string {
	var conds []string

	conds = append(conds, "1=1")

	if *additionalWhere != "" {
		conds = append(conds, "("+(*additionalWhere)+")")
	}

	if *fixedString != "" {
		conds = append(conds, `position(`+(*textField)+`, '`+Escape(*fixedString)+`') <> 0`)
	}

	if *regexString != "" {
		conds = append(conds, `match(`+(*textField)+`, '`+Escape(*regexString)+`') = 1`)
	}

	if *before != "" {
		conds = append(conds, `time < toDateTime('`+Escape(*before)+`')`)
	}

	if *after != "" {
		conds = append(conds, `time > toDateTime('`+Escape(*after)+`')`)
	}

	return conds
}

func printContextResults(date string, millis int, isBefore bool, numLines uint) error {
	start := time.Now()

	comparison := "<"
	desc := " DESC"

	if *reverse && isBefore || !isBefore && !*reverse {
		comparison = ">"
		desc = ""
	}

	query := fmt.Sprintf(`SELECT time,millis,%s FROM %s
		WHERE (time = '%s' AND millis %s %d) OR (time %s '%s')
		ORDER BY time%s, millis%s
		LIMIT %d
		SETTINGS max_threads=1
		FORMAT TabSeparatedRaw`,
		*fields, *table,
		date, comparison, millis, comparison, date,
		desc, desc,
		numLines)

	if *debug {
		fmt.Printf("Context query: %s\n", query)
	}

	u := url.Values{}
	u.Set("query", query)

	resp, err := http.Get("http://" + (*chAddr) + "/?" + u.Encode())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	allLines, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	res := strings.Split(strings.TrimSpace(string(allLines)), "\n")

	// reverse the result if context sorting does not match the desired
	// sorting
	if desc == "" && *reverse || desc != "" && !*reverse {
		for i := 0; i < len(res)/2; i++ {
			res[i], res[len(res)-i-1] = res[len(res)-i-1], res[i]
		}
	}

	if *debug {
		fmt.Printf("(context calculated for %s) ", time.Since(start))
	}

	_, _, err = printResults(bufio.NewReader(strings.NewReader(strings.Join(res, "\n")+"\n")), false)
	return err
}

func runMain() (lastDate string, lastMillis int, err error) {
	rand.Seed(time.Now().UnixNano())

	desc := " DESC"
	if !*reverse {
		desc = ""
	}

	limitPart := ""
	if *limit != 0 {
		limitPart = fmt.Sprintf("LIMIT %d", *limit)
	}

	query := `SELECT time,millis,` + (*fields) + `
		FROM ` + (*table) + `
		WHERE ` + strings.Join(makeFilterConds(), " AND ") + `
		ORDER BY time` + desc + `, millis` + desc + `
		` + limitPart + `
		FORMAT TabSeparatedRaw`

	if *debug {
		fmt.Printf("Executed query: %s\n", query)
	}

	u := url.Values{}
	u.Set("cancel_http_readonly_queries_on_client_close", "1")
	u.Set("send_progress_in_http_headers", "1")
	u.Set("query", query)

	conn, err := net.Dial("tcp", *chAddr)
	if err != nil {
		return "", 0, err
	}
	defer conn.Close()

	rd := bufio.NewReader(conn)
	wr := bufio.NewWriter(conn)
	if _, err := fmt.Fprintf(wr, "GET /?%s HTTP/1.0\n\n", u.Encode()); err != nil {
		return "", 0, err
	}
	if err := wr.Flush(); err != nil {
		return "", 0, err
	}

	start := time.Now()

	for {
		ln, err := rd.ReadString('\n')
		if err != nil {
			return "", 0, fmt.Errorf("unexpected error while reading headers: %v", err)
		}
		ln = strings.TrimSpace(ln)
		if ln == "" {
			break
		}

		if strings.HasPrefix(ln, "X-ClickHouse-Progress: ") {
			var p Progress
			data := strings.TrimPrefix(ln, "X-ClickHouse-Progress: ")
			if err := json.Unmarshal([]byte(data), &p); err != nil {
				return "", 0, fmt.Errorf("unmarshalling %q: %v", data, err)
			}

			read := float64(p.ReadBytes) / (1 << 30)
			readPerSec := float64(p.ReadBytes) / (float64(time.Since(start)) / float64(time.Second)) / (1 << 30)

			fmt.Fprintf(os.Stderr, clearLine+"Progress: %.0f%% (read %.2f GiB so far, %.2f GiB/sec)", float64(p.ReadRows)/float64(p.TotalRowsToRead)*100, read, readPerSec)
		}
	}

	fmt.Fprintf(os.Stderr, clearLine)

	return printResults(rd, true)
}

func printResults(rd *bufio.Reader, printContext bool) (lastDate string, lastMillis int, err error) {
	for {
		ln, err := rd.ReadString('\n')
		if err == io.EOF {
			return lastDate, lastMillis, nil
		} else if err != nil {
			return "", 0, err
		}

		parts := strings.SplitN(ln, "\t", 3)
		if len(parts) < 3 {
			if _, err := os.Stdout.WriteString(ln); err != nil {
				return "", 0, err
			}
			continue
		}

		date, millisStr, rest := parts[0], parts[1], parts[2]
		millis, _ := strconv.Atoi(millisStr)

		lastDate = date
		lastMillis = millis

		if printContext && (*beforeLines > 0) {
			if err := printContextResults(date, millis, true, *beforeLines); err != nil {
				return "", 0, err
			}
		}

		fmt.Printf("%s.%03d\t%s\n", date, millis, strings.TrimSpace(strings.ReplaceAll(rest, "\t", " ")))

		if printContext && (*afterLines > 0) {
			if err := printContextResults(date, millis, false, *afterLines); err != nil {
				return "", 0, err
			}
		}

		if printContext && (*beforeLines > 0 || *afterLines > 0) {
			fmt.Printf("---\n")
		}
	}

	return lastDate, lastMillis, nil
}

func main() {
	flag.Parse()

	if *contextLines != 0 {
		*beforeLines = *contextLines
		*afterLines = *contextLines
	}

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGPIPE)
	go func() {
		<-ch
		os.Exit(0)
	}()

	if *tailF {
		*reverse = false
		if *after == "" {
			*after = fmt.Sprint(time.Now().Add(-time.Minute).Unix())
		}

		for {
			lastDate, _, err := runMain()
			if err != nil {
				if strings.Contains(err.Error(), "broken pipe") {
					return
				}

				log.Fatalf("FATAL error: %v", err)
			}

			if lastDate != "" {
				*after = lastDate
			}

			time.Sleep(time.Second)
		}
	}

	if _, _, err := runMain(); err != nil {
		if strings.Contains(err.Error(), "broken pipe") {
			return
		}

		log.Fatalf("FATAL error: %v", err)
	}
}
