package main

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/360EntSecGroup-Skylar/excelize"
)

type Cell struct {
	bin                  int
	rnn                  int
	taxpayerOrganization int
	ownerName            int
	ownerIin             int
	ownerRnn             int
	orderNo              int
	orderDate            int
	violationType        int
}

type Violation struct {
	bin                  string
	rnn                  string
	taxpayerOrganization string
	ownerName            string
	ownerIin             string
	ownerRnn             string
	orderNo              string
	orderDate            string
	violationType        string
}

func (p Violation) toString() string {
	var id string

	if p.bin != "" {
		id = "\"_id\": \"" + p.bin + "\""
	}
	return "{ \"index\": {" + id + "}} \n" +
		"{ \"bin\":\"" + p.bin + "\"" +
		", \"rnn\":\"" + p.rnn + "\"" +
		", \"taxpayer_organization\":\"" + p.taxpayerOrganization + "\"" +
		", \"owner_name\":\"" + p.ownerName + "\"" +
		", \"owner_iin\":\"" + p.ownerIin + "\"" +
		", \"owner_rnn\":\"" + p.ownerRnn + "\"" +
		", \"order_no\":\"" + p.orderNo + "\"" +
		", \"order_date\":\"" + p.orderDate + "\"" +
		", \"violation_type\":\"" + p.violationType + "\"" +
		"}\n"
}

func parseAndSendToES(TaxInfoDescription string, f *excelize.File) error {
	cell := Cell{1, 2, 3, 4, 5,
		6, 7, 8, 9}

	replacer := strings.NewReplacer(
		"\"", "'",
		"\\", "/",
		"\n", "",
		"\n\n", "",
		"\r", "")

	for _, name := range f.GetSheetMap() {
		// Get all the rows in the name
		rows := f.GetRows(name)
		var input strings.Builder
		for i, row := range rows {
			if i < 3 {
				continue
			}
			violation := new(Violation)
			for j, colCell := range row {
				switch j {
				case cell.bin:
					violation.bin = replacer.Replace(colCell)
				case cell.rnn:
					violation.rnn = replacer.Replace(colCell)
				case cell.taxpayerOrganization:
					violation.taxpayerOrganization = replacer.Replace(colCell)
				case cell.ownerName:
					violation.ownerName = replacer.Replace(colCell)
				case cell.ownerIin:
					violation.ownerIin = replacer.Replace(colCell)
				case cell.ownerRnn:
					violation.ownerRnn = replacer.Replace(colCell)
				case cell.orderNo:
					violation.orderNo = replacer.Replace(colCell)
				case cell.orderDate:
					violation.orderDate = replacer.Replace(colCell)
				case cell.violationType:
					violation.violationType = replacer.Replace(colCell)
				}
			}
			if violation.bin != "" {
				input.WriteString(violation.toString())
			}
			if i%20000 == 0 {
				if errorT := sendPost(TaxInfoDescription, input.String()); errorT != nil {
					return errorT
				}
				input.Reset()
			}
		}
		if input.Len() != 0 {
			if errorT := sendPost(TaxInfoDescription, input.String()); errorT != nil {
				return errorT
			}
		}
	}
	return nil
}

func sendPost(TaxInfoDescription string, query string) error {
	data := []byte(query)
	r := bytes.NewReader(data)
	resp, err := http.Post("http://localhost:9200/violation_tax_code/companies/_bulk", "application/json", r)
	if err != nil {
		fmt.Println("Could not send the data to elastic search " + TaxInfoDescription)
		fmt.Println(err)
		return err
	}
	fmt.Println(TaxInfoDescription + " " + resp.Status)
	return nil
}
