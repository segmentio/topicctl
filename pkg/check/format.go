package check

import (
	"bytes"
	"fmt"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/segmentio/topicctl/pkg/util"
)

func FormatResults(results TopicCheckResults) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewWriter(buf)

	table.SetHeader([]string{
		"Name",
		"OK",
		"Details",
	})

	table.SetAutoWrapText(false)
	table.SetColumnAlignment(
		[]int{
			tablewriter.ALIGN_LEFT,
			tablewriter.ALIGN_CENTER,
			tablewriter.ALIGN_LEFT,
		},
	)
	table.SetBorders(
		tablewriter.Border{
			Left:   false,
			Top:    true,
			Right:  false,
			Bottom: true,
		},
	)

	for _, result := range results.Results {
		var checkPrinter func(f string, a ...interface{}) string
		if result.OK || !util.InTerminal() {
			checkPrinter = fmt.Sprintf
		} else {
			checkPrinter = color.New(color.FgRed).SprintfFunc()
		}

		var okStr string

		if result.OK {
			okStr = "✓"
		} else {
			okStr = "✗"
		}

		table.Append(
			[]string{
				checkPrinter("%s", string(result.Name)),
				checkPrinter("%s", okStr),
				checkPrinter("%s", result.Description),
			},
		)
	}

	table.Render()
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}
