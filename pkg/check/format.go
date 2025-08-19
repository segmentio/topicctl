package check

import (
	"bytes"
	"fmt"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/segmentio/topicctl/pkg/util"
)

// FormatResults generates a pretty table from topic check results.
func FormatResults(results TopicCheckResults) string {
	buf := &bytes.Buffer{}

	table := tablewriter.NewTable(buf,
		tablewriter.WithConfig(
			tablewriter.NewConfigBuilder().
				WithRowAutoWrap(tw.WrapNone).
				ForColumn(0).WithAlignment(tw.AlignLeft).Build().
				ForColumn(1).WithAlignment(tw.AlignCenter).Build().
				ForColumn(2).WithAlignment(tw.AlignLeft).Build().
				Build()),
		tablewriter.WithRendition(tw.Rendition{
			Borders: tw.Border{
				Left:   tw.Off,
				Top:    tw.On,
				Right:  tw.Off,
				Bottom: tw.On,
			},
		}),
	)
	table.Header("Name", "OK", "Details")

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
