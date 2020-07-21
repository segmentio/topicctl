package apply

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

func Confirm(prompt string, skip bool) (bool, error) {
	fmt.Printf("%s (yes/no) ", prompt)

	if skip {
		log.Infof("Automatically answering yes because skip is set to true")
		return true, nil
	}

	var response string
	_, err := fmt.Scanln(&response)
	if err != nil {
		log.Warnf("Got error reading response, not continuing: %+v", err)
		return false, err
	}
	if strings.TrimSpace(strings.ToLower(response)) != "yes" {
		log.Infof("Not continuing")
		return false, nil
	}

	return true, nil
}
