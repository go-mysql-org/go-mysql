package pkg

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// RenameRule 支持两种格式的 rename
// rule1: mv 格式的 {,} 匹配，我们叫做 curly rename rule
//   rename abc{,.bak} >> rename abc abc.bak
//   rename hhh{abc,abc_bak}_0 >> rename hhhabc_0 hhhabc_bak_0
// rule2: aa->bb，我们叫做 plain rename rule

type RenameRule struct {
	rules []string

	// renameMap cache
	renameMap map[string]string

	// curly rule reNameMatch
	reNameMatch []*regexp.Regexp
	reNewPart   []string
}

func NewRenameRule(rules []string) (*RenameRule, error) {
	r := &RenameRule{rules: rules}
	r.renameMap = make(map[string]string)

	for _, rule := range rules {
		if rule == "" {
			continue
		}
		if strings.Contains(rule, "->") { // plain rename rule
			renameParts := strings.Split(rule, "->")
			oldName := strings.TrimSpace(renameParts[0])
			newName := strings.TrimSpace(renameParts[1])
			if oldName == "" || newName == "" {
				return nil, errors.Errorf("oldName or newName cannot be empty: %s", rule)
			}
			r.renameMap[oldName] = newName
		} else if strings.Count(rule, "{") == 1 && strings.Count(rule, "}") == 1 &&
			strings.Count(rule, ",") == 1 { // curly rename rule
			if err := r.parseCurlyRule(rule); err != nil {
				return nil, err
			}
		} else {
			return nil, errors.Errorf("invalid rename rule %s", rule)
		}
	}
	return r, nil
}

func (r *RenameRule) parseCurlyRule(rule string) error {
	var err error
	curlyRenames := regexp.MustCompile(`(.*){(.*),(.*)}(.*)`)
	matches := curlyRenames.FindAllStringSubmatch(rule, -1)
	if len(matches) != 1 {
		return errors.Errorf("rule %s need separated by ,", rule)
	}

	prefix := matches[0][1]
	suffix := matches[0][4]
	oldPart := strings.TrimSpace(matches[0][2])
	newPart := strings.TrimSpace(matches[0][3])
	oldMatch := fmt.Sprintf(`^(%s)(%s)(%s)$`, prefix, oldPart, suffix)
	reMatch, err := regexp.Compile(oldMatch)
	if err != nil {
		return err
	}
	r.reNameMatch = append(r.reNameMatch, reMatch)
	r.reNewPart = append(r.reNewPart, newPart)
	return err
}

// GetNewName if no rule match oldName, return oldName
func (r *RenameRule) GetNewName(oldName string) string {
	if newName, ok := r.renameMap[oldName]; ok {
		return newName
	} else {
		for i, reNameMatch := range r.reNameMatch {
			newName = reNameMatch.ReplaceAllString(oldName, "${1}"+r.reNewPart[i]+"${3}")
			r.renameMap[oldName] = newName
			return newName
		}
	}
	return oldName
}

// GetNewNameMustMatch if no rule match oldName, return error
func (r *RenameRule) GetNewNameMustMatch(oldName string) (string, error) {
	if newName, ok := r.renameMap[oldName]; ok {
		return newName, nil
	} else {
		for i, reNameMatch := range r.reNameMatch {
			if !reNameMatch.MatchString(oldName) {
				return "", errors.Errorf("oldName %s not match any rules %v", oldName, r.renameMap)
			}
			newName = reNameMatch.ReplaceAllString(oldName, "${1}"+r.reNewPart[i]+"${3}")
			r.renameMap[oldName] = newName
			return newName, nil
		}
	}
	return "", errors.Errorf("no match rule found")
}

func main() {
	rule1 := "a.*{bb,bb2024}_3"
	rule2 := "aaa{,.bak}"
	r, e := NewRenameRule([]string{rule1, rule2, "aaa->bbb"})
	if e != nil {
		fmt.Println(e)
		os.Exit(1)
	}
	fmt.Println(r.GetNewName("aaa"))
}
