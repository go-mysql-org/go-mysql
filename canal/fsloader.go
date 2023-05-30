package canal

import (
	"bytes"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/instructure/mc-go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go/ioutil2"
)

type fsInfoLoader struct {
	path string
}

func NewFsInfoLoader(path string) MasterInfoLoader {
	return &fsInfoLoader{path: path}
}

func (l *fsInfoLoader) Load(setValues MasterInfoSetter) error {
	f, err := os.Open(l.path)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return nil
	}
	defer f.Close()

	var m masterInfo
	_, err = toml.DecodeReader(f, &m)

	if err != nil {
		return err
	}

	return setValues(m.Addr, m.pos.Name, m.pos.Pos)
}

func (l *fsInfoLoader) Save(addr, name string, position uint32, force bool) error {
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	pos := mysql.Position{Name: name, Pos: position}

	m := &masterInfo{
		Addr: addr,
		pos:  pos,
	}

	e.Encode(m)

	var err error
	if err = ioutil2.WriteFileAtomic(l.path, buf.Bytes(), 0644); err != nil {
		log.Errorf("canal save master info to file %s err %v", l.path, err)
	}

	return errors.Trace(err)
}
