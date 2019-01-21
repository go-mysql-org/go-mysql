package schema

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/boltdb/bolt"
	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/mysql"
	"time"
)

// The value of blotdb
type value struct {
	Name      string
	Pos       uint32
	Snapshot  []byte
	Database  string
	Statement string
	Time      time.Time
}

type boltdbStorage struct {
	path        string
	curServerID uint32

	db *bolt.DB
}

func NewBoltdbStorage(path string) (*boltdbStorage, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	storage := &boltdbStorage{
		path: path,
		db:   db,
	}

	return storage, nil
}

// Save snapshot data
func (o *boltdbStorage) SaveSnapshot(data []byte, pos mysql.Position) error {
	err := o.db.Update(func(tx *bolt.Tx) error {
		var err error
		bucket := tx.Bucket([]byte("data"))

		id, _ := bucket.NextSequence()

		// Make a sortable key base on id
		key, err := makeKey(id)
		if err != nil {
			return err
		}
		value, err := makeValue(data, "", "", pos)
		if err != nil {
			return err
		}

		// Save snapshot
		err = bucket.Put(key, value)
		if err != nil {
			return err
		}

		// Save the key of last snapshot into meta
		meta := tx.Bucket([]byte("meta"))
		err = meta.Put([]byte("last_snapshot"), key)
		if err != nil {
			return err
		}

		// Purge the expired data
		return o.purge(tx)
	})

	if err != nil {
		return err
	}

	return nil
}

func (o *boltdbStorage) LoadLastSnapshot() ([]byte, mysql.Position, error) {
	var pos mysql.Position
	var value value
	var data []byte

	err := o.db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		key := meta.Get([]byte("last_snapshot"))

		if key == nil {
			// Maybe this is in initial startup
			return nil
		}

		bucket := tx.Bucket([]byte("data"))

		valueBytes := bucket.Get([]byte(key))
		err := json.Unmarshal(valueBytes, &value)
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, pos, err
	}

	pos.Name = value.Name
	pos.Pos = value.Pos
	data = value.Snapshot

	return data, pos, nil
}

func (o *boltdbStorage) SaveStatement(db string, statement string, pos mysql.Position) error {
	// TODO
	return nil
}

func (o *boltdbStorage) Reset() error {
	err := o.db.Update(func(tx *bolt.Tx) error {
		// Delete meta bucket if exists
		if tx.Bucket([]byte("meta")) != nil {
			err := tx.DeleteBucket([]byte("meta"))
			if err != nil {
				return err
			}
		}
		// Re-create meta bucket
		_, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			return err
		}

		// Delete data bucket if exists
		if tx.Bucket([]byte("data")) != nil {
			err := tx.DeleteBucket([]byte("data"))
			if err != nil {
				return err
			}
		}
		// Re-create data bucket
		_, err = tx.CreateBucketIfNotExists([]byte("data"))
		if err != nil {
			return err
		}

		// Reset current server_id
		o.curServerID = 0
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (o *boltdbStorage) StatementIterator() Iterator {
	return &boltdbStorageIterator{}
}

// Purge the snapshot or statement before the last snapshot
func (o *boltdbStorage) purge(tx *bolt.Tx) error {
	meta := tx.Bucket([]byte("meta"))
	key := meta.Get([]byte("last_snapshot"))

	if key == nil {
		return nil
	}

	bucket := tx.Bucket([]byte("data"))
	if bucket == nil {
		return errors.Errorf("the bucket of server_id: %d is missing", o.curServerID)
	}

	c := bucket.Cursor()
	k, _ := c.Seek(key)
	if k == nil {
		return errors.Errorf("the k-v of key: %d is missing", key)
	}

	for {
		k, v := c.Prev()
		if k == nil {
			break
		}

		var value value
		err := json.Unmarshal(v, &value)
		if err != nil {
			return err
		}
		if time.Now().Sub(value.Time) >= 7*24*time.Hour {
			bucket.Delete(k)
		}

	}

	return nil
}

func makeValue(snapshot []byte, db string, statement string, pos mysql.Position) ([]byte, error) {
	var value value
	value.Name = pos.Name
	value.Pos = pos.Pos
	value.Snapshot = snapshot
	value.Database = db
	value.Statement = statement
	value.Time = time.Now()

	buf, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return buf, err
}

// Make a sortable bytes slice, used as key of blotdb key-value
func makeKey(id uint64) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, id)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unmarshal(b []byte, v interface{}) error {
	buffer := bytes.NewBuffer(b)
	err := binary.Read(buffer, binary.BigEndian, v)
	if err != nil {
		return err
	}
	return nil
}

type boltdbStorageIterator struct {
	nextKey []byte
	end     bool
}

func (o *boltdbStorageIterator) First() (string, string, mysql.Position, error) {
	// TODO
	var pos mysql.Position
	return "", "", pos, nil
}

func (o *boltdbStorageIterator) Next() (string, string, mysql.Position, error) {
	// TODO
	var pos mysql.Position
	return "", "", pos, nil
}

func (o *boltdbStorageIterator) End() bool {
	// TODO
	return o.end
}
