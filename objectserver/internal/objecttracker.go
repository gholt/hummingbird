package internal

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/gholt/kvt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/troubling/hummingbird/common/fs"
	"go.uber.org/zap"
)

// kvtCondense discards metadata based on the Swift object metadata rules. A
// Swift POST must contain all the metadata desired with no earlier metadata
// carried forward except Content-Length, Content-Type, delete, ETag, and any
// starting with X-Object-Sysmeta-.
func kvtCondense(s kvt.Store) {
	var newestTimestamp int64
	for _, vt := range s {
		if vt.Timestamp > newestTimestamp {
			newestTimestamp = vt.Timestamp
		}
	}
	for k, vt := range s {
		if vt.Timestamp < newestTimestamp && (k != "Content-Length" && k != "Content-Type" && k != "deleted" && k != "ETag" && !strings.HasPrefix(k, "X-Object-Sysmeta-")) {
			delete(s, k)
		}
	}
}

// ObjectTracker will track a set of objects for a path.
//
// This is the "index.db" per disk. Right now it just handles whole objects,
// but eventually we'd like to add either slab support or direct database
// embedding for small objects. But, those details should be transparent from
// users of a ObjectTracker.
//
// This is different from the standard Swift full replica object tracking in
// that the directory structure is much shallower, there are 64 databases per
// drive at most instead of a ton of hashes.pkl files, and the version tracking
// / consolidation is much simpler.
//
// The ObjectTracker stores the newest object contents it knows about and
// discards any older ones, like the standard Swift's .data files. It does not
// have .meta files at all, and certainly not stacked to infinity .meta files.
// Instead the metadata is stored in a JSON-db key=(value,timestamp) structure
// (github.com/gholt/kvt) along with its hash.
//
// A given ObjectTracker may not even store any metadata, such as in an EC
// system, with just "key" ObjectTrackers storing the metadata.
//
// Since there will be 64 databases, it's important to try to have at least
// that many ring partitions per drive. It will work with fewer, but it will
// perform better if it can use all 64 databases.
type ObjectTracker struct {
	path          string
	ringPartPower uint
	diskPartPower uint
	tempPath      string
	dbs           []*sql.DB
	logger        *zap.Logger
}

// NewObjectTracker creates a ObjectTracker to manage the path given.
//
// The ringPartPower is defined by the ring in use, but should be greater than
// the diskPartPower. The diskPartPower should be 6 except for in tests. At
// least, that's our plan for now, as 1<<6 gives 64 databases per disk and ends
// up with not too much over 1 million objects (not including tombstones) per
// database on an 8T disk with 100K average sized objects.
func NewObjectTracker(pth string, ringPartPower, diskPartPower uint, logger *zap.Logger) (*ObjectTracker, error) {
	if ringPartPower <= diskPartPower {
		return nil, fmt.Errorf("ringPartPower must be greater than diskPartPower: %d is not greater than %d", ringPartPower, diskPartPower)
	}
	ot := &ObjectTracker{
		path:          pth,
		tempPath:      path.Join(pth, "temp"),
		ringPartPower: ringPartPower,
		diskPartPower: diskPartPower,
		dbs:           make([]*sql.DB, 1<<diskPartPower),
		logger:        logger,
	}
	err := os.MkdirAll(ot.tempPath, 0700)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 1<<ot.diskPartPower; i++ {
		err := os.MkdirAll(path.Join(ot.path, fmt.Sprintf("objecttracker_%02x", i)), 0700)
		if err != nil {
			return nil, err
		}
		ot.dbs[i], err = sql.Open("sqlite3", path.Join(ot.path, fmt.Sprintf("objecttracker_%02x.sqlite3", i)))
		if err == nil {
			err = ot.init(i)
		}
		if err != nil {
			for j := 0; j < i; j++ {
				ot.dbs[j].Close()
			}
			return nil, err
		}
	}
	return ot, nil
}

func (ot *ObjectTracker) init(dbi int) error {
	db := ot.dbs[dbi]
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`
        SELECT name
        FROM sqlite_master
        WHERE name = 'objects'
    `)
	if err != nil {
		return err
	}
	tableExists := rows.Next()
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	if !tableExists {
		_, err = tx.Exec(`
            CREATE TABLE objects (
                hash TEXT NOT NULL,
                shard INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                deletion INTEGER NOT NULL,
                metahash TEXT, -- NULLable because not everyone stores the metadata
                metadata BLOB,
                CONSTRAINT ix_objects_hash_shard PRIMARY KEY (hash, shard)
            );
            CREATE INDEX ix_objects_hash_shard_timestamp ON objects (hash, shard, timestamp);
        `)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Close closes all the underlying databases for the ObjectTracker; you should
// discard the ObjectTracker after this call and use NewObjectTracker if you
// want to use the path again.
func (ot *ObjectTracker) Close() {
	for _, db := range ot.dbs {
		db.Close()
	}
}

// TempFile returns a temporary file to write to for eventually adding the
// hash:shard to the ObjectTracker with Commit; may return (nil, nil) if there
// is already a newer or equal timestamp in place for the hash:shard.
func (ot *ObjectTracker) TempFile(hsh string, shard int, timestamp int64, sizeHint int64) (fs.AtomicFileWriter, error) {
	storedTimestamp, _, _, _, _, err := ot.Lookup(hsh, shard)
	if err != nil {
		return nil, err
	}
	if storedTimestamp >= timestamp {
		return nil, nil
	}
	dir, err := ot.wholeObjectDir(hsh)
	if err != nil {
		return nil, err
	}
	return fs.NewAtomicFileWriter(ot.tempPath, dir)
}

// Commit moves the temporary file (from TempFile) into place and records its
// information in the database. It may actually discard it completely if there
// is already a newer object information in place for the hash:shard.
//
// Shard is mostly for EC type policies; just use 0 if you're using a full
// replica policy.
//
// Timestamp is the timestamp for the object contents, not the metadata.
//
// Metahash and metadata are from github.com/gholt/kvt.Store -- which is just a
// simple JSON database of key=(value,timestamp) similar to what we use in the
// account/container metadata. The ObjectTracker doesn't look too closely at
// these, but it does compare the hashes and merges metadata sets if needed.
func (ot *ObjectTracker) Commit(f fs.AtomicFileWriter, hsh string, shard int, timestamp int64, deletion bool, metahash string, metadata []byte) error {
	hsh, _, diskPart, err := ot.validateHash(hsh)
	if err != nil {
		return err
	}
	var tx *sql.Tx
	var rows *sql.Rows
	// Single defer so we can control the order of the tear down.
	defer func() {
		if rows != nil {
			rows.Close()
		}
		if tx != nil {
			// If tx.Commit() was already called, this is a No-Op.
			tx.Rollback()
		}
		if f != nil {
			// If f.Save() was already called, this is a No-Op.
			f.Abandon()
		}
	}()
	db := ot.dbs[diskPart]
	tx, err = db.Begin()
	if err != nil {
		return err
	}
	rows, err = tx.Query(`
        SELECT timestamp, metahash, metadata
        FROM objects
        WHERE hash = ? AND shard = ?
        ORDER BY timestamp DESC
    `, hsh, shard)
	if err != nil {
		return err
	}
	var removeOlderPath string
	var removeOlderTimestamp int64
	if !rows.Next() {
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
	} else {
		var dbTimestamp int64
		var dbMetahash string
		var dbMetadata []byte
		if err = rows.Scan(&dbTimestamp, &dbMetahash, &dbMetadata); err != nil {
			return err
		}
		if dbTimestamp >= timestamp {
			return nil
		}
		removeOlderPath, err = ot.wholeObjectPath(hsh, shard, dbTimestamp)
		if err != nil {
			return err
		}
		removeOlderTimestamp = dbTimestamp
		if metahash != dbMetahash {
			metastore := kvt.Store{}
			if err = json.Unmarshal(metadata, &metastore); err != nil {
				// We return this error because the caller gave us bad metadata.
				return err
			}
			dbMetastore := kvt.Store{}
			if err = json.Unmarshal(dbMetadata, &dbMetastore); err != nil {
				ot.logger.Error(
					"error decoding metadata from db; discarding",
					zap.Error(err),
					zap.String("hsh", hsh),
					zap.Int("shard", shard),
					zap.Int64("dbTimestamp", dbTimestamp),
					zap.String("dbMetahash", dbMetahash),
					zap.Binary("dbMetadata", dbMetadata),
				)
			} else {
				if f == nil {
					delete(metastore, "Content-Length")
					delete(metastore, "ETag")
				}
				metastore.Absorb(dbMetastore)
				kvtCondense(metastore)
				var newMetadata []byte
				if newMetadata, err = json.Marshal(metastore); err != nil {
					if _, err2 := json.Marshal(dbMetastore); err2 != nil {
						ot.logger.Error(
							"error reencoding metadata from db; discarding",
							zap.Error(err2),
							zap.String("hsh", hsh),
							zap.Int("shard", shard),
							zap.Int64("dbTimestamp", dbTimestamp),
							zap.String("dbMetahash", dbMetahash),
							zap.Binary("dbMetadata", dbMetadata),
							zap.String("metahash", metahash),
							zap.Binary("metadata", metadata),
						)
					} else {
						// We return this error because the caller (presumably)
						// gave us bad metadata.
						return err
					}
				} else {
					metahash = metastore.Hash()
					metadata = newMetadata
				}
			}
		}
	}
	rows.Close()
	if f == nil && !deletion {
		// We keep the original timestamp if just committing new metadata.
		timestamp = removeOlderTimestamp
	}
	var pth string
	pth, err = ot.wholeObjectPath(hsh, shard, timestamp)
	if err != nil {
		return err
	}
	if f != nil {
		if err = f.Save(pth); err != nil {
			return err
		}
	}
	dbdeletion := 0
	if deletion {
		dbdeletion = 1
	}
	if removeOlderPath == "" {
		_, err = tx.Exec(`
            INSERT INTO objects (hash, shard, timestamp, deletion, metahash, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        `, hsh, shard, timestamp, dbdeletion, metahash, metadata)
	} else {
		_, err = tx.Exec(`
            UPDATE objects
            SET timestamp = ?, deletion = ?, metahash = ?, metadata = ?
            WHERE hash = ? AND shard = ?
        `, timestamp, dbdeletion, metahash, metadata, hsh, shard)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err == nil && removeOlderPath != "" && (f != nil || deletion) {
		if err2 := os.Remove(removeOlderPath); err2 != nil {
			ot.logger.Error(
				"error removing older file",
				zap.Error(err2),
				zap.String("removeOlderPath", removeOlderPath),
			)
		}
	}
	return err
}

func (ot *ObjectTracker) wholeObjectDir(hsh string) (string, error) {
	hsh, _, diskPart, err := ot.validateHash(hsh)
	if err != nil {
		return "", err
	}
	return path.Join(ot.path, fmt.Sprintf("objecttracker_%02x", diskPart)), nil
}

func (ot *ObjectTracker) wholeObjectPath(hsh string, shard int, timestamp int64) (string, error) {
	hsh, _, diskPart, err := ot.validateHash(hsh)
	if err != nil {
		return "", err
	}
	return path.Join(ot.path, fmt.Sprintf("objecttracker_%02x/%s.%02x.%019d", diskPart, hsh, shard, timestamp)), nil
}

// Lookup returns the stored information for the hsh and shard.
func (ot *ObjectTracker) Lookup(hsh string, shard int) (timestamp int64, deletion bool, metahash string, metadata []byte, pth string, err error) {
	hsh, _, diskPart, err := ot.validateHash(hsh)
	if err != nil {
		return 0, false, "", nil, "", err
	}
	db := ot.dbs[diskPart]
	rows, err := db.Query(`
        SELECT timestamp, deletion, metahash, metadata
        FROM objects
        WHERE hash = ? AND shard = ?
        ORDER BY timestamp DESC
    `, hsh, shard)
	if err != nil {
		return 0, false, "", nil, "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, false, "", nil, "", rows.Err()
	}
	var deletionInt int
	if err = rows.Scan(&timestamp, &deletionInt, &metahash, &metadata); err != nil {
		return 0, false, "", nil, "", err
	}
	pth, err = ot.wholeObjectPath(hsh, shard, timestamp)
	return timestamp, deletionInt == 1, metahash, metadata, pth, err
}

// ObjectTrackerItem is a single item returned by List.
type ObjectTrackerItem struct {
	Hash      string
	Shard     int
	Timestamp int64
	Metahash  string
}

// List returns the items for the ringPart given.
//
// This is for replication, auditing, that sort of thing.
func (ot *ObjectTracker) List(ringPart int) ([]*ObjectTrackerItem, error) {
	startHash, stopHash := ot.ringPartRange(ringPart)
	_, _, startDiskPart, err := ot.validateHash(startHash)
	if err != nil {
		return nil, err
	}
	_, _, stopDiskPart, err := ot.validateHash(stopHash)
	if err != nil {
		return nil, err
	}
	listing := []*ObjectTrackerItem{}
	for diskPart := startDiskPart; diskPart <= stopDiskPart; diskPart++ {
		db := ot.dbs[diskPart]
		rows, err := db.Query(`
	        SELECT hash, shard, timestamp, metahash
	        FROM objects
	        WHERE hash BETWEEN ? AND ?
	    `, startHash, stopHash)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			item := &ObjectTrackerItem{}
			if err = rows.Scan(&item.Hash, &item.Shard, &item.Timestamp, &item.Metahash); err != nil {
				return listing, err
			}
			listing = append(listing, item)
		}
		if err = rows.Err(); err != nil {
			return listing, err
		}
	}
	return listing, nil
}

func (ot *ObjectTracker) validateHash(hsh string) (hshOut string, ringPart int, diskPart int, err error) {
	hsh = strings.ToLower(hsh)
	if len(hsh) != 32 {
		return "", 0, 0, fmt.Errorf("invalid hash %q; length was %d not 32", hsh, len(hsh))
	}
	hashBytes, err := hex.DecodeString(hsh)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid hash %q; decoding error: %s", hsh, err)
	}
	upper := uint64(hashBytes[0])<<24 | uint64(hashBytes[1])<<16 | uint64(hashBytes[2])<<8 | uint64(hashBytes[3])
	return hsh, int(upper >> (32 - ot.ringPartPower)), int(hashBytes[0] >> (8 - ot.diskPartPower)), nil
}

func (ot *ObjectTracker) ringPartRange(ringPart int) (string, string) {
	start := uint64(ringPart << (64 - ot.ringPartPower))
	stop := uint64((ringPart+1)<<(64-ot.ringPartPower)) - 1
	return fmt.Sprintf("%016x0000000000000000", start), fmt.Sprintf("%016xffffffffffffffff", stop)
}
