package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/tikv/client-go/v2/txnkv"
)

const signMask uint64 = 0x8000000000000000

func encodeInt(b []byte, v int64) []byte {
	var data [8]byte
	u := uint64(v) ^ signMask
	binary.BigEndian.PutUint64(data[:], u)
	return append(b, data[:]...)
}

func main() {
	log.SetFlags(0)
	addrs := []string{"127.0.0.1:2379"}
	client, err := txnkv.NewClient(addrs)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ts, _ := client.GetTimestamp(context.Background())
	snap := client.GetSnapshot(ts)
	snap.SetScanBatchSize(1)

	tableID := int64(115)
	var startKey []byte
	startKey = append(startKey, 't')
	startKey = encodeInt(startKey, tableID)
	startKey = append(startKey, '_', 'r')
	var endKey []byte
	endKey = append(endKey, 't')
	endKey = encodeInt(endKey, tableID+1)
	endKey = append(endKey, '_', 'r')

	iter, err := snap.Iter(startKey, endKey)
	if err != nil {
		log.Fatal(err)
	}
	defer iter.Close()

	for iter.Valid() {
		value := iter.Value()
		if len(value) < 6 || value[0] < 128 {
			iter.Next()
			continue
		}

		flags := value[1]
		notNullCnt := int(value[2]) | int(value[3])<<8
		isLarge := flags&1 != 0
		pos := 6

		var colIDs []int64
		for i := 0; i < notNullCnt; i++ {
			if isLarge {
				id := int64(binary.LittleEndian.Uint32(value[pos:]))
				pos += 4
				colIDs = append(colIDs, id)
			} else {
				colIDs = append(colIDs, int64(value[pos]))
				pos++
			}
		}

		var offsets []uint32
		for i := 0; i < notNullCnt; i++ {
			if isLarge {
				off := binary.LittleEndian.Uint32(value[pos:])
				pos += 4
				offsets = append(offsets, off)
			} else {
				off := uint32(binary.LittleEndian.Uint16(value[pos:]))
				pos += 2
				offsets = append(offsets, off)
			}
		}

		dataStart := pos
		handle := extractHandle(iter.Key())
		fmt.Printf("\nRow: handle=%d colIDs=%v offsets=%v\n", handle, colIDs, offsets)

		for i, id := range colIDs {
			var start, end int
			if i == 0 {
				start = dataStart
			} else {
				start = dataStart + int(offsets[i-1])
			}
			end = dataStart + int(offsets[i])
			if start >= end || end > len(value) {
				continue
			}
			colData := value[start:end]
			fmt.Printf("  colID=%d len=%d hex=%v", id, len(colData), fmt.Sprintf("%x", colData))

			if len(colData) == 8 {
				u := binary.BigEndian.Uint64(colData)
				fmt.Printf("  u64=%d", u)
				y := (u >> 63) & 1
				year := (u >> 46) & 0x1FFFF
				mon := (u >> 42) & 0xF
				day := (u >> 37) & 0x1F
				hour := (u >> 32) & 0x1F
				minute := (u >> 26) & 0x3F
				second := (u >> 20) & 0x3F
				micro := (u >> 4) & 0xFFFF
				tp := (u >> 1) & 0x7
				fmt.Printf("  time: y=%d year=%d mon=%d day=%d hour=%d min=%d sec=%d micro=%d type=%d",
					y, year, mon, day, hour, minute, second, micro, tp)
			}

			if id == 4 {
				fmt.Printf("  dec=[")
				for _, b := range colData {
					fmt.Printf("%d ", b)
				}
				fmt.Printf("]")
			}

			fmt.Println()
		}
		iter.Next()
	}
}

func extractHandle(k []byte) int64 {
	if len(k) < 19 {
		return 0
	}
	u := binary.BigEndian.Uint64(k[11:19])
	return int64(u ^ signMask)
}
