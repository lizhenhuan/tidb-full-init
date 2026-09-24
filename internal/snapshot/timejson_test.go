package snapshot

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

// L8a：TIME 的 fsp>0 输出恒带小数位（含全零），与 MySQL 文本口径逐位一致。
func TestFormatDurationFsp(t *testing.T) {
	cases := []struct {
		ns   int64
		fsp  int
		want string
	}{
		{0, 6, "00:00:00.000000"},
		{0, 0, "00:00:00"},
		{-(838*3600+59*60+59) * 1e9, 6, "-838:59:59.000000"},
		{(3600 + 60 + 1)*1e9 + 123456789, 6, "01:01:01.123456"},
		{(3600 + 60 + 1)*1e9 + 123456789, 3, "01:01:01.123"}, // 截位不进位
		{(3600 + 60 + 1)*1e9 + 123456789, 0, "01:01:01.123456"},
		{5 * 1e9, 0, "00:00:05"},
	}
	for _, c := range cases {
		if got := formatDuration(c.ns, c.fsp); got != c.want {
			t.Errorf("formatDuration(%d,%d) = %q, want %q", c.ns, c.fsp, got, c.want)
		}
	}
}

func TestFspOf(t *testing.T) {
	for mt, want := range map[string]int{
		"time": 0, "time(6)": 6, "time(3)": 3,
		"datetime(6)": 6, "time(x)": 0, "time(9)": 0, "time(-1)": 0,
	} {
		if got := fspOf(mt); got != want {
			t.Errorf("fspOf(%q) = %d, want %d", mt, got, want)
		}
	}
}

// L8b：TIMESTAMP 行内存 UTC 挂钟，解码必须换算到显示时区（进程本地），
// 否则整体偏移时差（实测 -8h）。DATETIME 不换算。
func TestDecodePackedTimeTimestampTZ(t *testing.T) {
	const (
		year, month, day = 2000, 1, 2
		hour, min, sec   = 3, 4, 5
		micro            = 600001
	)
	ym := uint64(year)*13 + uint64(month)
	ymd := ym<<5 | uint64(day)
	hms := uint64(hour)<<12 | uint64(min)<<6 | uint64(sec)
	packed := (ymd<<17|hms)<<24 | micro

	want := time.Date(year, time.Month(month), day, hour, min, sec, micro*1000, time.UTC).
		In(time.Local).Format("2006-01-02 15:04:05.000000")
	if got := decodePackedTime(packed, "timestamp(6)"); got != want {
		t.Fatalf("timestamp decode = %q, want %q (local display)", got, want)
	}

	// datetime 不换算：直接输出存的挂钟。
	wantDT := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, min, sec, micro)
	if got := decodePackedTime(packed, "datetime(6)"); got != wantDT {
		t.Fatalf("datetime decode = %q, want %q", got, wantDT)
	}
}

// L8c：JSON 文本输出对齐 TiDB json String()（", " 与 ": "），
// 驱动读出的带空格文本与快照输出逐字节一致。
func TestDecoderJSONSpacing(t *testing.T) {
	// {"a": null, "b": true}
	obj := make([]byte, 0, 32)
	obj = binary.LittleEndian.AppendUint32(obj, 2) // count
	obj = binary.LittleEndian.AppendUint32(obj, 40) // totalSize（解码器不读）
	obj = binary.LittleEndian.AppendUint32(obj, 30) // keyOff["a"]
	obj = binary.LittleEndian.AppendUint16(obj, 1)  // keyLen["a"]
	obj = binary.LittleEndian.AppendUint32(obj, 31) // keyOff["b"]
	obj = binary.LittleEndian.AppendUint16(obj, 1)  // keyLen["b"]
	obj = append(obj, jsonTypeCodeLiteral, 0, 0, 0, 0) // "a" → null（inline 0）
	obj = append(obj, jsonTypeCodeLiteral, 1, 0, 0, 0) // "b" → true（inline 1）
	obj = append(obj, 'a', 'b')
	if got := string(decodeBinaryJSONWithType(jsonTypeCodeObject, obj)); got != `{"a": null, "b": true}` {
		t.Fatalf("object = %q", got)
	}

	// [null, true]
	arr := binary.LittleEndian.AppendUint32(nil, 2)
	arr = binary.LittleEndian.AppendUint32(arr, 16)
	arr = append(arr, jsonTypeCodeLiteral, 0, 0, 0, 0)
	arr = append(arr, jsonTypeCodeLiteral, 1, 0, 0, 0)
	if got := string(decodeBinaryJSONWithType(jsonTypeCodeArray, arr)); got != `[null, true]` {
		t.Fatalf("array = %q", got)
	}
}
