package snapshot

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lizhenhuan/tidb-full-init/internal/encoder"
)

const (
	rowFlagLarge    = 1 << iota // colIDs or data spans exceed uint16
	rowFlagChecksum             // CRC32 checksum at end of value
)

// rowcodec version byte - if value[0] >= 128, it's the new rowcodec format.
const rowcodecVer = 128

// ExtractRowColumnIDs 返回一行 rowcodec 值中出现的原始列 ID 集合（新旧格式通吃）。
// 用于 D13 启动校验：schema 侧以 ORDINAL_POSITION 充当列 ID，若表经历过
// DROP COLUMN，真实列 ID 与序号错位，解码会静默丢列。
func ExtractRowColumnIDs(value []byte) map[int64]bool {
	ids := make(map[int64]bool)
	if len(value) == 0 {
		return ids
	}
	if IsRowcodecNewFormat(value) {
		extractNewFormatIDs(value, ids)
	} else {
		extractOldFormatIDs(value, ids)
	}
	return ids
}

func extractNewFormatIDs(value []byte, ids map[int64]bool) {
	if len(value) < 6 {
		return
	}
	flags := value[1]
	notNullCnt := int(value[2]) | int(value[3])<<8
	nullCnt := int(value[4]) | int(value[5])<<8
	isLarge := flags&rowFlagLarge != 0

	pos := 6
	notNullIDs, n := readColIDs(value[pos:], notNullCnt, isLarge)
	pos += n
	nullIDs, _ := readColIDs(value[pos:], nullCnt, isLarge)
	for _, id := range notNullIDs {
		ids[id] = true
	}
	for _, id := range nullIDs {
		ids[id] = true
	}
}

func extractOldFormatIDs(value []byte, ids map[int64]bool) {
	pos := 0
	for pos < len(value) {
		colID, n := readVarint(value[pos:])
		if n <= 0 {
			return
		}
		pos += n
		ids[colID] = true
		if pos >= len(value) {
			return
		}
		typeFlag := value[pos]
		pos++
		switch typeFlag {
		case 0: // Nil
		case 1, 6: // Bytes / Decimal (memcomparable)
			_, n := readMemcomparableBytes(value[pos:])
			pos += n
		case 2, 10: // CompactBytes / JSON
			_, n := readCompactBytes(value[pos:])
			pos += n
		case 3, 4: // Int / Uint (memcomparable, 8B)
			pos += 8
		case 5: // Float (9B)
			pos += 9
		case 7: // Duration (8B)
			pos += 8
		case 8: // Varint
			_, n := readVarint(value[pos:])
			pos += n
		case 9: // Uvarint
			_, n := readUvarint(value[pos:])
			pos += n
		default:
			return
		}
	}
}

// ValidateColumnIDs D13 启动校验：样本行中的原始列 ID 若有任何一个不在
// schema 列 ID 集合内，说明序号与真实列 ID 错位（典型成因 DROP COLUMN），
// 必须拒绝执行，避免静默丢列。
//
// realIDs（列 ID 来自 status API 真实值，L7）时语义不同：行数据里的未知
// 列 ID 只可能是被 DROP COLUMN 的残留值——TiDB 自己读这种行也忽略该列，
// 跳过即可（告警不拒绝），真实 ID 模式下拒绝会让所有带 DDL 历史的表
// 永远无法快照。序号兜底模式保持严格拒绝（错位语义不明，跳过=静默丢列）。
func ValidateColumnIDs(sampleValues [][]byte, columns []encoder.ColumnInfo, realIDs bool) error {
	if len(sampleValues) == 0 {
		return nil
	}
	schemaIDs := make(map[int64]bool, len(columns))
	for _, col := range columns {
		schemaIDs[col.ID] = true
	}
	unknown := make(map[int64]bool)
	for _, v := range sampleValues {
		for id := range ExtractRowColumnIDs(v) {
			if !schemaIDs[id] {
				unknown[id] = true
			}
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	list := make([]string, 0, len(unknown))
	for id := range unknown {
		list = append(list, fmt.Sprintf("%d", id))
	}
	if realIDs {
		log.Printf("[snapshot] row data contains dropped-column remnants (column IDs [%s] not in schema ID set %+v); skipping those columns", strings.Join(list, ","), schemaIDs)
		return nil
	}
	return fmt.Errorf(
		"column ID mismatch detected: row data contains column IDs [%s] not present in schema ID set %+v "+
			"(typically caused by DROP COLUMN; ORDINAL_POSITION cannot be used as column ID; "+
			"real column IDs require the TiDB status API — see TIDB_STATUS_ADDR). "+
			"Refusing to scan to avoid silently dropping columns", strings.Join(list, ","), schemaIDs)
}

// IsRowcodecNewFormat checks if the value uses TiDB's new rowcodec format.
func IsRowcodecNewFormat(value []byte) bool {
	return len(value) > 0 && value[0] >= rowcodecVer
}

// DecodeRowNewFormat decodes a TiDB rowcodec-format value into a map[string]string.
// The tableInfo.Columns must include column IDs for this to work correctly.
func DecodeRowNewFormat(value []byte, columns []encoder.ColumnInfo) (map[string]string, error) {
	result := make(map[string]string)
	if len(value) < 6 {
		return result, nil
	}

	pos := 0

	// VER (1B) - already verified >= 128
	pos++

	// FLAGS (1B)
	flags := value[pos]
	pos++

	// NOT_NULL_COL_CNT (2B LE)
	notNullCnt := int(value[pos]) | int(value[pos+1])<<8
	pos += 2

	// NULL_COL_CNT (2B LE)
	nullCnt := int(value[pos]) | int(value[pos+1])<<8
	pos += 2

	isLarge := flags&rowFlagLarge != 0

	// Read NOT_NULL column IDs
	notNullColIDs, n := readColIDs(value[pos:], notNullCnt, isLarge)
	pos += n

	// Read NULL column IDs
	nullColIDs, n := readColIDs(value[pos:], nullCnt, isLarge)
	pos += n

	// Build null set for fast lookup
	nullSet := make(map[int64]bool, len(nullColIDs))
	for _, id := range nullColIDs {
		nullSet[id] = true
	}

	// Read NOT_NULL column offsets
	offsets, n := readOffsets(value[pos:], notNullCnt, isLarge)
	pos += n

	dataStart := pos

	// Build colID -> columnInfo lookup
	colMap := make(map[int64]encoder.ColumnInfo, len(columns))
	for _, col := range columns {
		colMap[col.ID] = col
	}

	// Decode each not-null column value
	// Offsets are END offsets: col[i] data is data[offsets[i-1]:offsets[i]]
	for i, colID := range notNullColIDs {
		var start, end int
		if i == 0 {
			start = dataStart
		} else {
			start = dataStart + int(offsets[i-1])
		}
		end = dataStart + int(offsets[i])

		// J1 修正（对齐 TiDB row.go fromBytes ground truth）：offsets 是只数
		// 列数据的 END 偏移，checksum 尾巴（1B header + 4B CRC [+4B extra]）
		// 位于数据之后——不能对最后一列做 end-=4 补偿，那会砍掉真实数据，
		// ≤4 字节的最后一列（短串/整数）会被整体丢弃。
		// start==end 是合法的零长值（空串列），只有真正越界才跳过。
		if end > len(value) || start > end {
			continue
		}

		colData := value[start:end]

		col, ok := colMap[colID]
		if !ok {
			continue
		}
		result[col.Name] = decodeColumnValue(colData, col.MySQLType, col.IsUnsigned)
	}

	// Set null columns to empty string
	for _, colID := range nullColIDs {
		if col, ok := colMap[colID]; ok {
			result[col.Name] = ""
		}
	}

	return result, nil
}

// DecodeRowOldFormat decodes a TiDB old-format row value (colID-value pairs).
func DecodeRowOldFormat(value []byte, columns []encoder.ColumnInfo) (map[string]string, error) {
	result := make(map[string]string)
	colMap := make(map[int64]encoder.ColumnInfo, len(columns))
	for _, col := range columns {
		colMap[col.ID] = col
	}

	pos := 0
	for pos < len(value) {
		// Read column ID (varint)
		colID, n := readVarint(value[pos:])
		if n <= 0 {
			break
		}
		pos += n

		if pos >= len(value) {
			break
		}

		// Read type flag
		typeFlag := value[pos]
		pos++

		// Decode value based on type flag
		var val string
		var readN int

		switch typeFlag {
		case 0: // Nil
			val = ""
		case 1: // Bytes (memcomparable)
			val, readN = readMemcomparableBytes(value[pos:])
		case 2: // CompactBytes (varint length + raw bytes)
			val, readN = readCompactBytes(value[pos:])
		case 3: // Int (memcomparable, 8 bytes)
			val = readMemcomparableInt(value[pos:])
			readN = 8
		case 4: // Uint (memcomparable, 8 bytes)
			val = readMemcomparableUint(value[pos:])
			readN = 8
		case 5: // Float (9 bytes, memcomparable)
			val = readMemcomparableFloat(value[pos:])
			readN = 9
		case 6: // Decimal (memcomparable)
			val, readN = readMemcomparableBytes(value[pos:])
		case 7: // Duration (int64 nanoseconds)
			if pos+8 <= len(value) {
				ns := int64(binary.BigEndian.Uint64(value[pos:]))
				val = (time.Duration(ns) * time.Nanosecond).String()
			}
			readN = 8
		case 8: // Varint
			v, n := readVarint(value[pos:])
			val = fmt.Sprintf("%d", v)
			readN = n
		case 9: // Uvarint
			v, n := readUvarint(value[pos:])
			val = fmt.Sprintf("%d", v)
			readN = n
		case 10: // JSON
			rawJSON, r := readCompactBytes(value[pos:])
			readN = r
			val = decoderJSON([]byte(rawJSON))
		default:
			// Unknown type, skip remaining
			readN = len(value) - pos
		}

		if col, ok := colMap[int64(colID)]; ok {
			if typeFlag != 10 && isBinaryType(strings.ToLower(col.MySQLType)) && val != "" {
				val = base64.StdEncoding.EncodeToString([]byte(val))
			}
			result[col.Name] = val
		}

		if readN <= 0 {
			break
		}
		pos += readN
	}

	return result, nil
}

func readColIDs(data []byte, count int, isLarge bool) ([]int64, int) {
	ids := make([]int64, count)
	pos := 0
	for i := 0; i < count && pos < len(data); i++ {
		if isLarge {
			if pos+4 > len(data) {
				break
			}
			ids[i] = int64(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
		} else {
			ids[i] = int64(data[pos])
			pos++
		}
	}
	return ids, pos
}

func readOffsets(data []byte, count int, isLarge bool) ([]uint32, int) {
	offsets := make([]uint32, count)
	pos := 0
	for i := 0; i < count && pos < len(data); i++ {
		if isLarge {
			if pos+4 > len(data) {
				break
			}
			offsets[i] = binary.LittleEndian.Uint32(data[pos:])
			pos += 4
		} else {
			if pos+2 > len(data) {
				break
			}
			offsets[i] = uint32(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
		}
	}
	return offsets, pos
}

// decodeColumnValue decodes a raw column value (from rowcodec new format)
// based on the MySQL type. The data is stored WITHOUT type flags in the new format.
func decodeColumnValue(data []byte, mysqlType string, isUnsigned bool) string {
	if len(data) == 0 {
		return ""
	}

	mt := strings.ToLower(mysqlType)

	switch {
	case isIntegerType(mt):
		return decodeInteger(data, mt, isUnsigned)
	case isFloatType(mt):
		return decodeFloat(data, mt)
	case isDecimalType(mt):
		return decodeDecimal(data)
	case isStringType(mt):
		return string(data)
	case isTimeType(mt):
		return decodeTime(data, mt)
	case isBinaryType(mt):
		return encoder.EncodeBinaryValue(data)
	case mt == "json" || strings.HasPrefix(mt, "json("):
		return decoderJSON(data)
	case mt == "bit" || strings.HasPrefix(mt, "bit("):
		return fmt.Sprintf("%d", decodeUintLE(data))
	case mt == "enum" || strings.HasPrefix(mt, "enum("):
		return fmt.Sprintf("%d", decodeUintLE(data))
	case mt == "set" || strings.HasPrefix(mt, "set("):
		return fmt.Sprintf("%d", decodeUintLE(data))
	default:
		return string(data)
	}
}

func isIntegerType(mt string) bool {
	return strings.HasPrefix(mt, "tinyint") ||
		strings.HasPrefix(mt, "smallint") ||
		strings.HasPrefix(mt, "mediumint") ||
		strings.HasPrefix(mt, "int") ||
		strings.HasPrefix(mt, "bigint")
}

func isFloatType(mt string) bool {
	return strings.HasPrefix(mt, "float") ||
		strings.HasPrefix(mt, "double") ||
		mt == "real"
}

func isDecimalType(mt string) bool {
	return strings.HasPrefix(mt, "decimal") || strings.HasPrefix(mt, "numeric")
}

func isStringType(mt string) bool {
	return strings.HasPrefix(mt, "varchar") ||
		strings.HasPrefix(mt, "char") ||
		strings.Contains(mt, "text")
}

func isTimeType(mt string) bool {
	return mt == "datetime" ||
		strings.HasPrefix(mt, "datetime") ||
		mt == "timestamp" ||
		strings.HasPrefix(mt, "timestamp") ||
		mt == "date" ||
		strings.HasPrefix(mt, "date(") ||
		mt == "time" ||
		strings.HasPrefix(mt, "time(") ||
		mt == "year" ||
		strings.HasPrefix(mt, "year(")
}

func isBinaryType(mt string) bool {
	return strings.Contains(mt, "blob") ||
		strings.Contains(mt, "binary")
	// Note: "bit" is NOT binary — it's stored as uint via encodeUint, handled separately
}

// decodeUintLE reads a variable-length little-endian uint64, matching TiDB's rowcodec.encodeUint.
func decodeUintLE(data []byte) uint64 {
	switch len(data) {
	case 0:
		return 0
	case 1:
		return uint64(data[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(data))
	case 4:
		return uint64(binary.LittleEndian.Uint32(data))
	case 8:
		return binary.LittleEndian.Uint64(data)
	default:
		padded := make([]byte, 8)
		copy(padded, data)
		return binary.LittleEndian.Uint64(padded)
	}
}

func decodeInteger(data []byte, mt string, isUnsigned bool) string {
	if len(data) == 0 {
		return "0"
	}
	if isUnsigned {
		return fmt.Sprintf("%d", decodeUintLE(data))
	}
	// Signed integer: decode with proper signed extension through intermediate types
	var val int64
	switch len(data) {
	case 1:
		val = int64(int8(data[0]))
	case 2:
		val = int64(int16(binary.LittleEndian.Uint16(data)))
	case 4:
		val = int64(int32(binary.LittleEndian.Uint32(data)))
	case 8:
		val = int64(binary.LittleEndian.Uint64(data))
	default:
		padded := make([]byte, 8)
		copy(padded, data)
		val = int64(binary.LittleEndian.Uint64(padded))
	}
	return fmt.Sprintf("%d", val)
}

func decodeFloat(data []byte, mt string) string {
	if len(data) == 0 {
		return "0"
	}
	if len(data) != 8 {
		return string(data)
	}
	// TiDB rowcodec stores float/double via codec.EncodeFloat which produces
	// 8 bytes big-endian memcomparable encoding:
	//   positive: float64bits XOR (1<<63) — flips sign bit
	//   negative: float64bits XOR 0xFFFFFFFFFFFFFFFF — flips all bits
	bits := binary.BigEndian.Uint64(data)
	if bits&(1<<63) != 0 {
		// MSB set: was positive, sign bit was flipped
		bits ^= (1 << 63)
	} else {
		// MSB clear: was negative, all bits flipped
		bits ^= 0xFFFFFFFFFFFFFFFF
	}
	return fmt.Sprintf("%g", math.Float64frombits(bits))
}

func decodeDecimal(data []byte) string {
	// TiDB rowcodec decimal format (codec.EncodeDecimal):
	//   byte[0] = precision, byte[1] = frac, byte[2:] = ToBin binary data
	//
	// ToBin format: words of base-10^9 digits stored big-endian.
	// Sign encoding:
	//   positive: byte[0] bit7 = 1 (high bit flipped)
	//   negative: all bytes XOR 0xFF, then byte[0] bit7 = 1 (high bit flipped)
	// Each word stores up to 9 decimal digits as an int32 (NOT BCD).
	if len(data) < 3 {
		return string(data)
	}

	precision := int(data[0])
	frac := int(data[1])
	if precision == 0 {
		return "0"
	}
	intg := precision - frac

	binData := data[2:]
	if len(binData) == 0 {
		return "0"
	}

	// Determine sign and decode: if byte[0] bit7 set → positive, else negative
	// After flipping bit7, positive values are as-is; negative need XOR 0xFF on all bytes
	isNegative := binData[0]&0x80 == 0
	buf := make([]byte, len(binData))
	copy(buf, binData)
	buf[0] ^= 0x80 // flip high bit

	if isNegative {
		for i := range buf {
			buf[i] ^= 0xFF
		}
	}

	dig2bytes := [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

	// Read words from buf
	// Layout: leading partial int word, full int words, full frac words, trailing partial frac word
	xInt := intg % 9
	xFrac := frac % 9
	wordsInt := intg / 9
	wordsFrac := frac / 9

	var words []int64 // base-10^9 word values
	pos := 0

	// Leading partial integer word
	if xInt > 0 {
		nBytes := dig2bytes[xInt]
		if pos+nBytes <= len(buf) {
			words = append(words, readDecimalWordSigned(buf[pos:], nBytes))
			pos += nBytes
		}
	}
	// Full integer words
	for i := 0; i < wordsInt; i++ {
		if pos+4 <= len(buf) {
			words = append(words, readDecimalWordSigned(buf[pos:], 4))
			pos += 4
		}
	}
	// Full fractional words
	for i := 0; i < wordsFrac; i++ {
		if pos+4 <= len(buf) {
			words = append(words, readDecimalWordSigned(buf[pos:], 4))
			pos += 4
		}
	}
	// Trailing partial fractional word — read raw, extract xFrac digits directly
	if xFrac > 0 {
		nBytes := dig2bytes[xFrac]
		if pos+nBytes <= len(buf) {
			words = append(words, readDecimalWordSigned(buf[pos:], nBytes))
		}
	}

	// Convert words to digit string
	// Each word (except possibly the first) represents exactly 9 digits
	// The first word may have fewer (xInt) leading digits
	var intDigits, fracDigits []byte

	wordIdx := 0
	// Integer words: first has xInt digits, subsequent have 9 digits
	if xInt > 0 && wordIdx < len(words) {
		intDigits = appendWordDigits(intDigits, words[wordIdx], xInt)
		wordIdx++
	}
	for i := 0; i < wordsInt && wordIdx < len(words); i++ {
		intDigits = appendWordDigits(intDigits, words[wordIdx], 9)
		wordIdx++
	}
	// Fractional words: full words have 9 digits, last has xFrac digits
	for i := 0; i < wordsFrac && wordIdx < len(words); i++ {
		fracDigits = appendWordDigits(fracDigits, words[wordIdx], 9)
		wordIdx++
	}
	if xFrac > 0 && wordIdx < len(words) {
		fracDigits = appendWordDigits(fracDigits, words[wordIdx], xFrac)
	}

	// Trim leading zeros in integer part
	intStart := 0
	for intStart < len(intDigits)-1 && intDigits[intStart] == '0' {
		intStart++
	}
	intStr := string(intDigits[intStart:])

	// Build result
	// L9：符号条件是「值非零」，不是「整数部分非零」——-1<x<0 的负小数
	// 整数位是 0 但值非零，原判断会吞掉负号（-1e-30 落库成 +1e-30）。
	if isNegative && (intStr != "0" || strings.ContainsAny(string(fracDigits), "123456789")) {
		intStr = "-" + intStr
	}

	if frac > 0 && len(fracDigits) > 0 {
		return intStr + "." + string(fracDigits)
	}
	return intStr
}

// readDecimalWordSigned reads a big-endian signed word from n bytes.
// Matches MySQL's readWord: size 1 uses int8, size 2 uses int8<<8+uint8,
// size 3 uses conditional sign extension, size 4 uses int8<<24+uint8*3.
func readDecimalWordSigned(b []byte, n int) int64 {
	switch n {
	case 1:
		return int64(int8(b[0]))
	case 2:
		return int64(int8(b[0]))<<8 + int64(b[1])
	case 3:
		if b[0]&0x80 != 0 {
			return int64(int32(uint32(0xFF000000) | uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])))
		}
		return int64(int32(b[0])<<16 | int32(b[1])<<8 | int32(b[2]))
	case 4:
		return int64(int32(b[3]) + int32(b[2])<<8 + int32(b[1])<<16 + int32(int8(b[0]))<<24)
	}
	return 0
}

// appendWordDigits appends up to nDigits decimal digits from a base-10^9 word.
// The digits are extracted least-significant first but prepended in reverse order.
func appendWordDigits(dst []byte, word int64, nDigits int) []byte {
	if nDigits <= 0 {
		return dst
	}
	buf := make([]byte, nDigits)
	for i := nDigits - 1; i >= 0; i-- {
		buf[i] = byte('0' + word%10)
		word /= 10
	}
	return append(dst, buf...)
}

func decodeTime(data []byte, mt string) string {
	if len(data) == 0 {
		return "0000-00-00 00:00:00"
	}
	// YEAR type is stored as a simple uint64 (year number) via encodeUint.
	if mt == "year" || strings.HasPrefix(mt, "year(") {
		return fmt.Sprintf("%04d", decodeUintLE(data))
	}
	// TIME type (KindMysqlDuration) is stored as int64 nanoseconds via encodeInt,
	// NOT as ToPackedUint. Need signed LE decode.
	if mt == "time" || strings.HasPrefix(mt, "time(") {
		var ns int64
		switch len(data) {
		case 1:
			ns = int64(int8(data[0]))
		case 2:
			ns = int64(int16(binary.LittleEndian.Uint16(data)))
		case 4:
			ns = int64(int32(binary.LittleEndian.Uint32(data)))
		case 8:
			ns = int64(binary.LittleEndian.Uint64(data))
		default:
			padded := make([]byte, 8)
			copy(padded, data)
			ns = int64(binary.LittleEndian.Uint64(padded))
		}
		// L8a：time(6) 等 fsp>0 的列 MySQL 文本输出恒带小数位（含
		// ".000000"），verify 侧 TIME 按裸字符串比对，必须逐位一致。
		return formatDuration(ns, fspOf(mt))
	}
	// datetime/timestamp stored as ToPackedUint via rowcodec.encodeUint,
	// which uses variable-length LITTLE-ENDIAN encoding.
	packed := decodeUintLE(data)
	return decodePackedTime(packed, mt)
}

// decodePackedTime decodes TiDB's packed time format (FromPackedUint).
// Bit layout from TiDB time.go ToPackedUint:
//
//	1 bit unused | 17 bits: year*13+month | 5 bits: day | 5 bits: hour |
//	6 bits: minute | 6 bits: second | 24 bits: microsecond
func decodePackedTime(packed uint64, mt string) string {
	ymdhms := packed >> 24
	ymd := ymdhms >> 17
	day := int(ymd & (1<<5 - 1))
	ym := ymd >> 5
	month := int(ym % 13)
	year := int(ym / 13)
	hms := ymdhms & (1<<17 - 1)
	second := int(hms & (1<<6 - 1))
	minute := int((hms >> 6) & (1<<6 - 1))
	hour := int(hms >> 12)
	microsecond := int(packed % (1 << 24))

	switch {
	case mt == "date":
		return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	case mt == "time":
		sign := ""
		if hour > 838 || (hour == 838 && minute > 0) {
			// Negative time: in MySQL, negative time is stored with hour > 838
			// Actually, MySQL TIME stores sign in the packed uint: negative if packed > 0x3FFFFFFF
		}
		return fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, second)
	case strings.HasPrefix(mt, "timestamp"):
		// L8b：TIMESTAMP 列 TiDB 行内存的是 UTC 挂钟（读路径由 session
		// time_zone 换算回显示值）。快照绕过 SQL 层直接读行，必须自己做
		// UTC→显示时区换算，否则整体偏移时差（实测 -8h 假 DIFF）。换算
		// 目标取进程本地时区，与驱动 DSN loc=Local 的读出口径一致。
		t := time.Date(year, time.Month(month), day, hour, minute, second, microsecond*1000, time.UTC).In(time.Local)
		if microsecond > 0 {
			return t.Format("2006-01-02 15:04:05.000000")
		}
		return t.Format("2006-01-02 15:04:05")
	default: // datetime
		if microsecond > 0 {
			return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d",
				year, month, day, hour, minute, second, microsecond)
		}
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
			year, month, day, hour, minute, second)
	}
}

// fspOf 从 COLUMN_TYPE（如 "time(6)"、"datetime(3)"）解析小数秒位数；
// 无精度声明返回 0。
func fspOf(mt string) int {
	open := strings.IndexByte(mt, '(')
	if open < 0 {
		return 0
	}
	end := strings.IndexByte(mt[open:], ')')
	if end <= 1 {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(mt[open+1 : open+end]))
	if err != nil || n < 0 || n > 6 {
		return 0
	}
	return n
}

// formatDuration formats nanoseconds (from TiDB KindMysqlDuration) as HH:MM:SS[.ffffff].
// TiDB stores Duration.Nanoseconds() which is int64 nanoseconds.
// Negative durations have negative nanoseconds.
// fsp>0 时恒输出 fsp 位小数（含全零——MySQL 文本输出口径），截位不进位。
func formatDuration(ns int64, fsp int) string {
	neg := ""
	if ns < 0 {
		neg = "-"
		ns = -ns
	}
	totalSec := ns / 1e9
	frac := ns % 1e9
	hours := totalSec / 3600
	minutes := (totalSec % 3600) / 60
	seconds := totalSec % 60
	if fsp > 0 {
		micros := fmt.Sprintf("%06d", frac/1000)
		if fsp < 6 {
			micros = micros[:fsp]
		}
		return fmt.Sprintf("%s%02d:%02d:%02d.%s", neg, hours, minutes, seconds, micros)
	}
	if frac > 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", neg, hours, minutes, seconds, frac/1000)
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", neg, hours, minutes, seconds)
}

// TiDB JSON binary type codes (from pkg/types/json_constants.go).
const (
	jsonTypeCodeObject    byte = 0x01
	jsonTypeCodeArray     byte = 0x03
	jsonTypeCodeLiteral   byte = 0x04
	jsonTypeCodeInt16     byte = 0x05
	jsonTypeCodeUint16    byte = 0x06
	jsonTypeCodeInt32     byte = 0x07
	jsonTypeCodeUint32    byte = 0x08
	jsonTypeCodeInt64     byte = 0x09
	jsonTypeCodeUint64    byte = 0x0A
	jsonTypeCodeFloat64   byte = 0x0B
	jsonTypeCodeString    byte = 0x0C
	jsonTypeCodeOpaque    byte = 0x0D
	jsonTypeCodeDate      byte = 0x0E
	jsonTypeCodeDatetime  byte = 0x0F
	jsonTypeCodeTimestamp byte = 0x10
	jsonTypeCodeDuration  byte = 0x11
)

// decoderJSON decodes TiDB's binary JSON format back to a JSON string.
// Top-level binary JSON is: TypeCode(1B) + Value[] bytes.
func decoderJSON(data []byte) string {
	if len(data) == 0 {
		return "null"
	}
	return string(decodeBinaryJSON(data))
}

// decodeBinaryJSON decodes top-level binary JSON: TypeCode(1B) + Value[].
func decodeBinaryJSON(data []byte) []byte {
	if len(data) == 0 {
		return []byte("null")
	}
	return decodeBinaryJSONWithType(data[0], data[1:])
}

// decodeBinaryJSONWithType decodes a value given an explicit type code and raw value data.
// This is the core decoder used both for top-level values and for recursive
// values inside objects/arrays, where the type code comes from the entry header
// and the data is pointed to by the entry offset (without any type code prefix).
func decodeBinaryJSONWithType(tp byte, val []byte) []byte {
	switch tp {
	case jsonTypeCodeObject:
		return decodeBinaryJSONObject(val)
	case jsonTypeCodeArray:
		return decodeBinaryJSONArray(val)
	case jsonTypeCodeLiteral:
		if len(val) > 0 {
			switch val[0] {
			case 0x00:
				return []byte("null")
			case 0x01:
				return []byte("true")
			case 0x02:
				return []byte("false")
			}
		}
		return []byte("null")
	case jsonTypeCodeInt16:
		if len(val) >= 2 {
			v := int16(binary.LittleEndian.Uint16(val[:2]))
			return []byte(fmt.Sprintf("%d", v))
		}
	case jsonTypeCodeUint16:
		if len(val) >= 2 {
			v := binary.LittleEndian.Uint16(val[:2])
			return []byte(fmt.Sprintf("%d", v))
		}
	case jsonTypeCodeInt32:
		if len(val) >= 4 {
			v := int32(binary.LittleEndian.Uint32(val[:4]))
			return []byte(fmt.Sprintf("%d", v))
		}
	case jsonTypeCodeUint32:
		if len(val) >= 4 {
			v := binary.LittleEndian.Uint32(val[:4])
			return []byte(fmt.Sprintf("%d", v))
		}
	case jsonTypeCodeInt64:
		if len(val) >= 8 {
			v := int64(binary.LittleEndian.Uint64(val[:8]))
			return []byte(fmt.Sprintf("%d", v))
		}
	case jsonTypeCodeUint64:
		if len(val) >= 8 {
			v := binary.LittleEndian.Uint64(val[:8])
			return []byte(fmt.Sprintf("%d", v))
		}
	case jsonTypeCodeFloat64:
		if len(val) >= 8 {
			v := math.Float64frombits(binary.LittleEndian.Uint64(val[:8]))
			return []byte(fmt.Sprintf("%g", v))
		}
	case jsonTypeCodeString:
		return decodeBinaryJSONString(val)
	case jsonTypeCodeOpaque:
		return decodeBinaryJSONOpaque(val)
	case jsonTypeCodeDate, jsonTypeCodeDatetime, jsonTypeCodeTimestamp:
		if len(val) >= 8 {
			packed := binary.LittleEndian.Uint64(val[:8])
			return []byte(fmt.Sprintf("%q", decodeJSONTime(packed, tp)))
		}
	case jsonTypeCodeDuration:
		if len(val) >= 12 {
			nanos := int64(binary.LittleEndian.Uint64(val[:8]))
			fsp := binary.LittleEndian.Uint32(val[8:12])
			d := time.Duration(nanos)
			if fsp > 0 {
				// Truncate to fsp fractional digits
				scale := int64(1)
				for i := uint32(0); i < 6-fsp; i++ {
					scale *= 10
				}
				d = d / time.Duration(scale) * time.Duration(scale)
			}
			return []byte(fmt.Sprintf("%q", d.String()))
		}
	}
	return []byte("null")
}

// decodeBinaryJSONString decodes a string value: uvarint_length + bytes.
func decodeBinaryJSONString(val []byte) []byte {
	if len(val) < 1 {
		return []byte(`""`)
	}
	sLen, n := binary.Uvarint(val)
	if n <= 0 || n+int(sLen) > len(val) {
		return []byte(`""`)
	}
	s := val[n : n+int(sLen)]
	escaped := make([]byte, 0, len(s)*2+2)
	escaped = append(escaped, '"')
	for _, b := range s {
		switch b {
		case '"':
			escaped = append(escaped, '\\', '"')
		case '\\':
			escaped = append(escaped, '\\', '\\')
		case '\n':
			escaped = append(escaped, '\\', 'n')
		case '\r':
			escaped = append(escaped, '\\', 'r')
		case '\t':
			escaped = append(escaped, '\\', 't')
		default:
			if b < 0x20 {
				escaped = append(escaped, fmt.Sprintf("\\u%04x", b)...)
			} else {
				escaped = append(escaped, b)
			}
		}
	}
	escaped = append(escaped, '"')
	return escaped
}

// decodeBinaryJSONOpaque decodes an opaque value: typeId(1B) + uvarint_length + raw_bytes.
// Opaque values represent types TiDB doesn't natively support in JSON.
func decodeBinaryJSONOpaque(val []byte) []byte {
	if len(val) < 1 {
		return []byte(`""`)
	}
	// typeId at val[0], then uvarint length, then raw bytes
	sLen, n := binary.Uvarint(val[1:])
	if n <= 0 || 1+n+int(sLen) > len(val) {
		return []byte(`""`)
	}
	raw := val[1+n : 1+n+int(sLen)]
	// Encode as base64 since we don't know the type
	encoded := base64.StdEncoding.EncodeToString(raw)
	return []byte(`"` + encoded + `"`)
}

// decodeJSONTime decodes a CoreTime packed uint64 into a time string.
// CoreTime bit layout (from pkg/types/core_time.go):
//
//	Bits 0-19:  microsecond (20 bits)
//	Bits 20-25: second (6 bits)
//	Bits 26-31: minute (6 bits)
//	Bits 32-36: hour (5 bits)
//	Bits 37-41: day (5 bits)
//	Bits 42-45: month (4 bits)
//	Bits 46-59: year (14 bits)
func decodeJSONTime(packed uint64, tp byte) string {
	micro := int(packed & 0xFFFFF)       // bits 0-19
	sec := int((packed >> 20) & 0x3F)    // bits 20-25
	min := int((packed >> 26) & 0x3F)    // bits 26-31
	hour := int((packed >> 32) & 0x1F)   // bits 32-36
	day := int((packed >> 37) & 0x1F)    // bits 37-41
	month := int((packed >> 42) & 0xF)   // bits 42-45
	year := int((packed >> 46) & 0x3FFF) // bits 46-59

	switch tp {
	case jsonTypeCodeDate:
		return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	case jsonTypeCodeDatetime, jsonTypeCodeTimestamp:
		if micro > 0 {
			return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, min, sec, micro)
		}
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec)
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

// binaryJSONValSize returns the byte size of a value's raw data given its type code
// and the data buffer starting at the value's offset.
func binaryJSONValSize(tp byte, val []byte, off int) int {
	remaining := len(val) - off
	if remaining <= 0 {
		return 0
	}
	switch tp {
	case jsonTypeCodeLiteral:
		return 0 // literals are inlined in the entry, no separate data
	case jsonTypeCodeInt16, jsonTypeCodeUint16:
		if remaining >= 2 {
			return 2
		}
	case jsonTypeCodeInt32, jsonTypeCodeUint32:
		if remaining >= 4 {
			return 4
		}
	case jsonTypeCodeInt64, jsonTypeCodeUint64, jsonTypeCodeFloat64:
		if remaining >= 8 {
			return 8
		}
	case jsonTypeCodeString:
		sLen, n := binary.Uvarint(val[off:])
		if n > 0 && n+int(sLen) <= remaining {
			return n + int(sLen)
		}
	case jsonTypeCodeOpaque:
		if remaining >= 2 {
			sLen, n := binary.Uvarint(val[off+1:])
			if n > 0 && 1+n+int(sLen) <= remaining {
				return 1 + n + int(sLen)
			}
		}
	case jsonTypeCodeDate, jsonTypeCodeDatetime, jsonTypeCodeTimestamp:
		if remaining >= 8 {
			return 8
		}
	case jsonTypeCodeDuration:
		if remaining >= 12 {
			return 12
		}
	case jsonTypeCodeObject, jsonTypeCodeArray:
		// For container types, read the size field at offset+4 (after element count)
		if off+8 <= len(val) {
			return int(binary.LittleEndian.Uint32(val[off+4 : off+8]))
		}
	}
	return 0
}

// decodeBinaryJSONArray decodes a JSON array from Value[] payload.
// Layout:
//
//	0x00: element-count (4B LE)
//	0x04: total size of this sub-document (4B LE)
//	0x08: value entries (N * 5B: type(1) + offsetOrInline(4))
//	then: value data
//
// For literal types (0x04), offsetOrInline byte[0] is the literal code.
// For all other types, offsetOrInline is the offset from the start of this
// array's Value[] to the raw value data (NO type code prefix).
func decodeBinaryJSONArray(val []byte) []byte {
	if len(val) < 8 {
		return []byte("[]")
	}
	count := int(binary.LittleEndian.Uint32(val[0:4]))
	if count == 0 {
		return []byte("[]")
	}

	entriesStart := 8

	result := []byte{'['}
	for i := 0; i < count; i++ {
		if i > 0 {
			result = append(result, ',', ' ') // L8c：对齐 TiDB json String() 的 ", "
		}
		off := entriesStart + i*5
		if off+5 > len(val) {
			break
		}
		elemType := val[off]
		elemOffsetOrInline := binary.LittleEndian.Uint32(val[off+1 : off+5])

		// Literals are inlined: byte 0 of offsetOrInline is the literal code
		if elemType == jsonTypeCodeLiteral {
			switch byte(elemOffsetOrInline) {
			case 0x00:
				result = append(result, []byte("null")...)
			case 0x01:
				result = append(result, []byte("true")...)
			case 0x02:
				result = append(result, []byte("false")...)
			default:
				result = append(result, []byte("null")...)
			}
			continue
		}

		// Non-literal: offsetOrInline is offset from val start to raw value data
		dataOff := int(elemOffsetOrInline)
		size := binaryJSONValSize(elemType, val, dataOff)
		if size <= 0 || dataOff+size > len(val) {
			result = append(result, []byte("null")...)
			continue
		}
		result = append(result, decodeBinaryJSONWithType(elemType, val[dataOff:dataOff+size])...)
	}
	result = append(result, ']')
	return result
}

// decodeBinaryJSONObject decodes a JSON object from Value[] payload.
// Layout:
//
//	0x00: element-count (4B LE)
//	0x04: total size of this sub-document (4B LE)
//	0x08: key entries (N * 6B: keyOffset(4) + keyLength(2))
//	then: value entries (N * 5B: type(1) + offsetOrInline(4))
//	then: key data (concatenated key strings, no length prefix)
//	then: value data (nested sub-documents)
//
// All offsets are from the start of this object's Value[].
func decodeBinaryJSONObject(val []byte) []byte {
	if len(val) < 8 {
		return []byte("{}")
	}
	count := int(binary.LittleEndian.Uint32(val[0:4]))
	if count == 0 {
		return []byte("{}")
	}

	keyEntriesStart := 8
	keyEntrySize := 6 // keyOffset(4) + keyLength(2)
	valEntriesStart := keyEntriesStart + count*keyEntrySize
	valEntrySize := 5 // type(1) + offsetOrInline(4)

	result := []byte{'{'}
	for i := 0; i < count; i++ {
		if i > 0 {
			// L8c：分隔符对齐 TiDB json String()（", " 与 ": "）——驱动
			// 读出的 JSON 文本带空格，快照侧必须逐字节同口径，否则
			// verify 裸串比对假 DIFF。
			result = append(result, ',', ' ')
		}

		// Read key entry
		kEntryOff := keyEntriesStart + i*keyEntrySize
		if kEntryOff+keyEntrySize > len(val) {
			break
		}
		keyOff := int(binary.LittleEndian.Uint32(val[kEntryOff : kEntryOff+4]))
		keyLen := int(binary.LittleEndian.Uint16(val[kEntryOff+4 : kEntryOff+6]))

		// Read key string
		keyBegin := keyOff
		if keyBegin+keyLen > len(val) {
			break
		}
		keyStr := val[keyBegin : keyBegin+keyLen]

		// Read value entry
		vEntryOff := valEntriesStart + i*valEntrySize
		if vEntryOff+valEntrySize > len(val) {
			break
		}
		vType := val[vEntryOff]
		vOffsetOrInline := binary.LittleEndian.Uint32(val[vEntryOff+1 : vEntryOff+5])

		// Write key
		result = append(result, '"')
		result = append(result, keyStr...)
		result = append(result, '"', ':', ' ')

		// Handle literal values (inlined)
		if vType == jsonTypeCodeLiteral {
			switch byte(vOffsetOrInline) {
			case 0x00:
				result = append(result, []byte("null")...)
			case 0x01:
				result = append(result, []byte("true")...)
			case 0x02:
				result = append(result, []byte("false")...)
			default:
				result = append(result, []byte("null")...)
			}
			continue
		}

		// Non-literal: valOff is from start of this object's Value[]
		valDataOff := int(vOffsetOrInline)
		size := binaryJSONValSize(vType, val, valDataOff)
		if size <= 0 || valDataOff+size > len(val) {
			result = append(result, []byte("null")...)
			continue
		}
		result = append(result, decodeBinaryJSONWithType(vType, val[valDataOff:valDataOff+size])...)
	}
	result = append(result, '}')
	return result
}

// --- Old format helpers ---

func readVarint(data []byte) (int64, int) {
	var result int64
	for i := 0; i < len(data); i++ {
		b := data[i]
		result |= int64(b&0x7F) << (7 * uint(i))
		if b&0x80 == 0 {
			return result, i + 1
		}
	}
	return result, len(data)
}

func readUvarint(data []byte) (uint64, int) {
	var result uint64
	for i := 0; i < len(data); i++ {
		b := data[i]
		result |= uint64(b&0x7F) << (7 * uint(i))
		if b&0x80 == 0 {
			return result, i + 1
		}
	}
	return result, len(data)
}

func readCompactBytes(data []byte) (string, int) {
	length, n := readUvarint(data)
	if n <= 0 || int(n+int(length)) > len(data) {
		return string(data), len(data)
	}
	start := n
	end := start + int(length)
	if end > len(data) {
		return string(data[start:]), len(data)
	}
	return string(data[start:end]), end
}

func readMemcomparableBytes(data []byte) (string, int) {
	// Memcomparable bytes: data is split into 8-byte groups,
	// each group has a trailing marker byte (0x00 for middle/end, 0xFF for more groups).
	if len(data) == 0 {
		return "", 0
	}

	var result []byte
	pos := 0
	for pos+8 < len(data) {
		// Check marker byte
		marker := data[pos+8]
		result = append(result, data[pos:pos+8]...)
		pos += 9
		if marker == 0x00 {
			// End of memcomparable bytes
			break
		}
	}
	// Remaining bytes (last partial group)
	if pos < len(data) {
		result = append(result, data[pos:]...)
	}

	return string(result), pos
}

func readMemcomparableInt(data []byte) string {
	if len(data) < 8 {
		return "0"
	}
	u := binary.BigEndian.Uint64(data[:8])
	v := int64(u ^ 0x8000000000000000)
	return fmt.Sprintf("%d", v)
}

func readMemcomparableUint(data []byte) string {
	if len(data) < 8 {
		return "0"
	}
	u := binary.BigEndian.Uint64(data[:8])
	return fmt.Sprintf("%d", u)
}

func readMemcomparableFloat(data []byte) string {
	if len(data) < 9 {
		return "0"
	}
	u := binary.BigEndian.Uint64(data[:8])
	// Flip sign bit for memcomparable
	u ^= 1 << 63
	v := math.Float64frombits(u)
	return fmt.Sprintf("%g", v)
}
