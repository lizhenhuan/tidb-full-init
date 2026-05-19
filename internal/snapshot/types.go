package snapshot

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// parseDecimalPrecisionScale extracts precision and scale from MySQL type like "decimal(10,2)"
func parseDecimalPrecisionScale(mysqlType string) (precision, scale int) {
	precision = 10
	scale = 2
	if strings.HasPrefix(mysqlType, "decimal") || strings.HasPrefix(mysqlType, "numeric") {
		// Parse decimal(M,N) format
		start := strings.Index(mysqlType, "(")
		end := strings.Index(mysqlType, ")")
		if start >= 0 && end > start {
			parts := strings.Split(mysqlType[start+1:end], ",")
			if len(parts) >= 1 {
				fmt.Sscanf(strings.TrimSpace(parts[0]), "%d", &precision)
			}
			if len(parts) >= 2 {
				fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &scale)
			}
		}
	}
	if precision <= 0 {
		precision = 10
	}
	return precision, scale
}

// decodeDatetime decodes TiDB's packed datetime from rowcodec.
// TiDB uses MysqlTime.ToPackedUint() format, but the bit layout varies by TiDB version.
func decodeDatetime(data []byte, mysqlType string) string {
	if len(data) == 0 {
		return "0000-00-00 00:00:00"
	}

	if len(data) == 8 {
		return decodePackedUintTime(data)
	}

	if len(data) == 4 {
		return decodePackedUintTime([]byte{0, 0, 0, 0, data[0], data[1], data[2], data[3]})
	}

	return fmt.Sprintf("%s", data)
}

// decodePackedUintTime decodes TiDB's MysqlTime.ToPackedUint() format.
// Bit layout (from TiDB source):
//
//	y(1) << 63 | year(17) << 46 | month(4) << 42 | day(5) << 37 |
//	hour(5) << 32 | minute(6) << 26 | second(6) << 20 | micro(16) << 4
//	type(3) << 1
func decodePackedUintTime(data []byte) string {
	if len(data) != 8 {
		return "0000-00-00 00:00:00"
	}

	u := binary.BigEndian.Uint64(data)

	y := (u >> 63) & 1
	year := (u >> 46) & 0x1FFFF
	month := (u >> 42) & 0xF
	day := (u >> 37) & 0x1F
	hour := (u >> 32) & 0x1F
	minute := (u >> 26) & 0x3F
	second := (u >> 20) & 0x3F

	if y == 0 && year == 0 && month == 0 && day == 0 {
		// Year might be encoded differently in this TiDB version
		// Try alternative bit layouts
		return decodePackedUintTimeFallback(data)
	}

	if year == 0 && y == 0 {
		year = 2000 // fallback
	}

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		year, month, day, hour, minute, second)
}

// decodePackedUintTimeFallback tries alternative bit layouts for the packed datetime.
func decodePackedUintTimeFallback(data []byte) string {
	u := binary.BigEndian.Uint64(data)

	// Try: year(15)<<49 | month(4)<<45 | day(5)<<40 | hour(5)<<35
	year := int((u >> 49) & 0x7FFF)
	month := int((u >> 45) & 0xF)
	day := int((u >> 40) & 0x1F)
	hour := int((u >> 35) & 0x1F)
	minute := int((u >> 29) & 0x3F)
	second := int((u >> 23) & 0x3F)

	if year > 0 && year < 10000 && month >= 1 && month <= 12 && day >= 1 && day <= 31 {
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
			year, month, day, hour, minute, second)
	}

	// Last resort: return raw hex
	return fmt.Sprintf("0x%x", u)
}

var _ = binary.BigEndian.Uint64
var _ = strings.Join
