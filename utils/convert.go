package utils

import "strconv"

func convertSlice[From, To any](src []From, convert func(From) To) []To {
	dst := make([]To, len(src))
	for i, e := range src {
		dst[i] = convert(e)
	}
	return dst
}

func IntSlice2Uint64Slice(ints []int) []uint64 {
	return convertSlice(ints, func(from int) uint64 { return uint64(from) })
}

func IntSlice2Uint32Slice(ints []int) []uint32 {
	return convertSlice(ints, func(from int) uint32 { return uint32(from) })
}

func IntSlice2Uint16Slice(ints []int) []uint16 {
	return convertSlice(ints, func(from int) uint16 { return uint16(from) })
}

func IntSlice2Uint8Slice(ints []int) []uint8 {
	return convertSlice(ints, func(from int) uint8 { return uint8(from) })
}

func UintSlice2int64Slice(ints []uint) []int64 {
	return convertSlice(ints, func(from uint) int64 { return int64(from) })
}

func UintSlice2int32Slice(ints []uint) []int32 {
	return convertSlice(ints, func(from uint) int32 { return int32(from) })
}

func UintSlice2int16Slice(ints []uint) []int16 {
	return convertSlice(ints, func(from uint) int16 { return int16(from) })
}

func UintSlice2int8Slice(ints []uint) []int8 {
	return convertSlice(ints, func(from uint) int8 { return int8(from) })
}

func BytesToUint64(val []byte) uint64 {
	i, _ := strconv.ParseUint(string(val), 10, 64)
	return i
}

func BytesToFloat64(val []byte) float64 {
	f, _ := strconv.ParseFloat(string(val), 64)
	return f
}

func Float64ToBytes(val float64) []byte {
	return []byte(strconv.FormatFloat(val, 'f', -1, 64))
}
