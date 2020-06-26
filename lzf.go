package goyxdb

import (
	"fmt"
)

func decompress(inData []byte, inLen uint32, outData []byte, outLen uint32) (uint32, error) {
	var iidx uint32 = 0
	var oidx uint32 = 0

	doOuterLoop := true
	for doOuterLoop {
		var ctrl = uint32(inData[iidx])
		iidx++

		if ctrl < (1 << 5) {
			ctrl++
			if oidx+ctrl > outLen {
				return 0, fmt.Errorf(`E2BIG literal`)
			}

			doInnerLoop := true
			for doInnerLoop {
				outData[oidx] = inData[iidx]
				oidx++
				iidx++

				ctrl--
				doInnerLoop = ctrl != 0
			}
			doOuterLoop = iidx < inLen
			continue
		}

		var length = ctrl >> 5
		reference := oidx - ((ctrl & 0x1f) << 8) - 1
		if length == 7 {
			length += uint32(inData[iidx])
			iidx++
		}

		reference -= uint32(inData[iidx])
		iidx++

		if oidx+length+2 > outLen {
			return 0, fmt.Errorf(`E2BIG non literal`)
		}

		if reference < 0 {
			return 0, fmt.Errorf(`EINVAL`)
		}

		// short-cut if reference will not collide with oidx
		if fullLength := length + 2; reference+fullLength < oidx {
			copy(outData[oidx:oidx+fullLength], outData[reference:reference+fullLength])
			oidx += fullLength
			reference += fullLength
			doOuterLoop = iidx < inLen
			continue
		}

		outData[oidx] = outData[reference]
		oidx++
		reference++

		outData[oidx] = outData[reference]
		oidx++
		reference++

		doInnerLoop := true
		for doInnerLoop {
			outData[oidx] = outData[reference]
			oidx += 1
			reference += 1

			length -= 1
			doInnerLoop = length > 0
		}

		doOuterLoop = iidx < inLen
	}
	return oidx, nil
}
